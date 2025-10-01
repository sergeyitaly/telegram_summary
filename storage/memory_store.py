# storage/memory_store.py
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Any
import logging
import threading
from threading import Lock
import sys
from contextvars import ContextVar
import warnings
from models.message import TelegramMessage

logger = logging.getLogger(__name__)

# Free-threaded mode optimizations
IS_FREE_THREADED = hasattr(sys, '_is_gil_enabled') and not sys._is_gil_enabled()

@dataclass
class ChatSummary:
    summary: str
    timestamp: datetime
    message_count: int
    chat_title: str = "Unknown Chat"

class MemoryStorage:
    def __init__(self, max_messages_per_chat: int = 100):
        self.max_messages_per_chat = max_messages_per_chat
        
        # Use more granular locking for better parallelism
        self._message_locks: Dict[str, Lock] = {}
        self._summary_locks: Dict[str, Lock] = {}
        self._global_lock = Lock()  # For global operations
        
        # Main data structures
        self.messages: Dict[str, List[TelegramMessage]] = {}
        self.summaries: Dict[str, List[ChatSummary]] = {}
        self.consolidated_summary: Optional[str] = None
        self.consolidated_timestamp: Optional[datetime] = None
        
        # Progress tracking with atomic updates
        self._processing_start_time = datetime.now(timezone.utc)
        self._last_summary_time = datetime.now(timezone.utc)
        
        # Thread management
        self._processing_threads: Dict[str, threading.Thread] = {}
        self._thread_registry_lock = Lock()
        
        # Free-threaded optimizations
        self._use_fine_grained_locking = IS_FREE_THREADED
        self._batch_operations = IS_FREE_THREADED
        
        # Context-aware warnings for free-threaded mode
        self._warning_context: ContextVar[Optional[Any]] = ContextVar('warning_context', default=None)
    
        self._processing_stats = {
            'status': 'waiting',
            'progress_percent': 0,
            'last_update': datetime.now(timezone.utc).isoformat(),
            'processing_time': 0,
            'error_count': 0
        }
        self._stats_lock = Lock()
    
    def update_processing_stats(self, 
                              status: str = None, 
                              progress_percent: int = None,
                              processing_time: float = None,
                              error_count: int = None):
        """Update processing statistics - used by main application"""
        with self._stats_lock:
            if status is not None:
                self._processing_stats['status'] = status
            if progress_percent is not None:
                self._processing_stats['progress_percent'] = progress_percent
            if processing_time is not None:
                self._processing_stats['processing_time'] = processing_time
            if error_count is not None:
                self._processing_stats['error_count'] = error_count
            
            self._processing_stats['last_update'] = datetime.now(timezone.utc).isoformat()

    def _get_message_lock(self, chat_id: str) -> Lock:
        """Get or create a lock for a specific chat's messages"""
        if not self._use_fine_grained_locking:
            return self._global_lock
            
        with self._global_lock:
            if chat_id not in self._message_locks:
                self._message_locks[chat_id] = Lock()
            return self._message_locks[chat_id]
    
    def _get_summary_lock(self, chat_id: str) -> Lock:
        """Get or create a lock for a specific chat's summaries"""
        if not self._use_fine_grained_locking:
            return self._global_lock
            
        with self._global_lock:
            if chat_id not in self._summary_locks:
                self._summary_locks[chat_id] = Lock()
            return self._summary_locks[chat_id]
    
    def store_messages(self, chat_id: str, messages: List[TelegramMessage]):
        """Store messages with chat-specific locking"""
        if not messages:
            return
            
        lock = self._get_message_lock(chat_id)
        with lock:
            if chat_id not in self.messages:
                self.messages[chat_id] = []
            
            # Add messages and remove duplicates
            existing_ids = {msg.id for msg in self.messages[chat_id]}
            new_messages = [msg for msg in messages if msg.id not in existing_ids]
            
            self.messages[chat_id].extend(new_messages)
            
            # Sort by timestamp and keep only the latest
            self.messages[chat_id].sort(key=lambda x: x.timestamp)
            self.messages[chat_id] = self.messages[chat_id][-self.max_messages_per_chat:]
            
            logger.debug(f"📥 Stored {len(new_messages)} new messages for chat {chat_id}, total: {len(self.messages[chat_id])}")
    
    def store_messages_batch(self, messages_dict: Dict[str, List[TelegramMessage]]):
        """Batch store messages for multiple chats - optimized for free-threaded mode"""
        if not self._batch_operations:
            for chat_id, messages in messages_dict.items():
                self.store_messages(chat_id, messages)
            return
        
        # Process in parallel when possible
        threads = []
        for chat_id, messages in messages_dict.items():
            thread = threading.Thread(
                target=self.store_messages,
                args=(chat_id, messages)
            )
            thread.start()
            threads.append(thread)
        
        for thread in threads:
            thread.join()

    def store_summary(self, chat_id: str, summary: str, message_count: int):
        """Store summary with chat-specific locking - FIXED VERSION"""
        try:
            # Get chat title from messages first
            chat_title = "Unknown Chat"
            if chat_id in self.messages and self.messages[chat_id]:
                chat_title = getattr(self.messages[chat_id][0], 'chat_title', 'Unknown Chat')
            
            logger.info(f"🔍 DEBUG: Storing summary for '{chat_title}' ({chat_id}) - {len(summary)} chars")
            
            lock = self._get_summary_lock(chat_id)
            with lock:
                if chat_id not in self.summaries:
                    self.summaries[chat_id] = []
                    logger.info(f"🔍 DEBUG: Created new summaries list for {chat_id}")
                
                summary_obj = ChatSummary(
                    summary=summary,
                    timestamp=datetime.now(timezone.utc),
                    message_count=message_count,
                    chat_title=chat_title
                )
                
                self.summaries[chat_id].append(summary_obj)
                self.summaries[chat_id] = self.summaries[chat_id][-5:]  # Keep only latest 5
                
                logger.info(f"✅ SUCCESS: Stored summary for '{chat_title}' - now {len(self.summaries[chat_id])} summaries for this chat")
                
            # Update last summary time with atomic operation
            with self._global_lock:
                self._last_summary_time = datetime.now(timezone.utc)
            
            # Auto-update progress stats when new summary is added
            self._auto_update_progress()
            
            # Final verification
            with lock:
                if chat_id in self.summaries and self.summaries[chat_id]:
                    stored = self.summaries[chat_id][-1]
                    logger.info(f"✅ VERIFIED: Storage confirmed for '{stored.chat_title}' - {len(stored.summary)} chars")
                else:
                    logger.error(f"❌ STORAGE FAILED: Summary not found for {chat_id} after storage")
                    
        except Exception as e:
            logger.error(f"❌ ERROR in store_summary for {chat_id}: {e}")
                
    def _auto_update_progress(self):
        """Automatically update progress based on current state"""
        try:
            chat_info = self.get_chat_info()
            total_chats = chat_info['total_chats']
            processed_chats = chat_info['chats_with_summaries']
            
            if total_chats > 0:
                progress_percent = min(100, int((processed_chats / total_chats) * 100))
                status = 'complete' if progress_percent >= 100 else 'processing'
                
                with self._stats_lock:
                    self._processing_stats['progress_percent'] = progress_percent
                    self._processing_stats['status'] = status
                    self._processing_stats['last_update'] = datetime.now(timezone.utc).isoformat()
                    
                logger.debug(f"📊 Auto-updated progress: {progress_percent}% ({processed_chats}/{total_chats} chats)")
        except Exception as e:
            logger.error(f"Error in _auto_update_progress: {e}")

    def store_consolidated_summary(self, summary: str):
        """Store consolidated summary with proper locking"""
        with self._global_lock:
            self.consolidated_summary = summary
            self.consolidated_timestamp = datetime.now(timezone.utc)
            logger.info(f"✅ Stored consolidated summary: {len(summary)} chars")
    
    def get_recent_summaries(self, max_age_minutes: int = 60) -> List[ChatSummary]:
        """Get recent summaries with minimal locking"""
        recent_summaries = []
        cutoff_time = datetime.now(timezone.utc) - timedelta(minutes=max_age_minutes)
        
        # Snapshot chat IDs to minimize lock time
        with self._global_lock:
            chat_ids = list(self.summaries.keys())
        
        # Check each chat's summaries with chat-specific locking
        for chat_id in chat_ids:
            lock = self._get_summary_lock(chat_id)
            with lock:
                if chat_id in self.summaries and self.summaries[chat_id]:
                    latest = self.summaries[chat_id][-1]
                    if latest.timestamp > cutoff_time:
                        recent_summaries.append(latest)
        
        return recent_summaries
    
    def get_chat_info(self) -> Dict:
        """Get comprehensive chat information with optimized locking"""
        try:
            # Use context-aware warnings in free-threaded mode
            with warnings.catch_warnings():
                if IS_FREE_THREADED:
                    warnings.simplefilter("ignore")  # Reduce warning overhead
                
                # Snapshot structure first
                with self._global_lock:
                    chat_ids = list(self.messages.keys())
                    summary_chat_ids = list(self.summaries.keys())
                
                # Process counts with minimal locking
                total_messages = 0
                unique_messages_per_chat = {}
                
                for chat_id in chat_ids:
                    lock = self._get_message_lock(chat_id)
                    with lock:
                        if chat_id in self.messages:
                            # Count unique messages (remove duplicates by ID)
                            unique_messages = {}
                            for msg in self.messages[chat_id]:
                                unique_messages[msg.id] = msg
                            message_count = len(unique_messages)
                            total_messages += message_count
                            unique_messages_per_chat[chat_id] = message_count
                
                # Count chats with recent summaries
                chats_with_summaries = 0
                cutoff_time = datetime.now(timezone.utc) - timedelta(hours=1)
                
                for chat_id in summary_chat_ids:
                    lock = self._get_summary_lock(chat_id)
                    with lock:
                        if (chat_id in self.summaries and 
                            self.summaries[chat_id] and 
                            self.summaries[chat_id][-1].timestamp > cutoff_time):
                            chats_with_summaries += 1
                
                with self._global_lock:
                    consolidated_available = self.consolidated_summary is not None
                    last_summary_time = self._last_summary_time.isoformat()
                
                result = {
                    'total_chats': len(chat_ids),
                    'total_messages': total_messages,
                    'chats_with_summaries': chats_with_summaries,
                    'consolidated_available': consolidated_available,
                    'last_summary_time': last_summary_time,
                    'free_threaded': IS_FREE_THREADED,
                    'unique_messages_per_chat': unique_messages_per_chat
                }
                
                logger.debug(f"📊 Chat info: {len(chat_ids)} chats, {total_messages} messages, {chats_with_summaries} with summaries")
                return result
                
        except Exception as e:
            logger.error(f"Error in get_chat_info: {e}")
            return {
                'total_chats': 0,
                'total_messages': 0,
                'chats_with_summaries': 0,
                'consolidated_available': False,
                'last_summary_time': datetime.now(timezone.utc).isoformat(),
                'free_threaded': IS_FREE_THREADED
            }
    
    def get_processing_stats(self) -> Dict:
        """Get processing statistics with free-threaded optimizations"""
        with self._stats_lock:
            stats = self._processing_stats.copy()
        
        chat_info = self.get_chat_info()
        total_chats = chat_info['total_chats']
        processed_chats = chat_info['chats_with_summaries']
        
        # Calculate progress based on actual data if not manually set
        if stats['progress_percent'] == 0 and total_chats > 0:
            stats['progress_percent'] = min(100, int((processed_chats / total_chats) * 100))
        
        # Update status based on actual data if not manually set
        if stats['status'] == 'waiting' and total_chats > 0:
            stats['status'] = 'complete' if stats['progress_percent'] >= 100 else 'processing'
        
        stats.update({
            'total_chats': total_chats,
            'processed_chats': processed_chats,
            'consolidated_ready': self.consolidated_summary is not None,
            'free_threaded_optimized': self._use_fine_grained_locking,
            'batch_operations_enabled': self._batch_operations,
            'storage_summaries_count': len(self.summaries),
            'storage_messages_count': len(self.messages)
        })
        
        return stats
        
    def get_storage_debug_info(self) -> Dict[str, Any]:
        """Get detailed debug information about storage state"""
        with self._global_lock:
            chat_ids = list(self.messages.keys())
            summary_chat_ids = list(self.summaries.keys())
        
        debug_info = {
            'total_chats_with_messages': len(self.messages),
            'total_chats_with_summaries': len(self.summaries),
            'messages_by_chat': {},
            'summaries_by_chat': {},
            'consolidated_summary': bool(self.consolidated_summary),
            'consolidated_timestamp': self.consolidated_timestamp.isoformat() if self.consolidated_timestamp else None,
            'sample_summaries': []
        }
        
        # Count messages per chat
        for chat_id in chat_ids:
            lock = self._get_message_lock(chat_id)
            with lock:
                if chat_id in self.messages:
                    debug_info['messages_by_chat'][chat_id] = len(self.messages[chat_id])
        
        # Count summaries per chat and get sample data
        sample_count = 0
        for chat_id in summary_chat_ids:
            lock = self._get_summary_lock(chat_id)
            with lock:
                if chat_id in self.summaries:
                    debug_info['summaries_by_chat'][chat_id] = len(self.summaries[chat_id])
                    
                    # Add sample summaries for debugging
                    if self.summaries[chat_id] and sample_count < 3:
                        latest = self.summaries[chat_id][-1]
                        debug_info['sample_summaries'].append({
                            'chat_id': chat_id,
                            'chat_title': latest.chat_title,
                            'summary_length': len(latest.summary),
                            'summary_preview': latest.summary[:100] + '...' if len(latest.summary) > 100 else latest.summary,
                            'timestamp': latest.timestamp.isoformat(),
                            'message_count': latest.message_count
                        })
                        sample_count += 1
        
        return debug_info
        
    # Enhanced thread management for free-threaded mode
    def register_processing_thread(self, thread_id: str, thread: threading.Thread):
        """Register a processing thread with context inheritance"""
        with self._thread_registry_lock:
            self._processing_threads[thread_id] = thread
            
            # In free-threaded mode, inherit context if enabled
            if IS_FREE_THREADED and hasattr(thread, '_context'):
                # PEP 703: Threads inherit context in free-threaded mode
                pass
    
    def unregister_processing_thread(self, thread_id: str):
        """Unregister a completed processing thread"""
        with self._thread_registry_lock:
            if thread_id in self._processing_threads:
                del self._processing_threads[thread_id]
    
    def get_active_threads(self) -> Dict[str, threading.Thread]:
        """Get all active processing threads"""
        with self._thread_registry_lock:
            return {tid: t for tid, t in self._processing_threads.items() if t.is_alive()}
    
    def parallel_process_chats(self, 
                             chat_processor, 
                             chat_ids: List[str],
                             max_workers: Optional[int] = None) -> Dict[str, Any]:
        """Process multiple chats in parallel using free-threaded mode"""
        if not IS_FREE_THREADED and max_workers is None:
            max_workers = 4  # Conservative for GIL mode
        elif max_workers is None:
            max_workers = min(16, len(chat_ids))  # More aggressive in free-threaded
        
        import concurrent.futures
        
        results = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_chat = {
                executor.submit(chat_processor, chat_id, self.messages.get(chat_id, [])): chat_id 
                for chat_id in chat_ids
            }
            
            # Collect results as they complete
            for future in concurrent.futures.as_completed(future_to_chat):
                chat_id = future_to_chat[future]
                try:
                    result = future.result()
                    results[chat_id] = result
                except Exception as exc:
                    logger.error(f"Chat {chat_id} processing failed: {exc}")
                    results[chat_id] = {'error': str(exc)}
        
        return results
    
    def wait_for_completion(self, timeout: Optional[float] = None) -> bool:
        """Wait for all processing threads to complete"""
        threads = list(self.get_active_threads().values())
        for thread in threads:
            thread.join(timeout)
            if thread.is_alive():
                return False  # Timeout occurred
        return True  # All threads completed
    
    def cleanup_finished_threads(self):
        """Clean up finished threads from the registry"""
        with self._thread_registry_lock:
            finished_threads = [tid for tid, t in self._processing_threads.items() if not t.is_alive()]
            for thread_id in finished_threads:
                del self._processing_threads[thread_id]