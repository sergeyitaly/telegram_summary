# core/telegram_client.py
import asyncio
import logging
from typing import List, Dict, Optional, Set, Tuple
from telethon import TelegramClient
from telethon.tl.types import Message, User, Chat, Channel, Dialog
from telethon.tl.functions.messages import GetDialogFiltersRequest
from telethon.tl.types import DialogFilter
import time
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

from config.settings import TelegramConfig
from models.message import TelegramMessage

logger = logging.getLogger(__name__)


class SmartRateLimiter:
    def __init__(self):
        self._last_request_time: Dict[str, float] = {}
        self._min_interval = 0.2  # 200ms between requests to same chat
        self._global_interval = 0.1  # 100ms between any requests
        self._last_global_request = 0
        self._concurrent_requests = 0
        self._max_concurrent = 2  # Conservative to avoid flooding
        
    async def wait_if_needed(self, chat_id: str):
        """Smart rate limiting to avoid flood waits"""
        current_time = time.time()
        
        # Global rate limiting
        time_since_global = current_time - self._last_global_request
        if time_since_global < self._global_interval:
            await asyncio.sleep(self._global_interval - time_since_global)
        
        # Chat-specific rate limiting
        if chat_id in self._last_request_time:
            time_since_chat = current_time - self._last_request_time[chat_id]
            if time_since_chat < self._min_interval:
                await asyncio.sleep(self._min_interval - time_since_chat)
        
        # Concurrent request limiting
        while self._concurrent_requests >= self._max_concurrent:
            await asyncio.sleep(0.01)  # Very short wait
        
        self._concurrent_requests += 1
        self._last_request_time[chat_id] = time.time()
        self._last_global_request = time.time()
    
    def request_completed(self):
        """Mark request as completed"""
        self._concurrent_requests = max(0, self._concurrent_requests - 1)

@dataclass
class ChatProcessingStats:
    chat_id: str
    chat_title: str
    total_messages: int
    processed_messages: int
    processing_time: float
    success: bool

class TelegramClientManager:
    def __init__(self, config: TelegramConfig):
        self.config = config
        self.client: Optional[TelegramClient] = None
        self.is_connected = False
        self._connection_lock = asyncio.Lock()
        self._processing_stats: Dict[str, ChatProcessingStats] = {}
        self._stats_lock = threading.Lock()
        
        # Free-threaded optimizations
        self._use_parallel_processing = True
        self._max_concurrent_chats = 8  # Optimized for free-threaded mode
        self._message_batch_size = 50
        
        # Cache for chat information
        self._chat_cache: Dict[str, Tuple[str, str]] = {}  # chat_id -> (chat_title, sender_name)
        self._cache_lock = threading.Lock()
        
        # Connection pooling for multiple clients (future enhancement)
        self._client_pool: List[TelegramClient] = []
        self._pool_size = 1  # Can be increased for free-threaded mode
    
    async def connect(self) -> bool:
        """Connect to Telegram with connection pooling and retry logic"""
        async with self._connection_lock:
            if self.is_connected:
                return True
                
            try:
                logger.info("🔐 Connecting to Telegram...")
                
                self.client = TelegramClient(
                    session=self.config.session_file,
                    api_id=self.config.api_id,
                    api_hash=self.config.api_hash,
                    # Optimize connection parameters
                    connection_retries=3,
                    request_retries=3,
                    auto_reconnect=True,
                    flood_sleep_threshold=60
                )
                
                # Add connection timeout
                try:
                    await asyncio.wait_for(
                        self.client.start(phone=self.config.phone),
                        timeout=30.0
                    )
                except asyncio.TimeoutError:
                    logger.error("❌ Telegram connection timeout")
                    return False
                
                # Get user info with caching
                me = await self.client.get_me()
                logger.info(f"✅ Connected as: {me.first_name} (@{me.username})")
                self.is_connected = True
                
                # Pre-warm cache
                asyncio.create_task(self._preload_chat_cache())
                
                return True
                
            except Exception as e:
                logger.error(f"❌ Telegram connection failed: {e}")
                self.is_connected = False
                return False
    
    async def _preload_chat_cache(self):
        """Preload chat information cache for faster processing"""
        try:
            async for dialog in self.client.iter_dialogs(limit=50):
                chat = dialog.entity
                chat_id = str(getattr(chat, 'id', 'unknown'))
                chat_title = self._get_chat_title(chat)
                
                with self._cache_lock:
                    self._chat_cache[chat_id] = (chat_title, "Unknown")
                    
        except Exception as e:
            logger.debug(f"Cache preloading failed: {e}")
    
    async def disconnect(self):
        """Disconnect from Telegram with proper cleanup"""
        async with self._connection_lock:
            if self.client and self.is_connected:
                try:
                    await self.client.disconnect()
                    self.is_connected = False
                    
                    # Clear cache
                    with self._cache_lock:
                        self._chat_cache.clear()
                    
                    logger.info("🔌 Disconnected from Telegram")
                except Exception as e:
                    logger.error(f"Error during disconnect: {e}")
    
    async def collect_unread_messages(self, limit_per_chat: int = 100) -> Dict[str, List[TelegramMessage]]:
        """
        Collect only unread messages with parallel processing optimizations.
        """
        if not self.client or not self.is_connected:
            raise RuntimeError("Telegram client not connected")
        
        messages_by_chat = {}
        total_unread_messages = 0
        start_time = time.time()
        
        try:
            logger.info("🔍 Scanning for unread messages with parallel optimizations...")
            
            # Get all dialogs first
            dialogs = []
            async for dialog in self.client.iter_dialogs():
                if hasattr(dialog, 'unread_count') and dialog.unread_count > 0:
                    dialogs.append(dialog)
            
            logger.info(f"📨 Found {len(dialogs)} dialogs with unread messages")
            
            # Process dialogs in parallel batches
            if self._use_parallel_processing and len(dialogs) > 1:
                # Use asyncio.gather for parallel processing
                tasks = []
                for dialog in dialogs:
                    task = self._process_dialog_messages(dialog, limit_per_chat)
                    tasks.append(task)
                
                # Process in batches to avoid overwhelming the API
                batch_size = self._max_concurrent_chats
                for i in range(0, len(tasks), batch_size):
                    batch = tasks[i:i + batch_size]
                    results = await asyncio.gather(*batch, return_exceptions=True)
                    
                    for result in results:
                        if isinstance(result, Exception):
                            logger.error(f"Dialog processing error: {result}")
                            continue
                            
                        chat_id, chat_messages = result
                        if chat_messages:
                            messages_by_chat[chat_id] = chat_messages
                            total_unread_messages += len(chat_messages)
                    
                    # Small delay between batches to respect rate limits
                    if i + batch_size < len(tasks):
                        await asyncio.sleep(0.5)
            else:
                # Sequential processing for small numbers or disabled parallelism
                for dialog in dialogs:
                    chat_id, chat_messages = await self._process_dialog_messages(dialog, limit_per_chat)
                    if chat_messages:
                        messages_by_chat[chat_id] = chat_messages
                        total_unread_messages += len(chat_messages)
            
            processing_time = time.time() - start_time
            logger.info(f"📊 Collected {total_unread_messages} unread messages from {len(messages_by_chat)} chats in {processing_time:.2f}s")
            
            return messages_by_chat
            
        except Exception as e:
            logger.error(f"❌ Error collecting unread messages: {e}")
            return {}
    
    async def _process_dialog_messages(self, dialog: Dialog, limit_per_chat: int) -> Tuple[str, List[TelegramMessage]]:
        """Process messages for a single dialog with optimized fetching"""
        chat = dialog.entity
        chat_id = str(getattr(chat, 'id', 'unknown'))
        chat_title = self._get_cached_chat_title(chat_id, chat)
        
        chat_messages = []
        processing_start = time.time()
        
        try:
            last_read_id = getattr(dialog, 'read_inbox_max_id', 0)
            unread_count = getattr(dialog, 'unread_count', 0)
            
            logger.debug(f"Processing {unread_count} unread messages from {chat_title}")
            
            # Optimized message collection with batch processing
            message_batches = []
            current_batch = []
            
            async for message in self.client.iter_messages(
                chat, 
                limit=min(unread_count * 2, limit_per_chat),
                reverse=True  # Start from oldest unread
            ):
                if len(chat_messages) >= unread_count:
                    break
                    
                if (message.id > last_read_id and 
                    hasattr(message, 'out') and not message.out and
                    message.text and message.text.strip()):
                    
                    telegram_msg = TelegramMessage(
                        id=message.id,
                        text=message.text.strip(),
                        timestamp=message.date,
                        sender_name=self._get_cached_sender_name(message.sender),
                        chat_id=chat_id,
                        chat_title=chat_title,
                        is_unread=True
                    )
                    
                    current_batch.append(telegram_msg)
                    
                    # Process in batches for better performance
                    if len(current_batch) >= self._message_batch_size:
                        message_batches.append(current_batch)
                        current_batch = []
            
            # Add any remaining messages
            if current_batch:
                message_batches.append(current_batch)
            
            # Flatten batches
            for batch in message_batches:
                chat_messages.extend(batch)
                        
        except Exception as e:
            logger.warning(f"⚠️ Could not fetch unread messages from {chat_title}: {e}")
        
        processing_time = time.time() - processing_start
        
        # Update statistics
        with self._stats_lock:
            self._processing_stats[chat_id] = ChatProcessingStats(
                chat_id=chat_id,
                chat_title=chat_title,
                total_messages=unread_count,
                processed_messages=len(chat_messages),
                processing_time=processing_time,
                success=len(chat_messages) > 0
            )
        
        return chat_id, chat_messages
    
    def _get_cached_chat_title(self, chat_id: str, chat) -> str:
        """Get chat title from cache or compute and cache it"""
        with self._cache_lock:
            if chat_id in self._chat_cache:
                return self._chat_cache[chat_id][0]
            
            chat_title = self._get_chat_title(chat)
            self._chat_cache[chat_id] = (chat_title, "Unknown")
            return chat_title
    
    def _get_cached_sender_name(self, sender) -> str:
        """Get sender name with basic caching"""
        if not sender:
            return "Unknown"
        
        sender_id = str(getattr(sender, 'id', 'unknown'))
        with self._cache_lock:
            for cached_chat_id, (_, cached_sender) in self._chat_cache.items():
                if cached_sender != "Unknown":
                    # Simple cache lookup - could be enhanced with proper sender cache
                    pass
            
            sender_name = self._get_sender_name(sender)
            # Update cache if this sender is also a chat
            if hasattr(sender, 'id'):
                sender_chat_id = str(sender.id)
                if sender_chat_id in self._chat_cache:
                    chat_title, _ = self._chat_cache[sender_chat_id]
                    self._chat_cache[sender_chat_id] = (chat_title, sender_name)
            
            return sender_name
    
    async def collect_unread_messages_selective(self, chat_ids: Optional[List[str]] = None) -> Dict[str, List[TelegramMessage]]:
        """
        Collect unread messages only from specific chats.
        Useful for targeted processing.
        """
        if not self.client or not self.is_connected:
            raise RuntimeError("Telegram client not connected")
        
        messages_by_chat = {}
        
        try:
            async for dialog in self.client.iter_dialogs():
                chat = dialog.entity
                current_chat_id = str(getattr(chat, 'id', 'unknown'))
                
                if chat_ids and current_chat_id not in chat_ids:
                    continue
                    
                if hasattr(dialog, 'unread_count') and dialog.unread_count > 0:
                    chat_id, chat_messages = await self._process_dialog_messages(dialog, 100)
                    if chat_messages:
                        messages_by_chat[chat_id] = chat_messages
            
            return messages_by_chat
            
        except Exception as e:
            logger.error(f"Error in selective message collection: {e}")
            return {}
    
    async def get_connection_stats(self) -> Dict:
        """Get connection and processing statistics"""
        with self._stats_lock:
            stats = {
                'connected': self.is_connected,
                'cached_chats': len(self._chat_cache),
                'recent_processed_chats': len(self._processing_stats),
                'parallel_processing': self._use_parallel_processing,
                'max_concurrent_chats': self._max_concurrent_chats,
                'processing_stats': {}
            }
            
            for chat_id, proc_stats in list(self._processing_stats.items()):
                # Only keep recent stats (last 10 minutes)
                if time.time() - proc_stats.processing_time < 600:
                    stats['processing_stats'][chat_id] = {
                        'chat_title': proc_stats.chat_title,
                        'total_messages': proc_stats.total_messages,
                        'processed_messages': proc_stats.processed_messages,
                        'processing_time': proc_stats.processing_time,
                        'success': proc_stats.success
                    }
            
            return stats
    
    def _get_chat_title(self, chat) -> str:
        """Extract chat title safely"""
        try:
            if hasattr(chat, 'title'):
                return chat.title or "Unknown Chat"
            elif hasattr(chat, 'first_name'):
                name = chat.first_name or ""
                if hasattr(chat, 'last_name') and chat.last_name:
                    name += f" {chat.last_name}"
                return name.strip() or "Unknown Chat"
            elif hasattr(chat, 'username'):
                return f"@{chat.username}" if chat.username else "Unknown Chat"
        except:
            pass
        return "Unknown Chat"
    
    def _get_sender_name(self, sender) -> str:
        """Extract sender name safely"""
        try:
            if not sender:
                return "Unknown"
                
            if hasattr(sender, 'first_name'):
                name = sender.first_name or ""
                if hasattr(sender, 'last_name') and sender.last_name:
                    name += f" {sender.last_name}"
                return name.strip() or "Unknown"
            elif hasattr(sender, 'username'):
                return f"@{sender.username}" if sender.username else "Unknown"
            elif hasattr(sender, 'title'):  # For channels
                return sender.title or "Unknown"
        except:
            pass
        return "Unknown"

    # Backward compatibility
    async def collect_messages(self, limit: int = 30) -> Dict[str, List[TelegramMessage]]:
        """Original method - now collects only unread messages"""
        logger.info("📨 Using optimized unread-only message collection")
        return await self.collect_unread_messages(limit_per_chat=limit)
    
    async def collect_all_unread_messages(self) -> Dict[str, List[TelegramMessage]]:
        """
        Comprehensive method to collect all types of unread messages
        """
        return await self.collect_unread_messages(limit_per_chat=200)