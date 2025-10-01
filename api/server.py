import json
import logging
from http.server import HTTPServer, BaseHTTPRequestHandler
from threading import Thread, Lock
from datetime import datetime, timezone
from urllib.parse import urlparse, parse_qs
import time
from typing import Dict, Set, Optional, List, Any
import asyncio
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import queue
import heapq
from dataclasses import dataclass
import hashlib
import os
from contextvars import ContextVar
import warnings
import socket

from storage.memory_store import MemoryStorage

logger = logging.getLogger(__name__)

# Free-threaded mode optimizations
IS_FREE_THREADED = hasattr(__import__('sys'), '_is_gil_enabled') and not __import__('sys')._is_gil_enabled()

@dataclass
class CacheEntry:
    data: Any
    timestamp: float
    access_count: int = 0
    size: int = 0

class AdaptiveCache:
    """Intelligent cache with size-based eviction and access patterns"""
    
    def __init__(self, max_size_mb: int = 100, default_ttl: int = 2):
        self.max_size_bytes = max_size_mb * 1024 * 1024
        self.default_ttl = default_ttl
        self._cache: Dict[str, CacheEntry] = {}
        self._access_times: Dict[str, float] = {}
        self._lock = Lock()
        self._current_size = 0
        self.hits = 0
        self.misses = 0
        
    def get(self, key: str) -> Optional[Any]:
        with self._lock:
            if key in self._cache:
                entry = self._cache[key]
                if time.time() - entry.timestamp < self.default_ttl:
                    entry.access_count += 1
                    self._access_times[key] = time.time()
                    self.hits += 1
                    return entry.data
            
            self.misses += 1
            return None
    
    def set(self, key: str, data: Any, size_estimate: int = 0):
        with self._lock:
            # Calculate actual size if not provided
            if size_estimate == 0:
                size_estimate = len(str(data).encode('utf-8'))
            
            # Evict if needed
            while self._current_size + size_estimate > self.max_size_bytes and self._cache:
                self._evict_oldest()
            
            self._cache[key] = CacheEntry(
                data=data,
                timestamp=time.time(),
                size=size_estimate
            )
            self._access_times[key] = time.time()
            self._current_size += size_estimate
    
    def _evict_oldest(self):
        """Evict least recently used entries"""
        if not self._cache:
            return
            
        # Find candidates for eviction (oldest access time)
        candidates = []
        for key, access_time in self._access_times.items():
            if key in self._cache:
                candidates.append((access_time, key, self._cache[key]))
        
        if not candidates:
            return
            
        # Sort by access time (oldest first)
        candidates.sort(key=lambda x: x[0])
        
        # Remove oldest 10% or at least 1 entry
        evict_count = max(1, len(candidates) // 10)
        for i in range(evict_count):
            if i < len(candidates):
                access_time, key, entry = candidates[i]
                self._current_size -= entry.size
                self._cache.pop(key, None)
                self._access_times.pop(key, None)
    
    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            total_requests = self.hits + self.misses
            hit_rate = (self.hits / total_requests * 100) if total_requests > 0 else 0
            return {
                'hits': self.hits,
                'misses': self.misses,
                'hit_rate': round(hit_rate, 1),
                'size_mb': round(self._current_size / (1024 * 1024), 2),
                'entries': len(self._cache),
                'max_size_mb': self.max_size_bytes / (1024 * 1024)
            }

class BackgroundPreprocessor:
    """Advanced background data preprocessor with free-threaded optimizations"""
    
    def __init__(self, storage: MemoryStorage):
        self.storage = storage
        self.cache = AdaptiveCache(max_size_mb=50, default_ttl=1)
        self._is_running = True
        self._last_processing_time = 0
        self._processing_stats = {
            'cycles_completed': 0,
            'avg_cycle_time': 0,
            'last_error': None
        }
        
        # Free-threaded optimizations
        if IS_FREE_THREADED:
            self._executor = ProcessPoolExecutor(max_workers=2)  # Use processes for CPU-bound work
            self._io_executor = ThreadPoolExecutor(max_workers=8, thread_name_prefix="IO_Preproc")
        else:
            self._executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="Preproc")
            self._io_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="IO_Preproc")
        
        # Start specialized background threads
        self._summary_thread = Thread(target=self._continuous_summary_updates, daemon=True, name="Summary_Preproc")
        self._stats_thread = Thread(target=self._continuous_stats_updates, daemon=True, name="Stats_Preproc")
        self._message_thread = Thread(target=self._continuous_message_updates, daemon=True, name="Message_Preproc")
        
        self._summary_thread.start()
        self._stats_thread.start()
        self._message_thread.start()
        
        logger.info(f"🚀 BackgroundPreprocessor started (Free-threaded: {IS_FREE_THREADED})")
    
    def _continuous_summary_updates(self):
        """Continuous summary pre-computation with adaptive timing"""
        cycle_count = 0
        while self._is_running:
            try:
                start_time = time.time()
                
                # Adaptive sleep based on data change rate
                if cycle_count > 0:
                    change_rate = self._calculate_data_change_rate()
                    sleep_time = max(0.1, min(2.0, 1.0 / (change_rate + 0.1)))
                    time.sleep(sleep_time)
                
                data = self._generate_summaries_data()
                self.cache.set("summaries_data", data, size_estimate=len(str(data)))
                
                cycle_time = time.time() - start_time
                self._update_processing_stats(cycle_time)
                cycle_count += 1
                
            except Exception as e:
                logger.error(f"Summary preprocessor error: {e}")
                self._processing_stats['last_error'] = str(e)
                time.sleep(1)
    
    def _continuous_stats_updates(self):
        """Continuous stats pre-computation"""
        while self._is_running:
            try:
                data = self._generate_stats_data()
                self.cache.set("stats_data", data, size_estimate=len(str(data)))
                time.sleep(0.3)  # Stats update more frequently
            except Exception as e:
                logger.error(f"Stats preprocessor error: {e}")
                time.sleep(0.5)
    
    def _continuous_message_updates(self):
        """Continuous message pre-computation"""
        while self._is_running:
            try:
                data = self._generate_messages_data()
                self.cache.set("messages_data", data, size_estimate=len(str(data)))
                time.sleep(0.5)
            except Exception as e:
                logger.error(f"Message preprocessor error: {e}")
                time.sleep(0.5)
    
    def _calculate_data_change_rate(self) -> float:
        """Calculate how quickly data is changing"""
        try:
            # Simple implementation - track message count changes
            current_count = sum(len(msgs) for msgs in self.storage.messages.values())
            # Could be enhanced with more sophisticated change detection
            return min(10.0, current_count / 100.0)  # Normalize to 0-10 range
        except:
            return 1.0
    
    def _update_processing_stats(self, cycle_time: float):
        """Update processing statistics"""
        self._processing_stats['cycles_completed'] += 1
        current_avg = self._processing_stats['avg_cycle_time']
        cycles = self._processing_stats['cycles_completed']
        
        # Exponential moving average
        alpha = 0.1
        self._processing_stats['avg_cycle_time'] = (
            alpha * cycle_time + (1 - alpha) * current_avg
            if current_avg > 0 else cycle_time
        )
    
    def _generate_summaries_data(self) -> List[Dict[str, Any]]:
        """Generate summaries data with enhanced information"""
        result = []
        seen_chats = set()
        
        for chat_id, messages in self.storage.messages.items():
            if not messages or chat_id in seen_chats:
                continue
                
            seen_chats.add(chat_id)
            chat_title = messages[0].chat_title
            latest_summary = None
            if chat_id in self.storage.summaries and self.storage.summaries[chat_id]:
                latest_summary = self.storage.summaries[chat_id][-1]
            
            summary_age = None
            if latest_summary:
                # Use timezone-aware datetime for comparison
                current_time = datetime.now(timezone.utc)
                # Ensure both datetimes are timezone-aware
                if latest_summary.timestamp.tzinfo is None:
                    # If summary timestamp is naive, make it aware
                    summary_time = latest_summary.timestamp.replace(tzinfo=timezone.utc)
                else:
                    summary_time = latest_summary.timestamp
                
                age_seconds = (current_time - summary_time).total_seconds()
                summary_age = int(age_seconds / 60)
            
            unique_messages = self._get_unique_messages(chat_id, messages)
            unread_count = sum(1 for msg in unique_messages if getattr(msg, 'is_unread', False))
            
            # Calculate chat activity score
            activity_score = self._calculate_chat_activity(messages)
            
            result.append({
                'type': 'individual',
                'chat_id': chat_id,
                'chat_title': chat_title,
                'summary': latest_summary.summary if latest_summary else 'Collecting messages...',
                'last_updated': (latest_summary.timestamp if latest_summary else datetime.now(timezone.utc)).isoformat(),
                'message_count': len(unique_messages),
                'total_messages_collected': len(messages),
                'has_summary': latest_summary is not None,
                'summary_age_minutes': summary_age,
                'needs_refresh': summary_age is not None and summary_age > 10,
                'unread_count': unread_count,
                'activity_score': activity_score,
                'priority': 'high' if unread_count > 5 or activity_score > 0.7 else 'medium' if activity_score > 0.3 else 'low'
            })
        
        # Sort by priority and activity
        result.sort(key=lambda x: (
            -x['unread_count'],
            -x['activity_score'],
            x['last_updated']
        ), reverse=True)
        
        return result
    
    def _generate_stats_data(self) -> Dict[str, Any]:
        """Generate enhanced stats data"""
        chat_info = self.storage.get_chat_info()
        progress_data = self.storage.get_processing_stats()
        
        total_chats = chat_info['total_chats']
        chats_with_summaries = chat_info['chats_with_summaries']
        
        # Calculate enhanced message statistics
        total_messages = 0
        total_unread = 0
        active_chats = 0
        recent_activity = 0
        
        current_time = datetime.now(timezone.utc)  # Make it timezone-aware
        
        for chat_id, messages in self.storage.messages.items():
            unique_messages = self._get_unique_messages(chat_id, messages)
            total_messages += len(unique_messages)
            total_unread += sum(1 for msg in unique_messages if getattr(msg, 'is_unread', False))
            
            # Count active chats (messages in last 24 hours)
            if messages:
                latest_message = max(messages, key=lambda x: x.timestamp)
                # Ensure both datetimes are timezone-aware for comparison
                if latest_message.timestamp.tzinfo is None:
                    message_time = latest_message.timestamp.replace(tzinfo=timezone.utc)
                else:
                    message_time = latest_message.timestamp
                
                if (current_time - message_time).total_seconds() < 86400:
                    active_chats += 1
                    recent_activity += 1
        
        if total_chats > 0:
            coverage_percent = (chats_with_summaries / total_chats) * 100
            chats_needing_summaries = total_chats - chats_with_summaries
            activity_percent = (active_chats / total_chats) * 100
        else:
            coverage_percent = 0
            chats_needing_summaries = 0
            activity_percent = 0
        
        return {
            'total_chats': total_chats,
            'total_messages': total_messages,
            'total_unread_messages': total_unread,
            'chats_with_summaries': chats_with_summaries,
            'chats_needing_summaries': chats_needing_summaries,
            'active_chats': active_chats,
            'recent_activity': recent_activity,
            'coverage_percent': round(coverage_percent, 1),
            'activity_percent': round(activity_percent, 1),
            'progress_percent': progress_data['progress_percent'],
            'status': progress_data['status'],
            'consolidated_available': self.storage.consolidated_summary is not None,
            'last_summary_time': chat_info.get('last_summary_time', datetime.now(timezone.utc).isoformat()),
            'processing_time_seconds': progress_data['processing_time'],
            'last_update': progress_data['last_update'],
            'summary_freshness': 'excellent' if chats_needing_summaries == 0 else 'good' if coverage_percent > 70 else 'needs_work',
            'collection_status': 'active' if total_messages > 0 else 'starting',
            'system_health': self._assess_system_health(progress_data, total_messages)
        }
    
    def _generate_messages_data(self) -> Dict[str, Any]:
        """Generate enhanced messages data"""
        all_messages = []
        total_unread = 0
        chat_activity = {}
        
        current_time = datetime.now(timezone.utc)  # Make it timezone-aware
        
        for chat_id, messages in self.storage.messages.items():
            if messages:
                unique_messages = self._get_unique_messages(chat_id, messages)
                chat_unread = sum(1 for msg in unique_messages if getattr(msg, 'is_unread', False))
                total_unread += chat_unread
                
                # Calculate last activity with timezone-aware comparison
                if unique_messages:
                    latest_message = max(unique_messages, key=lambda x: x.timestamp)
                    if latest_message.timestamp.tzinfo is None:
                        message_time = latest_message.timestamp.replace(tzinfo=timezone.utc)
                    else:
                        message_time = latest_message.timestamp
                    last_activity = message_time.isoformat()
                else:
                    last_activity = None
                
                chat_activity[chat_id] = {
                    'title': messages[0].chat_title,
                    'message_count': len(unique_messages),
                    'unread_count': chat_unread,
                    'last_activity': last_activity
                }
                
                all_messages.extend([
                    {
                        'chat_id': chat_id,
                        'chat_title': messages[0].chat_title,
                        'message_id': msg.id,
                        'text': msg.text,
                        'timestamp': msg.timestamp.isoformat(),
                        'sender': msg.sender_name,
                        'is_unread': getattr(msg, 'is_unread', False),
                        'priority': 'high' if getattr(msg, 'is_unread', False) else 'normal'
                    }
                    for msg in unique_messages[-100:]  # Limit per chat for performance
                ])
        
        # Sort by priority and timestamp
        all_messages.sort(key=lambda x: (
            x['priority'] != 'high',  # Unread first
            x['timestamp']
        ), reverse=True)
        
        return {
            'total_messages': len(all_messages),
            'total_unread': total_unread,
            'total_chats': len(self.storage.messages),
            'chat_activity': chat_activity,
            'messages': all_messages[:500]  # Limit total for performance
        }
    
    def _get_unique_messages(self, chat_id: str, messages: list) -> list:
        """Remove duplicate messages based on message ID"""
        unique_messages = []
        seen_ids = set()
        
        for message in messages:
            if message.id not in seen_ids:
                seen_ids.add(message.id)
                unique_messages.append(message)
        
        unique_messages.sort(key=lambda x: x.timestamp, reverse=True)
        return unique_messages
    
    def _calculate_chat_activity(self, messages: List[Any]) -> float:
        """Calculate chat activity score (0-1)"""
        if not messages:
            return 0.0
        
        # Use timezone-aware datetime for comparison
        current_time = datetime.now(timezone.utc)
        
        # Consider message frequency, recency, and volume
        recent_messages = []
        for msg in messages:
            # Ensure both datetimes are timezone-aware for comparison
            if msg.timestamp.tzinfo is None:
                message_time = msg.timestamp.replace(tzinfo=timezone.utc)
            else:
                message_time = msg.timestamp
            
            if (current_time - message_time).total_seconds() < 86400:  # 24 hours
                recent_messages.append(msg)
        
        if not recent_messages:
            return 0.0
        
        activity_score = min(1.0, len(recent_messages) / 50.0)  # Normalize
        return round(activity_score, 2)
    
    def _assess_system_health(self, progress_data: Dict, total_messages: int) -> Dict[str, Any]:
        """Assess overall system health"""
        health_score = 100
        
        # Deduct points for issues
        if progress_data.get('status') != 'complete':
            health_score -= 20
        if total_messages == 0:
            health_score -= 30
        if self._processing_stats.get('last_error'):
            health_score -= 10
        
        health_level = 'excellent' if health_score >= 90 else 'good' if health_score >= 70 else 'degraded'
        
        return {
            'score': max(0, health_score),
            'level': health_level,
            'details': {
                'data_collection': 'active' if total_messages > 0 else 'inactive',
                'processing': progress_data.get('status', 'unknown'),
                'errors': bool(self._processing_stats.get('last_error'))
            }
        }
    
    def get_cached_data(self, data_type: str) -> Optional[Any]:
        """Get cached data with fallback to real-time generation"""
        cache_key = f"{data_type}_data"
        cached = self.cache.get(cache_key)
        
        if cached is not None:
            return cached
        
        # Fallback to real-time generation
        logger.debug(f"Cache miss for {data_type}, generating real-time")
        if data_type == "summaries":
            return self._generate_summaries_data()
        elif data_type == "stats":
            return self._generate_stats_data()
        elif data_type == "messages":
            return self._generate_messages_data()
        
        return None
    
    def get_performance_stats(self) -> Dict[str, Any]:
        """Get performance statistics"""
        cache_stats = self.cache.get_stats()
        return {
            'cache': cache_stats,
            'preprocessing': self._processing_stats,
            'free_threaded': IS_FREE_THREADED,
            'threads_active': all(
                t.is_alive() for t in [
                    self._summary_thread, 
                    self._stats_thread, 
                    self._message_thread
                ]
            )
        }
    
    def stop(self):
        """Stop background processors"""
        self._is_running = False
        self._executor.shutdown(wait=False)
        self._io_executor.shutdown(wait=False)

class HighPerformanceHTTPHandler(BaseHTTPRequestHandler):
    """High-performance HTTP handler with free-threaded optimizations"""
    
    def __init__(self, storage: MemoryStorage, preprocessor: BackgroundPreprocessor, *args, **kwargs):
        self.storage = storage
        self.preprocessor = preprocessor
        self._request_times = []
        self._lock = Lock()
        super().__init__(*args, **kwargs)
    
    def do_GET(self):
        start_time = time.time()
        
        try:
            parsed_path = urlparse(self.path)
            path = parsed_path.path
            
            # Route requests with optimized handlers
            if path == '/api/summaries':
                self._handle_summaries(parsed_path)
            elif path == '/api/stats':
                self._handle_stats()
            elif path == '/api/progress':
                self._handle_progress()
            elif path == '/api/status':
                self._handle_status()
            elif path == '/api/messages':
                self._handle_messages(parsed_path)
            elif path == '/api/health':
                self._handle_health()
            elif path == '/api/performance':
                self._handle_performance()
            elif path == '/api/refresh':
                self._handle_refresh()
            elif path == '/':
                self._handle_root()
            elif path == '/api/debug/storage':
                self._handle_debug_storage()
            else:
                self.send_response(404)
                self.end_headers()
        
        except Exception as e:
            logger.error(f"Request handling error: {e}")
            self.send_response(500)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({'error': 'Internal server error'}).encode())
        
        finally:
            # Track request performance
            request_time = time.time() - start_time
            with self._lock:
                self._request_times.append(request_time)
                # Keep only recent 100 requests
                if len(self._request_times) > 100:
                    self._request_times.pop(0)

    def _handle_debug_storage(self):
        """Debug endpoint to see what's actually in storage"""
        debug_info = {
            'total_chats_with_messages': len(self.storage.messages),
            'total_chats_with_summaries': len(self.storage.summaries),
            'messages_by_chat': {chat_id: len(msgs) for chat_id, msgs in self.storage.messages.items()},
            'summaries_by_chat': {chat_id: len(summaries) for chat_id, summaries in self.storage.summaries.items()},
            'consolidated_summary': bool(self.storage.consolidated_summary),
            'consolidated_timestamp': self.storage.consolidated_timestamp.isoformat() if self.storage.consolidated_timestamp else None
        }
        self._send_json_response(200, debug_info)

    def _handle_summaries(self, parsed_path):
        """Handle summaries with cached data"""
        query_params = parse_qs(parsed_path.query)
        summary_type = query_params.get('type', ['individual'])[0]
        
        try:
            if summary_type == 'consolidated':
                data = self._get_consolidated_summary()
            else:
                data = self.preprocessor.get_cached_data("summaries") or []
            
            # Return data directly without wrapping
            self._send_json_response(200, data)
            
        except Exception as e:
            logger.error(f"Error in summaries endpoint: {e}")
            self._send_json_response(500, {'error': str(e)})

    def _handle_stats(self):
        """Handle stats with cached data"""
        try:
            data = self.preprocessor.get_cached_data("stats") or {}
            # Return stats data directly
            self._send_json_response(200, data)
        except Exception as e:
            logger.error(f"Error in stats endpoint: {e}")
            self._send_json_response(500, {'error': str(e)})

    def _handle_progress(self):
        """Handle progress with real-time data"""
        try:
            progress = self._get_detailed_progress()
            # Return progress data directly
            self._send_json_response(200, progress)
        except Exception as e:
            logger.error(f"Error in progress endpoint: {e}")
            self._send_json_response(500, {'error': str(e)})

    def _handle_status(self):
        """Handle system status"""
        status = self._get_system_status()
        self._send_json_response(200, status)
    
    def _handle_messages(self, parsed_path):
        """Handle messages with cached data"""
        try:
            query_params = parse_qs(parsed_path.query)
            chat_id = query_params.get('chat_id', [None])[0]
            
            if chat_id:
                result = self._get_chat_messages(chat_id)
            else:
                result = self.preprocessor.get_cached_data("messages") or {}
            
            # Return messages data directly
            self._send_json_response(200, result)
        except Exception as e:
            logger.error(f"Error in messages endpoint: {e}")
            self._send_json_response(500, {'error': str(e)})
    
    def _handle_health(self):
        """Quick health check endpoint"""
        self._send_json_response(200, {
            'status': 'healthy',
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'free_threaded': IS_FREE_THREADED,
            'performance': self._get_request_stats()
        })
    
    def _handle_performance(self):
        """Performance metrics endpoint"""
        self._send_json_response(200, {
            'preprocessor': self.preprocessor.get_performance_stats(),
            'requests': self._get_request_stats(),
            'system': {
                'free_threaded': IS_FREE_THREADED,
                'python_version': os.environ.get('PYTHON_VERSION', 'unknown'),
                'timestamp': datetime.now(timezone.utc).isoformat()
            }
        })
    
    def _handle_refresh(self):
        """Manual refresh trigger endpoint"""
        try:
            # Trigger background processing if available
            if hasattr(self.storage, 'trigger_processing'):
                self.storage.trigger_processing()
            
            self._send_json_response(200, {
                'status': 'refresh_triggered',
                'message': 'Background processing initiated',
                'timestamp': datetime.now(timezone.utc).isoformat()
            })
        except Exception as e:
            logger.error(f"Error in refresh endpoint: {e}")
            self._send_json_response(500, {'error': str(e)})
    
    def _handle_root(self):
        """Serve HTML interface"""
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()
        
        html = self._generate_enhanced_html_interface()
        self.wfile.write(html.encode())
    
    def _send_json_response(self, status_code: int, data: Any):
        """Send JSON response with optimized headers"""
        self.send_response(status_code)
        self.send_header('Content-type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Cache-Control', 'no-cache, must-revalidate')
        self.send_header('X-Performance-Mode', 'free-threaded' if IS_FREE_THREADED else 'gil')
        self.end_headers()
        
        self.wfile.write(json.dumps(data, separators=(',', ':')).encode())  # Compact JSON
    
    def _get_request_stats(self) -> Dict[str, Any]:
        """Get request performance statistics"""
        with self._lock:
            if not self._request_times:
                return {'avg_response_time': 0, 'total_requests': 0}
            
            avg_time = sum(self._request_times) / len(self._request_times)
            return {
                'avg_response_time_ms': round(avg_time * 1000, 2),
                'total_requests_tracked': len(self._request_times),
                'min_response_time_ms': round(min(self._request_times) * 1000, 2),
                'max_response_time_ms': round(max(self._request_times) * 1000, 2)
            }
    
    def _get_consolidated_summary(self) -> list:
        """Get consolidated summary data"""
        if self.storage.consolidated_summary:
            chat_info = self.storage.get_chat_info()
            return [{
                'type': 'consolidated',
                'summary': self.storage.consolidated_summary,
                'timestamp': (self.storage.consolidated_timestamp or datetime.now(timezone.utc)).isoformat(),
                'total_chats': chat_info['total_chats'],
                'total_messages': chat_info['total_messages'],
                'chats_with_summaries': chat_info['chats_with_summaries']
            }]
        else:
            return [{
                'type': 'consolidated',
                'summary': 'No consolidated summary available yet. The system is still processing individual chats.',
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'total_chats': 0,
                'total_messages': 0,
                'chats_with_summaries': 0
            }]
    
    def _get_chat_messages(self, chat_id: str) -> dict:
        """Get messages for specific chat"""
        if chat_id in self.storage.messages:
            messages = self.storage.messages[chat_id]
            unique_messages = self.preprocessor._get_unique_messages(chat_id, messages)
            return {
                'chat_id': chat_id,
                'chat_title': messages[0].chat_title if messages else 'Unknown',
                'messages': [
                    {
                        'id': msg.id,
                        'text': msg.text,
                        'timestamp': msg.timestamp.isoformat(),
                        'sender': msg.sender_name,
                        'is_unread': getattr(msg, 'is_unread', False)
                    }
                    for msg in unique_messages
                ]
            }
        else:
            return {'error': 'Chat not found'}
    
    def _get_detailed_progress(self) -> dict:
        """Get detailed progress information"""
        chat_info = self.storage.get_chat_info()
        progress_data = self.storage.get_processing_stats()
        
        total_chats = chat_info['total_chats']
        chats_with_summaries = chat_info['chats_with_summaries']
        
        if total_chats == 0:
            progress_percent = 0
            status = 'waiting'
            stage = 'collecting'
        else:
            progress_percent = progress_data['progress_percent']
            status = progress_data['status']
            stage = 'summarizing' if progress_percent < 100 else 'complete'
        
        cached_stats = self.preprocessor.get_cached_data("stats") or {}
        total_messages = cached_stats.get('total_messages', 0)
        
        eta_seconds = None
        if progress_percent > 0 and progress_percent < 100:
            time_elapsed = progress_data['processing_time']
            eta_seconds = (time_elapsed / progress_percent) * (100 - progress_percent)
        
        return {
            'total_chats': total_chats,
            'processed_chats': chats_with_summaries,
            'progress_percent': progress_percent,
            'status': status,
            'stage': stage,
            'total_messages_collected': total_messages,
            'collection_active': total_messages > 0,
            'eta_seconds': eta_seconds,
            'eta_formatted': self._format_eta(eta_seconds) if eta_seconds else 'Calculating...',
            'processing_time_seconds': progress_data['processing_time'],
            'last_update': progress_data['last_update'],
            'consolidated_ready': self.storage.consolidated_summary is not None,
            'consolidated_timestamp': self.storage.consolidated_timestamp.isoformat() if self.storage.consolidated_timestamp else None,
            'chats_being_processed': max(0, total_chats - chats_with_summaries),
            'completion_estimate': self._get_completion_estimate(progress_percent, stage),
            'performance_mode': 'free-threaded' if IS_FREE_THREADED else 'standard'
        }
    
    def _get_system_status(self) -> dict:
        """Get system status information"""
        progress_data = self.storage.get_processing_stats()
        cached_stats = self.preprocessor.get_cached_data("stats") or {}
        total_messages = cached_stats.get('total_messages', 0)
        
        if total_messages == 0:
            health = 'initializing'
            message = 'System is starting up and collecting messages...'
        elif progress_data['status'] == 'complete':
            health = 'healthy'
            message = 'All chats have been processed and summarized'
        elif progress_data['progress_percent'] > 70:
            health = 'healthy'
            message = f'System is running well, {total_messages} messages collected'
        else:
            health = 'processing'
            message = f'System is processing, {total_messages} messages collected'
        
        return {
            'health': health,
            'message': message,
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'real_time_stats': {
                'total_messages': total_messages,
                'total_chats': len(self.storage.messages),
                'collection_active': True,
                'free_threaded_enabled': IS_FREE_THREADED,
                'cache_performance': self.preprocessor.cache.get_stats()['hit_rate']
            },
            'version': '2.0.0',
            'features': {
                'individual_summaries': True,
                'consolidated_overview': True,
                'real_time_updates': True,
                'progress_tracking': True,
                'duplicate_removal': True,
                'all_messages_display': True,
                'python_3_14_free_threaded': IS_FREE_THREADED,
                'adaptive_caching': True,
                'background_preprocessing': True,
                'performance_monitoring': True
            }
        }
    
    def _format_eta(self, seconds: float) -> str:
        """Format ETA for display"""
        if seconds is None:
            return "Unknown"
        if seconds < 60:
            return f"{int(seconds)}s"
        elif seconds < 3600:
            minutes = int(seconds / 60)
            return f"{minutes}m"
        else:
            hours = int(seconds / 3600)
            minutes = int((seconds % 3600) / 60)
            return f"{hours}h {minutes}m"
    
    def _get_completion_estimate(self, progress_percent: int, stage: str) -> str:
        """Get completion estimate message"""
        estimates = {
            'collecting': 'Collecting messages from Telegram...',
            'summarizing': 'Generating AI summaries...',
            'complete': 'All processing complete!'
        }
        return estimates.get(stage, 'Processing...')
    
    def _generate_enhanced_html_interface(self) -> str:
        return """
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Telegram AI Summarizer - Real-time Progress</title>
            <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
            <style>
                .performance-badge {
                    position: fixed;
                    bottom: 10px;
                    right: 10px;
                    background: var(--secondary);
                    color: white;
                    padding: 5px 10px;
                    border-radius: 12px;
                    font-size: 0.7rem;
                    z-index: 1000;
                }
                .performance-badge.free-threaded {
                    background: #10b981;
                }
                .performance-badge.standard {
                    background: #f59e0b;
                }
                
                * {
                    margin: 0;
                    padding: 0;
                    box-sizing: border-box;
                }

                :root {
                    --primary: #6366f1;
                    --primary-dark: #4f46e5;
                    --secondary: #10b981;
                    --warning: #f59e0b;
                    --error: #ef4444;
                    --background: #0f172a;
                    --surface: #1e293b;
                    --surface-light: #334155;
                    --text: #f8fafc;
                    --text-secondary: #cbd5e1;
                    --border: #475569;
                }

                body {
                    font-family: 'Segoe UI', system-ui, -apple-system, sans-serif;
                    background: linear-gradient(135deg, var(--background) 0%, #1e1b4b 100%);
                    color: var(--text);
                    min-height: 100vh;
                    line-height: 1.6;
                }

                .container {
                    max-width: 1400px;
                    margin: 0 auto;
                    padding: 20px;
                }

                .header {
                    text-align: center;
                    padding: 40px 20px;
                    background: linear-gradient(135deg, var(--primary) 0%, var(--primary-dark) 100%);
                    border-radius: 20px;
                    margin-bottom: 30px;
                    box-shadow: 0 20px 40px rgba(0,0,0,0.3);
                    position: relative;
                    overflow: hidden;
                }

                .header::before {
                    content: '';
                    position: absolute;
                    top: 0;
                    left: 0;
                    right: 0;
                    bottom: 0;
                    background: url('data:image/svg+xml,<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 100 100"><defs><pattern id="grid" width="10" height="10" patternUnits="userSpaceOnUse"><path d="M 10 0 L 0 0 0 10" fill="none" stroke="rgba(255,255,255,0.1)" stroke-width="1"/></pattern></defs><rect width="100" height="100" fill="url(%23grid)"/></svg>');
                }

                .header-content {
                    position: relative;
                    z-index: 2;
                }

                .logo {
                    font-size: 3.5rem;
                    margin-bottom: 15px;
                    display: block;
                }

                h1 {
                    font-size: 3rem;
                    font-weight: 800;
                    margin-bottom: 10px;
                    background: linear-gradient(135deg, #fff 0%, #e2e8f0 100%);
                    -webkit-background-clip: text;
                    -webkit-text-fill-color: transparent;
                    background-clip: text;
                }

                .subtitle {
                    font-size: 1.2rem;
                    color: var(--text-secondary);
                    margin-bottom: 20px;
                }

                .python-badge {
                    background: rgba(255,255,255,0.2);
                    padding: 8px 16px;
                    border-radius: 20px;
                    font-size: 0.9rem;
                    backdrop-filter: blur(10px);
                    border: 1px solid rgba(255,255,255,0.3);
                }

                .dashboard {
                    display: grid;
                    grid-template-columns: 350px 1fr;
                    gap: 25px;
                    margin-bottom: 30px;
                }

                .sidebar {
                    background: var(--surface);
                    border-radius: 16px;
                    padding: 25px;
                    box-shadow: 0 8px 32px rgba(0,0,0,0.3);
                    border: 1px solid var(--border);
                }

                .main-content {
                    background: var(--surface);
                    border-radius: 16px;
                    padding: 25px;
                    box-shadow: 0 8px 32px rgba(0,0,0,0.3);
                    border: 1px solid var(--border);
                }

                .progress-section {
                    margin-bottom: 25px;
                }

                .progress-header {
                    display: flex;
                    justify-content: space-between;
                    align-items: center;
                    margin-bottom: 15px;
                }

                .progress-title {
                    font-size: 1.1rem;
                    font-weight: 600;
                }

                .progress-percent {
                    font-size: 1.5rem;
                    font-weight: 700;
                    color: var(--secondary);
                }

                .progress-bar {
                    background: var(--surface-light);
                    height: 16px;
                    border-radius: 8px;
                    overflow: hidden;
                    margin-bottom: 10px;
                    position: relative;
                }

                .progress-fill {
                    height: 100%;
                    background: linear-gradient(90deg, var(--primary) 0%, var(--secondary) 100%);
                    border-radius: 8px;
                    transition: width 0.5s ease;
                    position: relative;
                    overflow: hidden;
                }

                .progress-fill::after {
                    content: '';
                    position: absolute;
                    top: 0;
                    left: -100%;
                    width: 100%;
                    height: 100%;
                    background: linear-gradient(90deg, transparent, rgba(255,255,255,0.4), transparent);
                    animation: shimmer 2s infinite;
                }

                @keyframes shimmer {
                    0% { left: -100%; }
                    100% { left: 100%; }
                }

                .progress-details {
                    display: grid;
                    grid-template-columns: 1fr 1fr;
                    gap: 10px;
                    margin-top: 15px;
                }

                .progress-item {
                    text-align: center;
                    padding: 12px;
                    background: var(--surface-light);
                    border-radius: 8px;
                    border: 1px solid var(--border);
                }

                .progress-number {
                    font-size: 1.5rem;
                    font-weight: 700;
                    margin-bottom: 5px;
                }

                .progress-label {
                    font-size: 0.8rem;
                    color: var(--text-secondary);
                    text-transform: uppercase;
                    letter-spacing: 0.5px;
                }

                .stats-grid {
                    display: grid;
                    grid-template-columns: 1fr 1fr;
                    gap: 15px;
                    margin-bottom: 25px;
                }

                .stat-card {
                    background: var(--surface-light);
                    padding: 20px;
                    border-radius: 12px;
                    text-align: center;
                    border: 1px solid var(--border);
                    transition: transform 0.2s, box-shadow 0.2s;
                }

                .stat-card:hover {
                    transform: translateY(-2px);
                    box-shadow: 0 8px 25px rgba(0,0,0,0.4);
                }

                .stat-number {
                    font-size: 2rem;
                    font-weight: 800;
                    margin-bottom: 5px;
                }

                .stat-number.total-chats { color: var(--primary); }
                .stat-number.processed { color: var(--secondary); }
                .stat-number.messages { color: #8b5cf6; }
                .stat-number.consolidated { color: var(--warning); }

                .stat-label {
                    font-size: 0.9rem;
                    color: var(--text-secondary);
                    text-transform: uppercase;
                    letter-spacing: 0.5px;
                }

                .status-section {
                    margin-top: 20px;
                }

                .status-indicator {
                    display: flex;
                    align-items: center;
                    gap: 10px;
                    padding: 12px;
                    border-radius: 10px;
                    font-size: 0.9rem;
                    font-weight: 600;
                }

                .status-healthy {
                    background: rgba(16, 185, 129, 0.2);
                    color: var(--secondary);
                    border: 1px solid var(--secondary);
                }

                .status-processing {
                    background: rgba(245, 158, 11, 0.2);
                    color: var(--warning);
                    border: 1px solid var(--warning);
                }

                .status-initializing {
                    background: rgba(99, 102, 241, 0.2);
                    color: var(--primary);
                    border: 1px solid var(--primary);
                }

                .pulse {
                    animation: pulse 2s infinite;
                }

                @keyframes pulse {
                    0% { opacity: 1; }
                    50% { opacity: 0.5; }
                    100% { opacity: 1; }
                }

                .controls {
                    display: grid;
                    grid-template-columns: 1fr 1fr 1fr 1fr;
                    gap: 12px;
                    margin-bottom: 25px;
                }

                .btn {
                    background: var(--surface-light);
                    color: var(--text);
                    border: 1px solid var(--border);
                    padding: 12px 16px;
                    border-radius: 10px;
                    cursor: pointer;
                    font-size: 14px;
                    font-weight: 600;
                    transition: all 0.2s;
                    display: flex;
                    align-items: center;
                    justify-content: center;
                    gap: 8px;
                }

                .btn:hover {
                    background: var(--primary);
                    transform: translateY(-1px);
                    box-shadow: 0 4px 12px rgba(99, 102, 241, 0.4);
                }

                .btn.active {
                    background: var(--primary);
                    border-color: var(--primary);
                }

                .btn i {
                    font-size: 16px;
                }

                .eta-display {
                    text-align: center;
                    padding: 15px;
                    background: var(--surface-light);
                    border-radius: 10px;
                    margin-bottom: 20px;
                    border: 1px solid var(--border);
                }

                .eta-title {
                    font-size: 0.9rem;
                    color: var(--text-secondary);
                    margin-bottom: 5px;
                }

                .eta-value {
                    font-size: 1.2rem;
                    font-weight: 700;
                    color: var(--secondary);
                }

                .chat-list {
                    max-height: 600px;
                    overflow-y: auto;
                }

                .chat-item {
                    background: var(--surface-light);
                    border: 1px solid var(--border);
                    border-radius: 12px;
                    padding: 20px;
                    margin-bottom: 15px;
                    transition: all 0.2s;
                }

                .chat-item:hover {
                    transform: translateY(-2px);
                    box-shadow: 0 8px 25px rgba(0,0,0,0.4);
                    border-color: var(--primary);
                }

                .chat-header {
                    display: flex;
                    justify-content: space-between;
                    align-items: flex-start;
                    margin-bottom: 12px;
                }

                .chat-title {
                    font-size: 1.1rem;
                    font-weight: 600;
                    color: var(--text);
                    margin-bottom: 5px;
                }

                .chat-meta {
                    display: flex;
                    gap: 15px;
                    font-size: 0.85rem;
                    color: var(--text-secondary);
                }

                .chat-badge {
                    padding: 4px 8px;
                    border-radius: 6px;
                    font-size: 0.75rem;
                    font-weight: 600;
                }

                .badge-summarized {
                    background: var(--secondary);
                    color: white;
                }

                .badge-processing {
                    background: var(--warning);
                    color: white;
                }

                .badge-pending {
                    background: var(--surface);
                    color: var(--text-secondary);
                    border: 1px solid var(--border);
                }

                .chat-summary {
                    color: var(--text-secondary);
                    line-height: 1.5;
                    white-space: pre-wrap;
                }

                .consolidated-view {
                    background: linear-gradient(135deg, var(--surface) 0%, var(--surface-light) 100%);
                    border: 1px solid var(--border);
                    border-radius: 16px;
                    padding: 30px;
                    margin-bottom: 20px;
                }

                .consolidated-header {
                    display: flex;
                    align-items: center;
                    gap: 12px;
                    margin-bottom: 20px;
                }

                .consolidated-header i {
                    font-size: 2rem;
                    color: var(--secondary);
                }

                .consolidated-content {
                    background: rgba(0,0,0,0.3);
                    padding: 20px;
                    border-radius: 12px;
                    border: 1px solid var(--border);
                }

                .loading {
                    text-align: center;
                    padding: 40px;
                    color: var(--text-secondary);
                }

                .loading-spinner {
                    font-size: 2rem;
                    margin-bottom: 15px;
                    animation: spin 1s linear infinite;
                }

                @keyframes spin {
                    from { transform: rotate(0deg); }
                    to { transform: rotate(360deg); }
                }

                /* Message View Styles */
                .message-view {
                    max-height: 600px;
                    overflow-y: auto;
                }
                
                .message-item {
                    background: var(--surface-light);
                    border: 1px solid var(--border);
                    border-radius: 8px;
                    padding: 12px;
                    margin-bottom: 8px;
                    transition: all 0.2s;
                }
                
                .message-item.unread {
                    border-left: 4px solid var(--secondary);
                    background: rgba(16, 185, 129, 0.1);
                }
                
                .message-header {
                    display: flex;
                    justify-content: space-between;
                    margin-bottom: 8px;
                    font-size: 0.85rem;
                }
                
                .message-sender {
                    font-weight: 600;
                    color: var(--primary);
                }
                
                .message-time {
                    color: var(--text-secondary);
                }
                
                .message-content {
                    line-height: 1.4;
                }
                
                .unread-badge {
                    background: var(--secondary);
                    color: white;
                    padding: 2px 6px;
                    border-radius: 4px;
                    font-size: 0.7rem;
                    margin-left: 8px;
                }

                /* Scrollbar styling */
                ::-webkit-scrollbar {
                    width: 8px;
                }

                ::-webkit-scrollbar-track {
                    background: var(--surface-light);
                    border-radius: 4px;
                }

                ::-webkit-scrollbar-thumb {
                    background: var(--primary);
                    border-radius: 4px;
                }

                ::-webkit-scrollbar-thumb:hover {
                    background: var(--primary-dark);
                }

                /* Responsive design */
                @media (max-width: 1024px) {
                    .dashboard {
                        grid-template-columns: 1fr;
                    }
                    
                    .controls {
                        grid-template-columns: 1fr 1fr;
                    }
                }

                @media (max-width: 768px) {
                    .stats-grid {
                        grid-template-columns: 1fr;
                    }
                    
                    .controls {
                        grid-template-columns: 1fr;
                    }
                    
                    h1 {
                        font-size: 2rem;
                    }
                    
                    .progress-details {
                        grid-template-columns: 1fr;
                    }
                }
            </style>
        </head>
        <body>
            <div class="performance-badge" id="performanceBadge">
                <i class="fas fa-bolt"></i> <span id="threadingMode">Python 3.14 Threading</span>
            </div>
            
            <div class="container">
                <div class="header">
                    <div class="header-content">
                        <i class="fas fa-robot logo"></i>
                        <h1>Telegram AI Summarizer</h1>
                        <p class="subtitle">Real-time Progress Tracking & AI-Powered Summaries</p>
                        <span class="python-badge">Python 3.14 • Multi-threaded • Live Updates</span>
                    </div>
                </div>

                <div class="dashboard">
                    <div class="sidebar">
                        <div class="progress-section">
                            <div class="progress-header">
                                <div class="progress-title">Processing Progress</div>
                                <div class="progress-percent" id="progressPercent">0%</div>
                            </div>
                            <div class="progress-bar">
                                <div class="progress-fill" id="progressFill" style="width: 0%"></div>
                            </div>
                            
                            <div class="progress-details">
                                <div class="progress-item">
                                    <div class="progress-number" id="processedCount">0</div>
                                    <div class="progress-label">Processed</div>
                                </div>
                                <div class="progress-item">
                                    <div class="progress-number" id="totalCount">0</div>
                                    <div class="progress-label">Total Chats</div>
                                </div>
                            </div>
                        </div>

                        <div class="eta-display">
                            <div class="eta-title">Estimated Completion</div>
                            <div class="eta-value" id="etaValue">Calculating...</div>
                        </div>

                        <div class="stats-grid">
                            <div class="stat-card">
                                <div class="stat-number total-chats" id="totalChats">0</div>
                                <div class="stat-label">Total Chats</div>
                            </div>
                            <div class="stat-card">
                                <div class="stat-number processed" id="processedChats">0</div>
                                <div class="stat-label">Processed</div>
                            </div>
                            <div class="stat-card">
                                <div class="stat-number messages" id="totalMessages">0</div>
                                <div class="stat-label">Messages</div>
                            </div>
                            <div class="stat-card">
                                <div class="stat-number consolidated" id="consolidatedStatus">No</div>
                                <div class="stat-label">Consolidated Ready</div>
                            </div>
                        </div>

                        <div class="status-section">
                            <div class="status-indicator status-initializing" id="statusIndicator">
                                <i class="fas fa-circle pulse"></i>
                                <span id="statusText">Initializing System</span>
                            </div>
                            <div style="margin-top: 10px; font-size: 0.8rem; color: var(--text-secondary); text-align: center;">
                                Last Update: <span id="lastUpdateTime">Just now</span>
                            </div>
                        </div>
                    </div>

                    <div class="main-content">
                        <div class="controls">
                            <button class="btn active" onclick="switchView('individual')">
                                <i class="fas fa-comments"></i>
                                Summaries
                            </button>
                            <button class="btn" onclick="switchView('messages')">
                                <i class="fas fa-envelope"></i>
                                All Messages
                            </button>
                            <button class="btn" onclick="switchView('consolidated')">
                                <i class="fas fa-globe"></i>
                                Consolidated
                            </button>
                            <button class="btn" onclick="refreshData()">
                                <i class="fas fa-sync-alt"></i>
                                Refresh
                            </button>
                            <button class="btn" onclick="toggleAutoRefresh()">
                                <i class="fas fa-play" id="autoRefreshIcon"></i>
                                <span id="autoRefreshText">Auto: On</span>
                            </button>
                        </div>

                        <div id="content">
                            <div class="loading">
                                <i class="fas fa-robot loading-spinner"></i>
                                <p>Loading your Telegram data...</p>
                                <p style="margin-top: 10px; font-size: 0.9rem;" id="loadingDetails">
                                    Collecting messages and generating AI summaries
                                </p>
                            </div>
                        </div>
                    </div>
                </div>
            </div>

            <script>
                let currentView = 'individual';
                let autoRefresh = true;
                let refreshInterval;
                let lastRefreshTime = performance.now();

                // DOM elements
                const contentEl = document.getElementById('content');
                const progressFillEl = document.getElementById('progressFill');
                const progressPercentEl = document.getElementById('progressPercent');
                const processedCountEl = document.getElementById('processedCount');
                const totalCountEl = document.getElementById('totalCount');
                const totalChatsEl = document.getElementById('totalChats');
                const processedChatsEl = document.getElementById('processedChats');
                const totalMessagesEl = document.getElementById('totalMessages');
                const consolidatedStatusEl = document.getElementById('consolidatedStatus');
                const statusIndicatorEl = document.getElementById('statusIndicator');
                const statusTextEl = document.getElementById('statusText');
                const etaValueEl = document.getElementById('etaValue');
                const lastUpdateTimeEl = document.getElementById('lastUpdateTime');
                const loadingDetailsEl = document.getElementById('loadingDetails');
                const autoRefreshIconEl = document.getElementById('autoRefreshIcon');
                const autoRefreshTextEl = document.getElementById('autoRefreshText');

                // Enhanced JavaScript with threading optimizations
                class ConcurrentDataLoader {
                    constructor() {
                        this.cache = new Map();
                        this.pendingRequests = new Map();
                    }

                    async loadConcurrent(urls) {
                        // Load multiple endpoints concurrently
                        const requests = urls.map(url => this._fetchWithCache(url));
                        return Promise.all(requests);
                    }

                    async _fetchWithCache(url) {
                        const cacheKey = url;
                        const cacheTime = 1000; // 1 second cache
                        
                        if (this.cache.has(cacheKey)) {
                            const { data, timestamp } = this.cache.get(cacheKey);
                            if (Date.now() - timestamp < cacheTime) {
                                return data;
                            }
                        }

                        // Debounce rapid requests
                        if (this.pendingRequests.has(cacheKey)) {
                            return this.pendingRequests.get(cacheKey);
                        }

                        const requestPromise = fetch(url)
                            .then(response => {
                                if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
                                return response.json();
                            })
                            .then(data => {
                                this.cache.set(cacheKey, { data, timestamp: Date.now() });
                                this.pendingRequests.delete(cacheKey);
                                return data;
                            })
                            .catch(error => {
                                this.pendingRequests.delete(cacheKey);
                                throw error;
                            });

                        this.pendingRequests.set(cacheKey, requestPromise);
                        return requestPromise;
                    }

                    clearCache() {
                        this.cache.clear();
                    }
                }

                // Initialize concurrent loader
                const concurrentLoader = new ConcurrentDataLoader();

                function switchView(viewType) {
                    currentView = viewType;
                    // Update button states
                    document.querySelectorAll('.btn').forEach(btn => {
                        btn.classList.remove('active');
                    });
                    event.target.classList.add('active');
                    
                    refreshData();
                }

                async function refreshDataConcurrent() {
                    lastRefreshTime = performance.now();
                    
                    // Build URLs based on current view
                    let contentUrl;
                    if (currentView === 'messages') {
                        contentUrl = '/api/messages';
                    } else if (currentView === 'consolidated') {
                        contentUrl = '/api/summaries?type=consolidated';
                    } else {
                        contentUrl = '/api/summaries?type=individual';
                    }
                    
                    const urls = [contentUrl, '/api/stats', '/api/progress'];

                    try {
                        const [contentData, statsData, progressData] = await concurrentLoader.loadConcurrent(urls);
                        
                        // Update UI concurrently
                        await Promise.all([
                            updateContent(contentData),
                            updateStats(statsData),
                            updateProgress(progressData)
                        ]);

                        updateLastUpdateTime();
                        showNotification('Data refreshed successfully!', 'success');
                        
                    } catch (error) {
                        console.error('Concurrent load error:', error);
                        
                        // Try sequential loading as fallback
                        try {
                            console.log('Trying sequential loading...');
                            const contentResponse = await fetch(contentUrl);
                            const statsResponse = await fetch('/api/stats');
                            const progressResponse = await fetch('/api/progress');
                            
                            if (!contentResponse.ok || !statsResponse.ok || !progressResponse.ok) {
                                throw new Error('One or more API calls failed');
                            }
                            
                            const contentData = await contentResponse.json();
                            const statsData = await statsResponse.json();
                            const progressData = await progressResponse.json();
                            
                            await updateContent(contentData);
                            await updateStats(statsData);
                            await updateProgress(progressData);
                            
                            updateLastUpdateTime();
                            showNotification('Data loaded (sequential fallback)', 'warning');
                            
                        } catch (fallbackError) {
                            console.error('Sequential loading also failed:', fallbackError);
                            showNotification('Refresh failed - server may be busy', 'error');
                            
                            // Show offline status
                            statusIndicatorEl.className = 'status-indicator status-initializing';
                            statusIndicatorEl.innerHTML = '<i class="fas fa-wifi-slash"></i><span>Connection Issues</span>';
                        }
                    }
                }


                // Replace the original refreshData function
                function refreshData() {
                    refreshDataConcurrent();
                }

                function toggleAutoRefresh() {
                    autoRefresh = !autoRefresh;
                    if (autoRefresh) {
                        autoRefreshIconEl.className = 'fas fa-play';
                        autoRefreshTextEl.textContent = 'Auto: On';
                        showNotification('Auto-refresh enabled', 'success');
                    } else {
                        autoRefreshIconEl.className = 'fas fa-pause';
                        autoRefreshTextEl.textContent = 'Auto: Off';
                        showNotification('Auto-refresh disabled', 'warning');
                    }
                }

                function showNotification(message, type = 'info') {
                    // Simple notification - you could enhance this with a proper notification system
                    console.log(`${type.toUpperCase()}: ${message}`);
                }

                function updateLastUpdateTime() {
                    const now = new Date();
                    lastUpdateTimeEl.textContent = now.toLocaleTimeString();
                }

                function updateContent(data) {
                    // Extract the actual data from the response object
                    const contentData = data.data || data;
                    
                    if (!contentData || (Array.isArray(contentData) && contentData.length === 0) || (typeof contentData === 'object' && contentData.messages && contentData.messages.length === 0)) {
                        contentEl.innerHTML = `
                            <div class="loading">
                                <i class="fas fa-inbox"></i>
                                <p>No data found yet. The system is collecting messages from your Telegram account...</p>
                                <p style="margin-top: 10px; font-size: 0.9rem;">
                                    This may take a few moments depending on the number of chats.
                                </p>
                            </div>
                        `;
                        return;
                    }

                    if (currentView === 'messages') {
                        renderMessagesView(contentData);
                    } else if (currentView === 'consolidated') {
                        // Handle consolidated view - it might be an array or object
                        if (Array.isArray(contentData) && contentData[0] && contentData[0].type === 'consolidated') {
                            const consolidated = contentData[0];
                            contentEl.innerHTML = `
                                <div class="consolidated-view">
                                    <div class="consolidated-header">
                                        <i class="fas fa-globe-americas"></i>
                                        <h2>Consolidated Overview</h2>
                                    </div>
                                    <div class="consolidated-content">
                                        <div class="chat-summary">${escapeHtml(consolidated.summary)}</div>
                                        <div style="margin-top: 15px; font-size: 0.9rem; color: #94a3b8;">
                                            📊 Based on ${consolidated.chats_with_summaries} of ${consolidated.total_chats} chats • 
                                            ${consolidated.total_messages} messages analyzed • 
                                            Updated: ${new Date(consolidated.timestamp).toLocaleString()}
                                        </div>
                                    </div>
                                </div>
                            `;
                        } else if (contentData.type === 'consolidated') {
                            // Handle single consolidated object
                            const consolidated = contentData;
                            contentEl.innerHTML = `
                                <div class="consolidated-view">
                                    <div class="consolidated-header">
                                        <i class="fas fa-globe-americas"></i>
                                        <h2>Consolidated Overview</h2>
                                    </div>
                                    <div class="consolidated-content">
                                        <div class="chat-summary">${escapeHtml(consolidated.summary)}</div>
                                        <div style="margin-top: 15px; font-size: 0.9rem; color: #94a3b8;">
                                            📊 Based on ${consolidated.chats_with_summaries} of ${consolidated.total_chats} chats • 
                                            ${consolidated.total_messages} messages analyzed • 
                                            Updated: ${new Date(consolidated.timestamp).toLocaleString()}
                                        </div>
                                    </div>
                                </div>
                            `;
                        } else {
                            contentEl.innerHTML = `
                                <div class="loading">
                                    <i class="fas fa-info-circle"></i>
                                    <p>No consolidated summary available yet.</p>
                                </div>
                            `;
                        }
                    } else {
                        renderSummariesView(contentData);
                    }
                }

                // Replace the renderSummariesView function with this fixed version:
                function renderSummariesView(data) {
                    let html = '<div class="chat-list">';
                    
                    // Ensure data is an array
                    const chatData = Array.isArray(data) ? data : [data];
                    
                    chatData.forEach(chat => {
                        // Handle both object and array response formats
                        const hasSummary = chat.has_summary || (chat.summary && chat.summary !== 'Collecting messages...');
                        const needsRefresh = chat.needs_refresh;
                        
                        let badgeClass, badgeText;
                        if (hasSummary) {
                            if (needsRefresh) {
                                badgeClass = 'chat-badge badge-processing';
                                badgeText = 'Needs Refresh';
                            } else {
                                badgeClass = 'chat-badge badge-summarized';
                                badgeText = 'Summarized';
                            }
                        } else {
                            badgeClass = 'chat-badge badge-pending';
                            badgeText = 'Processing';
                        }
                        
                        const summaryAge = chat.summary_age_minutes ? 
                            ` • ${chat.summary_age_minutes}m ago` : '';
                        
                        html += `
                            <div class="chat-item">
                                <div class="chat-header">
                                    <div style="flex: 1;">
                                        <div class="chat-title">${escapeHtml(chat.chat_title || 'Unknown Chat')}</div>
                                        <div class="chat-meta">
                                            <span><i class="fas fa-message"></i> ${chat.message_count || 0} messages</span>
                                            <span><i class="fas fa-clock"></i> ${new Date(chat.last_updated || Date.now()).toLocaleString()}${summaryAge}</span>
                                        </div>
                                    </div>
                                    <span class="${badgeClass}">${badgeText}</span>
                                </div>
                                <div class="chat-summary">${escapeHtml(chat.summary || 'No summary available yet.')}</div>
                            </div>
                        `;
                    });
                    
                    html += '</div>';
                    contentEl.innerHTML = html;
                }

                // Replace the renderMessagesView function with this fixed version:
                function renderMessagesView(messagesData) {
                    let html = '<div class="message-view">';
                    
                    // Extract messages array from the response
                    const messages = messagesData.messages || messagesData;
                    
                    if (!messages || (Array.isArray(messages) && messages.length === 0)) {
                        html = '<div class="loading"><p>No messages found yet. Collecting messages...</p></div>';
                    } else {
                        // Ensure messages is an array
                        const messageArray = Array.isArray(messages) ? messages : [messages];
                        
                        messageArray.forEach(message => {
                            const messageClass = message.is_unread ? 'message-item unread' : 'message-item';
                            html += `
                                <div class="${messageClass}">
                                    <div class="message-header">
                                        <div>
                                            <span class="message-sender">${escapeHtml(message.sender || 'Unknown')}</span>
                                            ${message.is_unread ? '<span class="unread-badge">UNREAD</span>' : ''}
                                        </div>
                                        <div class="message-time">${new Date(message.timestamp || Date.now()).toLocaleString()}</div>
                                    </div>
                                    <div class="message-content">${escapeHtml(message.text || 'No content')}</div>
                                    ${message.chat_title ? `<div style="margin-top: 8px; font-size: 0.8rem; color: var(--text-secondary);">Chat: ${escapeHtml(message.chat_title)}</div>` : ''}
                                </div>
                            `;
                        });
                    }
                    
                    html += '</div>';
                    contentEl.innerHTML = html;
                }

                // Replace the updateStats function with this fixed version:
                function updateStats(stats) {
                    // Extract the actual stats data from the response object
                    const statsData = stats.data || stats;
                    
                    totalChatsEl.textContent = statsData.total_chats || 0;
                    processedChatsEl.textContent = statsData.chats_with_summaries || 0;
                    totalMessagesEl.textContent = statsData.total_messages || 0;
                    
                    // Update consolidated status with color coding
                    if (statsData.consolidated_available) {
                        consolidatedStatusEl.textContent = 'Yes';
                        consolidatedStatusEl.style.color = 'var(--secondary)';
                    } else {
                        consolidatedStatusEl.textContent = 'No';
                        consolidatedStatusEl.style.color = 'var(--warning)';
                    }
                }

                // Replace the updateProgress function with this fixed version:
                function updateProgress(progress) {
                    // Extract the actual progress data from the response object
                    const progressData = progress.data || progress;
                    
                    const percent = progressData.progress_percent || 0;
                    progressFillEl.style.width = `${percent}%`;
                    progressPercentEl.textContent = `${percent}%`;
                    
                    processedCountEl.textContent = progressData.processed_chats || 0;
                    totalCountEl.textContent = progressData.total_chats || 0;
                    
                    // Update ETA
                    if (progressData.eta_formatted) {
                        etaValueEl.textContent = progressData.eta_formatted;
                    } else {
                        etaValueEl.textContent = progressData.stage === 'complete' ? 'Complete!' : 'Calculating...';
                    }
                    
                    // Update status indicator
                    updateStatusIndicator(progressData.status, progressData.stage);
                    
                    // Update loading details
                    if (progressData.stage === 'collecting') {
                        loadingDetailsEl.textContent = 'Collecting messages from Telegram...';
                    } else if (progressData.stage === 'summarizing') {
                        loadingDetailsEl.textContent = `Generating AI summaries... (${percent}% complete)`;
                    } else if (progressData.stage === 'complete') {
                        loadingDetailsEl.textContent = 'All summaries generated successfully!';
                    }
                }
                function updateStatusIndicator(status, stage) {
                    let statusClass, statusIcon, statusText;
                    
                    if (stage === 'complete') {
                        statusClass = 'status-healthy';
                        statusIcon = 'fas fa-check-circle';
                        statusText = 'System Ready';
                    } else if (stage === 'summarizing') {
                        statusClass = 'status-processing';
                        statusIcon = 'fas fa-sync-alt pulse';
                        statusText = 'Processing';
                    } else {
                        statusClass = 'status-initializing';
                        statusIcon = 'fas fa-circle pulse';
                        statusText = 'Initializing';
                    }
                    
                    statusIndicatorEl.className = `status-indicator ${statusClass}`;
                    statusIndicatorEl.innerHTML = `<i class="${statusIcon}"></i><span>${statusText}</span>`;
                    statusTextEl.textContent = statusText;
                }

                function escapeHtml(unsafe) {
                    if (unsafe === null || unsafe === undefined) return '';
                    return String(unsafe)
                        .replace(/&/g, "&amp;")
                        .replace(/</g, "&lt;")
                        .replace(/>/g, "&gt;")
                        .replace(/"/g, "&quot;")
                        .replace(/'/g, "&#039;");
                }

                // Enhanced auto-refresh with intelligent timing
                function startIntelligentAutoRefresh() {
                    let refreshSpeed = 2000; // Start with 2 seconds
                    
                    setInterval(() => {
                        if (autoRefresh) {
                            refreshDataConcurrent();
                            
                            // Adaptive refresh rate based on system load
                            const now = performance.now();
                            if (now - lastRefreshTime < 100) {
                                // Slow down if refreshing too fast
                                refreshSpeed = Math.min(refreshSpeed * 1.5, 5000);
                            } else {
                                // Speed up if system can handle it
                                refreshSpeed = Math.max(refreshSpeed * 0.9, 1000);
                            }
                        }
                    }, refreshSpeed);
                }

                // Enhanced initialization with free-threaded detection
                async function initializeApp() {
                    try {
                        const healthResponse = await fetch('/api/health');
                        const healthData = await healthResponse.json();
                        
                        // Update performance badge based on threading mode
                        const badge = document.getElementById('performanceBadge');
                        const modeText = document.getElementById('threadingMode');
                        
                        if (healthData.free_threaded) {
                            badge.className = 'performance-badge free-threaded';
                            modeText.textContent = 'Python 3.14 Free-Threaded';
                        } else {
                            badge.className = 'performance-badge standard';
                            modeText.textContent = 'Python Standard Threading';
                        }
                        
                        // Start normal app initialization
                        refreshDataConcurrent();
                        startIntelligentAutoRefresh();
                        updateLastUpdateTime();
                        
                    } catch (error) {
                        console.error('Initialization error:', error);
                        // Fallback to standard initialization
                        refreshDataConcurrent();
                        startIntelligentAutoRefresh();
                        updateLastUpdateTime();
                    }
                }

                // Initialize
                document.addEventListener('DOMContentLoaded', initializeApp);
            </script>
        </body>
        </html>
        """    

    def log_message(self, format, *args):
        """Optimized logging - only log slow requests or errors"""
        if self.path.startswith('/api/') and 'health' not in self.path:
            message = format % args
            # Only log requests that took significant time or had errors
            if '500' in message or '404' in message:
                logger.info(message)
        else:
            # Log non-API requests normally
            logger.info(format % args)

def start_high_performance_server(storage: MemoryStorage, host: str = "0.0.0.0", port: int = 5000):
    """Start high-performance web server with free-threaded optimizations"""
    
    # Initialize advanced preprocessor
    preprocessor = BackgroundPreprocessor(storage)
    
    def handler(*args, **kwargs):
        HighPerformanceHTTPHandler(storage, preprocessor, *args, **kwargs)
    
    # Configure server for high performance
    server = HTTPServer((host, port), handler)
    
    # Enhanced server configuration
    import socketserver
    socketserver.TCPServer.request_queue_size = 256  # Increased for high concurrency
    socketserver.TCPServer.timeout = 30  # Reasonable timeout
    
    # Set socket options for better performance
    server.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    if hasattr(socket, 'SO_REUSEPORT'):
        server.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    
    def run_server():
        logger.info(f"🚀 Ultra-high-performance web server: http://{host}:{port}")
        logger.info(f"📊 Free-threaded mode: {IS_FREE_THREADED}")
        logger.info("🎯 Features: Adaptive caching, Background preprocessing, Real-time metrics")
        logger.info("⚡ Performance: Concurrent request handling, Intelligent caching, Process pooling")
        
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            logger.info("🛑 Server shutdown requested")
        finally:
            preprocessor.stop()
    
    # Start server in dedicated thread
    server_thread = Thread(
        target=run_server, 
        daemon=True, 
        name="WebServer_Main",
        args=()
    )
    server_thread.start()
    
    # Return both server and preprocessor for proper cleanup
    return server, preprocessor

# Backward compatibility
def start_web_server(storage: MemoryStorage, host: str = "0.0.0.0", port: int = 5000):
    """Backward compatibility wrapper"""
    return start_high_performance_server(storage, host, port)