"""
Proper threading implementation for Python 3.14
"""
import asyncio
import threading
import concurrent.futures
import logging
import queue
import time
from typing import List, Callable, Any, Dict, Optional
from dataclasses import dataclass
import os

logger = logging.getLogger(__name__)

@dataclass
class ThreadTask:
    name: str
    function: Callable
    args: tuple
    kwargs: dict

class ThreadManager:
    def __init__(self, max_workers: int = None):
        # Use Python 3.14's ThreadPoolExecutor with optimal settings
        self.max_workers = max_workers or min(32, (os.cpu_count() or 1) + 4)
        self.thread_pool = concurrent.futures.ThreadPoolExecutor(
            max_workers=self.max_workers,
            thread_name_prefix="TGSummary"
        )
        self.thread_pool._threads = set()  # Track active threads
        self.task_queue = queue.Queue()
        self.results = {}
        self.lock = threading.Lock()
        
        logger.info(f"🧵 Python 3.14 ThreadPool initialized with {self.max_workers} workers")
    
    def run_sync_in_thread(self, task: ThreadTask) -> Any:
        """
        Run a synchronous function in a thread - PROPER threading approach
        This is for CPU-bound or blocking I/O operations
        """
        try:
            logger.debug(f"📋 Executing thread task: {task.name}")
            result = task.function(*task.args, **task.kwargs)
            logger.debug(f"✅ Thread task completed: {task.name}")
            return result
        except Exception as e:
            logger.error(f"❌ Thread task {task.name} failed: {e}")
            raise
    
    async def run_async_wrapper(self, task: ThreadTask) -> Any:
        """
        Proper async wrapper for thread execution
        Uses run_in_executor to avoid event loop conflicts
        """
        loop = asyncio.get_event_loop()
        
        try:
            # This is the CORRECT way to run sync functions in threads from async code
            result = await loop.run_in_executor(
                self.thread_pool,
                self.run_sync_in_thread,
                task
            )
            return result
        except Exception as e:
            logger.error(f"❌ Async thread wrapper failed for {task.name}: {e}")
            raise
    
    def run_batch_sync(self, tasks: List[ThreadTask]) -> Dict[str, Any]:
        """
        Run multiple sync tasks in parallel using proper threading
        Returns: {task_name: result}
        """
        results = {}
        
        # Use ThreadPoolExecutor to run tasks in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks
            future_to_task = {
                executor.submit(self.run_sync_in_thread, task): task
                for task in tasks
            }
            
            # Collect results as they complete
            for future in concurrent.futures.as_completed(future_to_task):
                task = future_to_task[future]
                try:
                    result = future.result()
                    results[task.name] = result
                    logger.debug(f"✅ Batch thread task completed: {task.name}")
                except Exception as e:
                    logger.error(f"❌ Batch thread task {task.name} failed: {e}")
                    results[task.name] = None
        
        return results
    
    def get_thread_info(self) -> Dict:
        """Get information about current threading state"""
        return {
            "max_workers": self.max_workers,
            "active_threads": threading.active_count(),
            "current_thread": threading.current_thread().name,
            "thread_ident": threading.get_ident()
        }
    
    def shutdown(self):
        """Proper shutdown of thread pool"""
        self.thread_pool.shutdown(wait=True, timeout=5)
        logger.info("🧵 Thread pool shutdown complete")

class AsyncProcessor:
    """
    Handles async operations that should NOT be threaded
    (like aiohttp requests that need the event loop)
    """
    def __init__(self):
        self.semaphore = asyncio.Semaphore(3)  # Limit concurrent async operations
    
    async def process_async_operations(self, operations: List[Callable]) -> List[Any]:
        """Process async operations with proper concurrency control"""
        results = []
        
        async with self.semaphore:
            tasks = [op() for op in operations]
            results = await asyncio.gather(*tasks, return_exceptions=True)
        
        return results

class ProcessingOrchestrator:
    """
    Orchestrates when to use threading vs async processing
    """
    def __init__(self):
        self.thread_manager = ThreadManager()
        self.async_processor = AsyncProcessor()
        logger.info("🎯 Processing orchestrator initialized")
    
    async def process_data_preparation(self, chats_data: Dict, prep_function: Callable) -> Dict:
        """
        Use threading for data preparation (CPU-bound work)
        """
        tasks = []
        
        for chat_id, messages in chats_data.items():
            if len(messages) >= 3:
                task = ThreadTask(
                    name=f"prep_{chat_id}",
                    function=prep_function,
                    args=(messages,),
                    kwargs={}
                )
                tasks.append(task)
        
        if not tasks:
            return {}
        
        logger.info(f"🧵 Preparing data for {len(tasks)} chats using threading")
        
        # Use threading for data preparation (sync work)
        prepared_data = self.thread_manager.run_batch_sync(tasks)
        
        logger.info(f"✅ Data preparation complete: {len(prepared_data)} chats prepared")
        return prepared_data
    
    async def process_async_operations(self, operations: List[Callable]) -> List[Any]:
        """
        Use async for I/O operations (like API calls)
        """
        return await self.async_processor.process_async_operations(operations)