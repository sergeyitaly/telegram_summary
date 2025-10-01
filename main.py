# main.py
import sys
import os
import threading
import concurrent.futures
from typing import Dict, List

# Add the project root and utils to Python path
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)
sys.path.append(os.path.join(project_root, 'utils'))

# Apply compatibility patches BEFORE importing anything else
try:
    from utils.telethon_patch import patch_telethon
    patch_telethon()
    print("✅ Telethon patches applied successfully")
except ImportError as e:
    print(f"❌ Failed to import telethon_patch: {e}")
    # Create a minimal fallback
    import types
    imghdr = types.ModuleType('imghdr')
    imghdr.what = lambda file, h=None: None
    sys.modules['imghdr'] = imghdr
    print("✅ Applied fallback imghdr patch")
    
import asyncio
import logging
from contextlib import AsyncExitStack
import time
from datetime import datetime

from dotenv import load_dotenv
load_dotenv()

from config.settings import load_config
from core.telegram_client import TelegramClientManager
from core.ai_summarizer import AISummarizer
from storage.memory_store import MemoryStorage
from api.server import start_web_server

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class TelegramSummaryApp:
    def __init__(self):
        self.config = load_config()
        self.storage = MemoryStorage()
        self.telegram_client = TelegramClientManager(self.config.telegram)
        self.ai_summarizer = AISummarizer(self.config.openai)
        self.is_running = False
        self._exit_stack = AsyncExitStack()
        
        # Threading optimization
        self.thread_pool = concurrent.futures.ThreadPoolExecutor(
            max_workers=6,  # Reduced for stability
            thread_name_prefix="telegram_processor"
        )
        
        # Progress tracking
        self.stats = {
            'cycles_completed': 0,
            'total_summaries_generated': 0,
            'last_processing_time': 0,
            'total_chats_collected': 0,
            'total_messages_collected': 0,
            'last_successful_cycle': None
        }
        
        # Thread-safe locks
        self._stats_lock = threading.Lock()
        self._storage_lock = threading.Lock()
    
    async def start(self):
        logger.info("🚀 Starting Telegram Summary App")
        
        if not self._validate_config():
            return False
        
        try:
            if not await self.telegram_client.connect():
                logger.error("❌ Failed to connect to Telegram")
                return False
            
            await self._exit_stack.enter_async_context(self.ai_summarizer)
            
            # Start web server in background thread
            start_web_server(self.storage, self.config.web_host, self.config.web_port)
            
            self.is_running = True
            logger.info("✅ Application started successfully")
            
            # Start SINGLE CYCLE processing with threading
            asyncio.create_task(self._single_cycle_processing())
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to start application: {e}")
            return False
    
    async def stop(self):
        logger.info("🛑 Stopping app")
        self.is_running = False
        self.thread_pool.shutdown(wait=True)
        await self._exit_stack.aclose()
        await self.telegram_client.disconnect()
    
    def _validate_config(self) -> bool:
        """Validate configuration"""
        errors = []
        
        if self.config.telegram.api_id == 0:
            errors.append("TELEGRAM_API_ID not set or invalid")
        if not self.config.telegram.api_hash or self.config.telegram.api_hash.startswith('your_'):
            errors.append("TELEGRAM_API_HASH not set")
        if not self.config.telegram.phone or self.config.telegram.phone.startswith('your_'):
            errors.append("TELEGRAM_PHONE not set")
        if not self.config.openai.api_key or self.config.openai.api_key.startswith('your_'):
            errors.append("OPENAI_API_KEY not set")
        
        if errors:
            logger.error("❌ Configuration errors:")
            for error in errors:
                logger.error(f"   - {error}")
            logger.error("📝 Please check your .env file")
            return False
        
        logger.info("✅ Configuration validated")
        return True

    async def _single_cycle_processing(self):
        """Process ALL chats in ONE cycle using threading for maximum speed"""
        logger.info("🔄 Starting SINGLE CYCLE processing for ALL chats...")
        
        start_time = time.time()
        
        try:
            # Update progress to show we're collecting
            self.storage.update_processing_stats(
                status='collecting',
                progress_percent=10
            )
            
            # Step 1: Collect ALL messages from ALL chats
            logger.info("🔍 Collecting messages from ALL chats...")
            all_chats_data = await self._collect_all_messages_optimized()
            
            if not all_chats_data:
                logger.warning("⚠️ No messages found in any chats")
                return
            
            total_chats = len(all_chats_data)
            total_messages = sum(len(messages) for messages in all_chats_data.values())
            
            # Update stats thread-safely
            with self._stats_lock:
                self.stats['total_chats_collected'] = total_chats
                self.stats['total_messages_collected'] = total_messages
            
            collection_time = time.time() - start_time
            logger.info(f"📊 Collected {total_messages} messages from {total_chats} chats in {collection_time:.2f}s")
            
            # Update progress to show we're processing
            self.storage.update_processing_stats(
                status='processing',
                progress_percent=50
            )
            
            # Step 2: Generate ONE comprehensive consolidated summary from ALL chats
            logger.info(f"🌐 Generating SINGLE comprehensive summary from ALL {total_chats} chats...")
            
            # Store all messages first
            with self._storage_lock:
                for chat_id, messages in all_chats_data.items():
                    self.storage.store_messages(chat_id, messages)
            
            # Generate the comprehensive consolidated summary
            consolidated_summary = await self.ai_summarizer.generate_comprehensive_consolidated_summary(
                all_chats_data,
                include_hashtags=True,
                include_chat_references=True
            )
            
            if consolidated_summary:
                # Store the consolidated summary
                with self._storage_lock:
                    self.storage.store_consolidated_summary(consolidated_summary)
                
                processing_time = time.time() - start_time
                
                # Update stats
                with self._stats_lock:
                    self.stats['cycles_completed'] += 1
                    self.stats['last_processing_time'] = processing_time
                    self.stats['last_successful_cycle'] = datetime.now().isoformat()
                    self.stats['total_summaries_generated'] = 1  # One consolidated summary
                
                # Update progress to complete
                self.storage.update_processing_stats(
                    status='complete',
                    progress_percent=100
                )
                
                logger.info(f"✅ SINGLE CYCLE COMPLETED: Generated 1 comprehensive summary from {total_chats} chats in {processing_time:.2f}s")
                logger.info(f"📋 Summary length: {len(consolidated_summary)} characters")
                
                # Log the summary preview
                logger.info("=" * 80)
                logger.info("CONSOLIDATED SUMMARY PREVIEW:")
                preview_lines = consolidated_summary.split('\n')[:8]  # First 8 lines
                for line in preview_lines:
                    if line.strip():  # Only log non-empty lines
                        logger.info(f"  {line}")
                logger.info("=" * 80)
                
                # Show current status
                await self._log_current_status()
                
            else:
                logger.error("❌ Failed to generate consolidated summary")
                self.storage.update_processing_stats(
                    status='error',
                    progress_percent=0
                )
                
        except Exception as e:
            logger.error(f"❌ Error in single cycle processing: {e}")
            self.storage.update_processing_stats(
                status='error',
                progress_percent=0
            )
        
        # Wait much longer before next cycle since we processed everything
        logger.info("⏰ Next cycle in 1 hour...")
        await asyncio.sleep(3600)  # Wait 1 hour instead of short intervals

    async def _collect_all_messages_optimized(self) -> Dict[str, List]:
        """Collect messages from ALL dialogs using optimized approach"""
        all_chats_data = {}
        
        try:
            # Use the existing method from telegram_client to get dialogs with unread messages
            logger.info("📨 Getting dialogs with unread messages...")
            
            # Use the collect_unread_messages method which should exist
            chats_data = await self.telegram_client.collect_unread_messages(limit_per_chat=25)
            
            if chats_data:
                all_chats_data = chats_data
                logger.info(f"✅ Collected messages from {len(chats_data)} chats")
            else:
                logger.warning("⚠️ No unread messages found in any chats")
                
        except Exception as e:
            logger.error(f"❌ Error collecting messages: {e}")
            
        return all_chats_data

    async def _collect_all_messages_threaded_fallback(self) -> Dict[str, List]:
        """Fallback method using threading if main method fails"""
        all_chats_data = {}
        
        try:
            # Get client instance
            client = self.telegram_client.client
            if not client:
                logger.error("❌ Telegram client not available")
                return all_chats_data
            
            # Get dialogs directly using telethon
            dialogs = await client.get_dialogs()
            
            # Filter only dialogs with unread messages
            dialogs_with_unread = [dialog for dialog in dialogs if getattr(dialog, 'unread_count', 0) > 0]
            
            logger.info(f"📨 Processing {len(dialogs_with_unread)} dialogs with unread messages")
            
            # Process dialogs in smaller batches to avoid rate limits
            batch_size = 5
            for i in range(0, len(dialogs_with_unread), batch_size):
                batch = dialogs_with_unread[i:i + batch_size]
                logger.info(f"🔄 Processing batch {i//batch_size + 1}/{(len(dialogs_with_unread)-1)//batch_size + 1}")
                
                batch_tasks = []
                for dialog in batch:
                    task = self._process_single_dialog(dialog)
                    batch_tasks.append(task)
                
                # Process batch with limited concurrency
                batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
                
                for result in batch_results:
                    if isinstance(result, Exception):
                        logger.warning(f"⚠️ Error processing dialog: {result}")
                    elif result:
                        chat_id, messages, chat_title = result
                        all_chats_data[chat_id] = messages
                        logger.debug(f"✅ Collected {len(messages)} messages from '{chat_title}'")
                
                # Small delay between batches to avoid rate limits
                if i + batch_size < len(dialogs_with_unread):
                    await asyncio.sleep(2)
                    
        except Exception as e:
            logger.error(f"❌ Error in threaded collection: {e}")
            
        return all_chats_data

    async def _process_single_dialog(self, dialog):
        """Process a single dialog to get messages"""
        try:
            chat_id = str(dialog.entity.id)
            chat_title = getattr(dialog.entity, 'title', 'Unknown')
            unread_count = getattr(dialog, 'unread_count', 0)
            
            if unread_count == 0:
                return None
            
            # Get messages using the telegram client
            messages = await self.telegram_client.get_messages(
                dialog.entity, 
                limit=min(unread_count, 20)
            )
            
            if messages:
                # Convert to TelegramMessage format
                from models.message import TelegramMessage
                telegram_messages = []
                
                for msg in messages:
                    if hasattr(msg, 'text') and msg.text:
                        telegram_messages.append(
                            TelegramMessage(
                                id=msg.id,
                                text=msg.text,
                                sender_name=self._get_sender_name(msg),
                                chat_title=chat_title,
                                timestamp=getattr(msg, 'date', datetime.now()),
                                chat_id=chat_id
                            )
                        )
                    elif hasattr(msg, 'message') and msg.message:
                        telegram_messages.append(
                            TelegramMessage(
                                id=msg.id,
                                text=msg.message,
                                sender_name=self._get_sender_name(msg),
                                chat_title=chat_title,
                                timestamp=getattr(msg, 'date', datetime.now()),
                                chat_id=chat_id
                            )
                        )
                
                if telegram_messages:
                    return chat_id, telegram_messages, chat_title
            
            return None
            
        except Exception as e:
            logger.warning(f"⚠️ Failed to process dialog '{chat_title}': {e}")
            return None

    def _get_sender_name(self, message):
        """Extract sender name from message"""
        try:
            if hasattr(message, 'sender') and message.sender:
                if hasattr(message.sender, 'first_name'):
                    return message.sender.first_name or "Unknown"
                elif hasattr(message.sender, 'title'):
                    return message.sender.title or "Unknown"
                elif hasattr(message.sender, 'username'):
                    return f"@{message.sender.username}" or "Unknown"
        except:
            pass
        return "Unknown"

    async def _log_current_status(self):
        """Log current status for monitoring"""
        chat_info = self.storage.get_chat_info()
        progress_data = self.storage.get_processing_stats()
        
        logger.info("📊 Current Status:")
        logger.info(f"   • Total chats processed: {self.stats['total_chats_collected']}")
        logger.info(f"   • Total messages processed: {self.stats['total_messages_collected']}")
        logger.info(f"   • Last processing time: {self.stats['last_processing_time']:.2f}s")
        logger.info(f"   • Cycles completed: {self.stats['cycles_completed']}")
        logger.info(f"   • Status: {progress_data['status']}")
        logger.info(f"   • Consolidated summaries generated: {self.stats['total_summaries_generated']}")
        
        # Show consolidated summary info if available
        if hasattr(self.storage, 'consolidated_summaries') and self.storage.consolidated_summaries:
            latest_consolidated = self.storage.consolidated_summaries[-1]
            logger.info(f"   • Latest consolidated summary: {len(latest_consolidated.summary)} chars")
        
        logger.info(f"   • Last successful cycle: {self.stats['last_successful_cycle']}")

async def main():
    app = TelegramSummaryApp()
    
    try:
        success = await app.start()
        if not success:
            logger.error("❌ Application failed to start")
            return
        
        logger.info("🎯 Application is running! Open http://localhost:5000 to view the dashboard")
        logger.info("📈 The system will now process ALL chats in ONE cycle")
        
        # Keep the application running
        while app.is_running:
            await asyncio.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("👋 Received interrupt signal")
    except Exception as e:
        logger.error(f"💥 Fatal error: {e}")
    finally:
        await app.stop()

if __name__ == "__main__":
    # Create .env template if it doesn't exist
    if not os.path.exists('.env'):
        with open('.env', 'w') as f:
            f.write("# Get these from https://my.telegram.org/apps\n")
            f.write("TELEGRAM_API_ID=your_api_id_here\n")
            f.write("TELEGRAM_API_HASH=your_api_hash_here\n")
            f.write("TELEGRAM_PHONE=your_phone_number\n")
            f.write("\n# Get this from https://platform.openai.com/api-keys\n")
            f.write("OPENAI_API_KEY=your_openai_key_here\n")
            f.write("OPENAI_MODEL=gpt-4o-mini\n")
            f.write("OPENAI_BASE_URL=https://api.openai.com/v1\n")
            f.write("OPENAI_TIMEOUT=30\n")
            f.write("\n# Web server configuration\n")
            f.write("WEB_HOST=0.0.0.0\n")
            f.write("WEB_PORT=5000\n")
            f.write("SUMMARY_INTERVAL=3600\n")  # 1 hour default for single cycle
        print("📝 Created .env template - please edit with your real API credentials")
        sys.exit(1)
    
    asyncio.run(main())