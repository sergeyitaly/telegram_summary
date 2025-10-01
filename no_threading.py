#!/usr/bin/env python3.14
import asyncio
import logging
import sys
import os
import json
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
import threading
import aiohttp
import ssl
from urllib.parse import urlparse, parse_qs

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Simple configuration
def get_config():
    """Load configuration from environment variables"""
    try:
        from dotenv import load_dotenv
        load_dotenv()
        logger.info("✅ Environment variables loaded")
    except ImportError:
        logger.warning("⚠️ python-dotenv not available, using system environment")
    
    return {
        'api_id': int(os.getenv('TELEGRAM_API_ID', '0')),
        'api_hash': os.getenv('TELEGRAM_API_HASH', ''),
        'phone': os.getenv('TELEGRAM_PHONE', '').strip(),
        'openai_key': os.getenv('OPENAI_API_KEY', ''),
        'openai_model': os.getenv('OPENAI_MODEL', 'gpt-4o-mini'),
        'max_messages': 100,
        'summary_interval': 2,  # Reduced for testing
        'session_file': 'telegram_session.session'
    }

config = get_config()

# Simple global storage
messages_data = {}
summaries_data = {}
consolidated_summary = None
consolidated_timestamp = None

class SimpleHTTPHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed_path = urlparse(self.path)
        path = parsed_path.path
        
        if path == '/api/summaries':
            # Parse query parameters
            query_params = parse_qs(parsed_path.query)
            summary_type = query_params.get('type', ['individual'])[0]
            
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            
            if summary_type == 'consolidated':
                # Return consolidated summary
                result = {
                    'type': 'consolidated',
                    'summary': consolidated_summary or 'No consolidated summary available yet. Individual summaries are being generated first.',
                    'timestamp': consolidated_timestamp or datetime.now().isoformat(),
                    'total_chats': len(messages_data),
                    'total_messages': sum(len(msgs) for msgs in messages_data.values())
                }
                self.wfile.write(json.dumps([result]).encode())
            else:
                # Return individual chat summaries
                result = []
                for chat_id in messages_data:
                    if messages_data[chat_id]:
                        # Safely get the latest summary
                        latest_summary = None
                        if chat_id in summaries_data and summaries_data[chat_id]:
                            latest_summary = summaries_data[chat_id][-1]
                        
                        # Safely get chat title
                        chat_title = "Unknown Chat"
                        if messages_data[chat_id]:
                            chat_title = messages_data[chat_id][-1].get('chat_title', 'Unknown Chat') or "Unknown Chat"
                        
                        result.append({
                            'type': 'individual',
                            'chat_id': chat_id,
                            'chat_title': chat_title,
                            'summary': latest_summary.get('summary', 'No summary generated yet') if latest_summary else 'Collecting messages...',
                            'last_updated': latest_summary.get('timestamp', datetime.now().isoformat()) if latest_summary else datetime.now().isoformat(),
                            'message_count': len(messages_data[chat_id])
                        })
                
                self.wfile.write(json.dumps(result).encode())
            
        elif path == '/':
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            
            html = """
            <!DOCTYPE html>
            <html>
            <head>
                <title>Telegram Summarizer</title>
                <meta charset="utf-8">
                <meta name="viewport" content="width=device-width, initial-scale=1">
                <style>
                    * { box-sizing: border-box; margin: 0; padding: 0; }
                    body { 
                        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; 
                        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                        min-height: 100vh;
                        padding: 20px;
                        color: #333;
                    }
                    .container {
                        max-width: 1000px;
                        margin: 0 auto;
                        background: white;
                        border-radius: 15px;
                        box-shadow: 0 20px 40px rgba(0,0,0,0.1);
                        overflow: hidden;
                    }
                    .header {
                        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                        color: white;
                        padding: 30px;
                        text-align: center;
                    }
                    .header h1 {
                        font-size: 2.5em;
                        margin-bottom: 10px;
                    }
                    .content {
                        padding: 30px;
                    }
                    .chat {
                        border: 1px solid #e1e5e9;
                        border-radius: 10px;
                        padding: 20px;
                        margin: 20px 0;
                        background: white;
                    }
                    .summary {
                        background: #f8f9fa;
                        padding: 15px;
                        border-radius: 8px;
                        margin: 15px 0;
                        white-space: pre-wrap;
                        line-height: 1.5;
                    }
                    .consolidated-summary {
                        background: #e7f3ff;
                        border: 2px solid #4dabf7;
                        border-radius: 10px;
                        padding: 25px;
                        margin: 20px 0;
                    }
                    .python-badge {
                        background: #3776ab;
                        color: white;
                        padding: 5px 10px;
                        border-radius: 20px;
                        font-size: 0.8em;
                        display: inline-block;
                        margin-left: 10px;
                    }
                    .status {
                        background: #e7f3ff;
                        padding: 15px;
                        border-radius: 8px;
                        margin: 20px 0;
                        text-align: center;
                    }
                    .controls {
                        display: flex;
                        gap: 15px;
                        margin: 20px 0;
                        flex-wrap: wrap;
                    }
                    .btn {
                        background: #667eea;
                        color: white;
                        border: none;
                        padding: 12px 24px;
                        border-radius: 8px;
                        cursor: pointer;
                        font-size: 16px;
                        transition: background 0.3s;
                    }
                    .btn:hover {
                        background: #5a6fd8;
                    }
                    .btn.active {
                        background: #4959c6;
                        box-shadow: 0 2px 8px rgba(0,0,0,0.2);
                    }
                    .stats {
                        background: #f8f9fa;
                        padding: 15px;
                        border-radius: 8px;
                        margin: 15px 0;
                        display: grid;
                        grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
                        gap: 15px;
                    }
                    .stat-item {
                        text-align: center;
                        padding: 10px;
                    }
                    .stat-number {
                        font-size: 2em;
                        font-weight: bold;
                        color: #667eea;
                    }
                    .stat-label {
                        font-size: 0.9em;
                        color: #666;
                    }
                    .debug {
                        background: #fff3cd;
                        padding: 10px;
                        border-radius: 5px;
                        margin: 10px 0;
                        font-family: monospace;
                        font-size: 12px;
                    }
                    .loading {
                        text-align: center;
                        padding: 20px;
                        color: #666;
                    }
                </style>
            </head>
            <body>
                <div class="container">
                    <div class="header">
                        <h1>🤖 Telegram Summarizer <span class="python-badge">Python 3.14</span></h1>
                        <p>AI-powered summaries of your Telegram chats</p>
                    </div>
                    <div class="content">
                        <div id="status" class="status">
                            <p>🟡 Connecting to Telegram and collecting messages...</p>
                        </div>
                        
                        <div class="controls">
                            <button class="btn active" onclick="switchView('individual')">📱 Individual Chat Summaries</button>
                            <button class="btn" onclick="switchView('consolidated')">🌐 Consolidated Overview</button>
                            <button class="btn" onclick="refreshSummaries()">🔄 Refresh Now</button>
                        </div>
                        
                        <div id="stats" class="stats"></div>
                        <div id="debug"></div>
                        <div id="chats">
                            <div class="loading">Loading chats and summaries...</div>
                        </div>
                    </div>
                </div>

                <script>
                    let currentView = 'individual';
                    
                    function switchView(viewType) {
                        currentView = viewType;
                        // Update button states
                        document.querySelectorAll('.btn').forEach(btn => {
                            btn.classList.remove('active');
                        });
                        event.target.classList.add('active');
                        // Reload summaries
                        loadSummaries();
                    }
                    
                    function refreshSummaries() {
                        loadSummaries();
                        document.getElementById('status').innerHTML = '<p>🔄 Refreshing summaries...</p>';
                    }
                    
                    async function loadSummaries() {
                        try {
                            const response = await fetch(`/api/summaries?type=${currentView}`);
                            if (!response.ok) {
                                throw new Error(`HTTP error! status: ${response.status}`);
                            }
                            const data = await response.json();
                            const container = document.getElementById('chats');
                            const status = document.getElementById('status');
                            const debug = document.getElementById('debug');
                            const stats = document.getElementById('stats');
                            
                            // Debug info
                            debug.innerHTML = `<div class="debug">View: ${currentView} | Items: ${data.length} | Status: ${response.status}</div>`;
                            
                            if (data.length === 0) {
                                container.innerHTML = '<div class="loading">No chats found yet. The system is collecting messages from your Telegram account...</div>';
                                status.innerHTML = '<p>🟡 Status: Collecting messages from Telegram</p>';
                                stats.innerHTML = '';
                            } else {
                                if (currentView === 'consolidated' && data[0].type === 'consolidated') {
                                    // Show consolidated summary
                                    const consolidated = data[0];
                                    container.innerHTML = `
                                        <div class="consolidated-summary">
                                            <h2>🌐 Consolidated Overview</h2>
                                            <div class="summary">${escapeHtml(consolidated.summary)}</div>
                                            <small>Updated: ${new Date(consolidated.timestamp).toLocaleString()}</small>
                                        </div>
                                    `;
                                    
                                    stats.innerHTML = `
                                        <div class="stat-item">
                                            <div class="stat-number">${consolidated.total_chats}</div>
                                            <div class="stat-label">Active Chats</div>
                                        </div>
                                        <div class="stat-item">
                                            <div class="stat-number">${consolidated.total_messages}</div>
                                            <div class="stat-label">Total Messages</div>
                                        </div>
                                        <div class="stat-item">
                                            <div class="stat-number">${new Date(consolidated.timestamp).toLocaleTimeString()}</div>
                                            <div class="stat-label">Last Updated</div>
                                        </div>
                                    `;
                                    
                                    status.innerHTML = '<p>🟢 Status: Active - Consolidated Overview</p>';
                                } else {
                                    // Show individual chat summaries
                                    container.innerHTML = data.map(chat => {
                                        const safeTitle = chat.chat_title || 'Unknown Chat';
                                        const safeSummary = chat.summary || 'No summary available yet';
                                        const safeDate = chat.last_updated || new Date().toISOString();
                                        const messageCount = chat.message_count || 0;
                                        
                                        return `
                                            <div class="chat">
                                                <h3>${escapeHtml(safeTitle)}</h3>
                                                <div class="summary">${escapeHtml(safeSummary)}</div>
                                                <small>Updated: ${new Date(safeDate).toLocaleString()} • ${messageCount} messages</small>
                                            </div>
                                        `;
                                    }).join('');
                                    
                                    const totalMessages = data.reduce((sum, chat) => sum + (chat.message_count || 0), 0);
                                    stats.innerHTML = `
                                        <div class="stat-item">
                                            <div class="stat-number">${data.length}</div>
                                            <div class="stat-label">Active Chats</div>
                                        </div>
                                        <div class="stat-item">
                                            <div class="stat-number">${totalMessages}</div>
                                            <div class="stat-label">Total Messages</div>
                                        </div>
                                        <div class="stat-item">
                                            <div class="stat-number">${new Date().toLocaleTimeString()}</div>
                                            <div class="stat-label">Last Refresh</div>
                                        </div>
                                    `;
                                    
                                    status.innerHTML = '<p>🟢 Status: Active - Monitoring ' + data.length + ' individual chats</p>';
                                }
                            }
                        } catch (error) {
                            console.error('Error loading summaries:', error);
                            document.getElementById('chats').innerHTML = '<div class="loading">Error loading summaries: ' + error.message + '</div>';
                            document.getElementById('debug').innerHTML = '<div class="debug">Error: ' + error.message + '</div>';
                        }
                    }
                    
                    function escapeHtml(unsafe) {
                        if (unsafe === null || unsafe === undefined) {
                            return '';
                        }
                        return String(unsafe)
                            .replace(/&/g, "&amp;")
                            .replace(/</g, "&lt;")
                            .replace(/>/g, "&gt;")
                            .replace(/"/g, "&quot;")
                            .replace(/'/g, "&#039;");
                    }
                    
                    // Load on start and refresh every 10 seconds
                    loadSummaries();
                    setInterval(loadSummaries, 10000);
                </script>
            </body>
            </html>
            """
            self.wfile.write(html.encode())
        else:
            self.send_response(404)
            self.end_headers()
    
    def log_message(self, format, *args):
        pass

def run_server():
    """Run the web server"""
    server = HTTPServer(('0.0.0.0', 5000), SimpleHTTPHandler)
    logger.info("🌐 Web interface available at: http://localhost:5000")
    server.serve_forever()

async def call_openai_api(messages_text, chat_title=None, is_consolidated=False):
    """Call OpenAI API directly using aiohttp"""
    if not config['openai_key'] or config['openai_key'].startswith('your_'):
        logger.warning("⚠️ OpenAI API key not configured properly")
        return "OpenAI API key not configured - please check your .env file"
    
    url = "https://api.openai.com/v1/chat/completions"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {config['openai_key']}"
    }
    
    if is_consolidated:
        prompt = f"""
Please provide a comprehensive consolidated overview of all my Telegram conversations.

Here are summaries from individual chats:
{messages_text}

Create a unified overview that:
1. Identifies common themes and topics across all conversations
2. Highlights the most important information and updates
3. Notes any urgent matters or action items that need attention
4. Provides an overall sentiment analysis of my communications
5. Groups related information by category (work, personal, projects, etc.)

Format the response in a clear, structured way that gives me a complete picture of my Telegram activity.
"""
    else:
        prompt = f"""
Please provide a concise summary of the recent conversation in "{chat_title}".

Recent messages:
{messages_text}

Focus on:
- Main topics and themes discussed
- Key information or announcements
- Important decisions or action items
- Overall tone and sentiment

Keep the summary clear and easy to read (2-3 paragraphs maximum).
"""
    
    data = {
        "model": config['openai_model'],
        "messages": [
            {"role": "system", "content": "You are a helpful assistant that creates clear, concise summaries of conversations."},
            {"role": "user", "content": prompt}
        ],
        "max_tokens": 800 if is_consolidated else 500,
        "temperature": 0.3
    }
    
    try:
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        
        async with aiohttp.ClientSession() as session:
            async with session.post(
                url, 
                headers=headers, 
                json=data,
                ssl=ssl_context,
                timeout=aiohttp.ClientTimeout(total=45)
            ) as response:
                if response.status == 200:
                    result = await response.json()
                    summary = result['choices'][0]['message']['content'].strip()
                    logger.info(f"✅ Successfully generated {'consolidated ' if is_consolidated else ''}summary")
                    return summary
                else:
                    error_text = await response.text()
                    logger.error(f"❌ OpenAI API error {response.status}: {error_text}")
                    return f"Unable to generate summary (API error: {response.status})"
                    
    except Exception as e:
        logger.error(f"❌ OpenAI API call failed: {e}")
        return f"Unable to generate summary: {str(e)}"

async def generate_consolidated_summary():
    """Generate a consolidated summary from all individual summaries"""
    global consolidated_summary, consolidated_timestamp
    
    if not summaries_data:
        logger.info("⏳ No individual summaries available for consolidation")
        return
    
    # Collect all recent summaries
    all_summaries = []
    for chat_id in summaries_data:
        if summaries_data[chat_id]:
            latest_summary = summaries_data[chat_id][-1]
            chat_title = "Unknown Chat"
            if chat_id in messages_data and messages_data[chat_id]:
                chat_title = messages_data[chat_id][-1].get('chat_title', 'Unknown Chat') or "Unknown Chat"
            
            all_summaries.append(f"--- {chat_title} ---\n{latest_summary['summary']}\n")
    
    if all_summaries:
        summary_text = "\n".join(all_summaries)
        logger.info(f"🤖 Generating consolidated summary from {len(all_summaries)} chats")
        
        consolidated_summary = await call_openai_api(summary_text, is_consolidated=True)
        consolidated_timestamp = datetime.now().isoformat()
        
        logger.info("✅ Consolidated summary generated")

# ... [Keep the telegram_collector, ai_summarizer, and main functions exactly the same as before]
# Just copy those functions from the previous version - they don't need changes

async def telegram_collector():
    """Telegram message collector"""
    try:
        from telethon import TelegramClient, events
    except ImportError:
        logger.error("❌ Telethon not installed. Please run: pip install telethon")
        return
    
    client = None
    try:
        # Validate configuration
        if not config['phone'] or config['phone'].startswith('your_'):
            logger.error("❌ Please set TELEGRAM_PHONE in your .env file")
            return
            
        if config['api_id'] == 0 or not config['api_hash'] or config['api_hash'].startswith('your_'):
            logger.error("❌ Please set TELEGRAM_API_ID and TELEGRAM_API_HASH in your .env file")
            return
        
        client = TelegramClient(config['session_file'], config['api_id'], config['api_hash'])
        
        logger.info("🔐 Connecting to Telegram...")
        await client.start(phone=config['phone'])
        logger.info("✅ Successfully connected to Telegram")
        
        # Get user info
        me = await client.get_me()
        logger.info(f"👤 Logged in as: {me.first_name} ({me.username})")
        
        # Collect messages from all dialogs
        logger.info("📥 Collecting messages from your chats...")
        chat_count = 0
        message_count = 0
        
        async for dialog in client.iter_dialogs():
            if dialog.is_group or dialog.is_channel or dialog.is_user:
                chat = dialog.entity
                chat_id = str(chat.id)
                chat_title = getattr(chat, 'title', getattr(chat, 'first_name', getattr(chat, 'username', 'Unknown Chat'))) or "Unknown Chat"
                
                chat_messages = []
                async for message in client.iter_messages(chat, limit=30):
                    if message.text and message.text.strip():
                        chat_messages.append({
                            'id': message.id,
                            'text': message.text,
                            'timestamp': message.date.isoformat() if message.date else datetime.now().isoformat(),
                            'sender_name': getattr(message.sender, 'first_name', getattr(message.sender, 'username', 'Unknown')) or "Unknown",
                            'chat_id': chat_id,
                            'chat_title': chat_title
                        })
                        message_count += 1
                
                if chat_messages:
                    messages_data[chat_id] = chat_messages
                    logger.info(f"💬 Collected {len(chat_messages)} messages from {chat_title}")
                    chat_count += 1
        
        logger.info(f"📊 Found {chat_count} chats with {message_count} total messages")
        
        # Set up real-time monitoring
        @client.on(events.NewMessage)
        async def handler(event):
            try:
                message = event.message
                chat = await event.get_chat()
                if message.text and message.text.strip():
                    chat_id = str(chat.id)
                    chat_title = getattr(chat, 'title', getattr(chat, 'first_name', getattr(chat, 'username', 'Unknown Chat'))) or "Unknown Chat"
                    
                    message_data = {
                        'id': message.id,
                        'text': message.text,
                        'timestamp': datetime.now().isoformat(),
                        'sender_name': getattr(message.sender, 'first_name', getattr(message.sender, 'username', 'Unknown')) or "Unknown",
                        'chat_id': chat_id,
                        'chat_title': chat_title
                    }
                    
                    if chat_id not in messages_data:
                        messages_data[chat_id] = []
                    messages_data[chat_id].append(message_data)
                    # Keep only recent messages
                    messages_data[chat_id] = messages_data[chat_id][-100:]
                    
                    logger.info(f"🆕 New message in {chat_title}")
            except Exception as e:
                logger.error(f"❌ Error processing message: {e}")
        
        logger.info("🔄 Telegram monitoring active")
        # Keep the connection alive
        while True:
            await asyncio.sleep(60)
            
    except Exception as e:
        logger.error(f"❌ Telegram error: {e}")
    finally:
        if client:
            await client.disconnect()

async def ai_summarizer():
    """AI summarization service for both individual and consolidated summaries"""
    logger.info("🤖 AI Summarizer started")
    
    # Wait a bit for messages to be collected first
    await asyncio.sleep(10)
    
    while True:
        try:
            # Generate individual chat summaries
            chat_count = len(messages_data)
            if chat_count == 0:
                logger.info("⏳ No chats with messages yet, waiting...")
                await asyncio.sleep(30)
                continue
            
            logger.info(f"📝 Checking {chat_count} chats for summarization...")
            summarized_count = 0
            
            for chat_id in list(messages_data.keys()):
                messages = messages_data.get(chat_id, [])
                if len(messages) >= 3:  # Need at least 3 messages
                    chat_title = messages[0].get('chat_title', 'Unknown Chat') or "Unknown Chat"
                    
                    # Check if we need a new summary (no summary or older than 10 minutes)
                    needs_summary = True
                    if chat_id in summaries_data and summaries_data[chat_id]:
                        latest_summary = summaries_data[chat_id][-1]
                        summary_time = datetime.fromisoformat(latest_summary['timestamp'])
                        if (datetime.now() - summary_time).total_seconds() < 600:  # 10 minutes
                            needs_summary = False
                    
                    if needs_summary:
                        # Prepare messages for AI
                        message_texts = "\n".join([
                            f"{msg.get('sender_name', 'Unknown')}: {msg.get('text', '')[:150]}"
                            for msg in messages[-15:]
                        ])
                        
                        if message_texts.strip():
                            logger.info(f"🤖 Generating individual summary for {chat_title} ({len(messages)} messages)")
                            summary = await call_openai_api(message_texts, chat_title, is_consolidated=False)
                            
                            if chat_id not in summaries_data:
                                summaries_data[chat_id] = []
                            summaries_data[chat_id].append({
                                'summary': summary,
                                'timestamp': datetime.now().isoformat()
                            })
                            # Keep only recent summaries
                            summaries_data[chat_id] = summaries_data[chat_id][-3:]
                            
                            logger.info(f"✅ Individual summary created for {chat_title}")
                            summarized_count += 1
                            
                            # Rate limiting between API calls
                            await asyncio.sleep(3)
            
            # Generate consolidated summary if we have individual summaries
            if len(summaries_data) >= 2:  # At least 2 chats with summaries
                await generate_consolidated_summary()
            
            if summarized_count > 0:
                logger.info(f"🎉 Generated {summarized_count} new individual summaries")
            else:
                logger.info("💤 No new individual summaries needed")
            
            # Wait before next summarization cycle
            await asyncio.sleep(config['summary_interval'] * 60)
            
        except Exception as e:
            logger.error(f"❌ Summarizer error: {e}")
            await asyncio.sleep(30)

async def main():
    print("=" * 60)
    print("🚀 TELEGRAM SUMMARIZER - Python 3.14")
    print("=" * 60)
    
    # Validate configuration
    if config['api_id'] == 0 or not config['api_hash'] or config['api_hash'].startswith('your_'):
        print("❌ Please set TELEGRAM_API_ID and TELEGRAM_API_HASH in .env file")
        print("   Get them from: https://my.telegram.org/apps")
        return
    
    if not config['phone'] or config['phone'].startswith('your_'):
        print("❌ Please set TELEGRAM_PHONE in .env file")
        return
    
    if not config['openai_key'] or config['openai_key'].startswith('your_'):
        print("❌ Please set OPENAI_API_KEY in .env file")
        print("   Get it from: https://platform.openai.com/api-keys")
        return
    
    print("✅ Configuration validated")
    print("🔐 You will be asked to enter a verification code")
    print("🌐 Web interface: http://localhost:5000")
    print("📊 Features: Individual & Consolidated summaries")
    print("⏱️  Summary interval: 2 minutes (for testing)")
    print("=" * 60)
    
    # Start web server
    server_thread = threading.Thread(target=run_server, daemon=True)
    server_thread.start()
    
    # Run both services
    await asyncio.gather(
        telegram_collector(),
        ai_summarizer()
    )

if __name__ == "__main__":
    # Create .env template if it doesn't exist
    if not os.path.exists('.env'):
        with open('.env', 'w') as f:
            f.write("TELEGRAM_API_ID=your_id_here\n")
            f.write("TELEGRAM_API_HASH=your_hash_here\n")
            f.write("TELEGRAM_PHONE=your_phone_number\n")
            f.write("OPENAI_API_KEY=your_openai_key_here\n")
            f.write("OPENAI_MODEL=gpt-4o-mini\n")
        print("📝 Created .env template - please edit with your real API credentials")
        sys.exit(1)
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Application stopped by user")
    except Exception as e:
        print(f"💥 Fatal error: {e}")