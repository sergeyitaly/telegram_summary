# core/ai_summarizer.py
import aiohttp
import ssl
import json
import logging
from typing import List, Optional, Dict, Any
import asyncio
import time
from dataclasses import dataclass
import hashlib
import re
import concurrent.futures
from threading import Lock

from config.settings import OpenAIConfig
from models.message import TelegramMessage

logger = logging.getLogger(__name__)

@dataclass
class SummaryCache:
    content: str
    timestamp: float
    message_count: int

class AISummarizer:
    def __init__(self, config: OpenAIConfig):
        self.config = config
        self._session: Optional[aiohttp.ClientSession] = None
        
        # Enhanced caching
        self.summary_cache: Dict[str, SummaryCache] = {}
        self.cache_hits = 0
        self.cache_misses = 0
        self._cache_lock = Lock()
        
        # Consolidated summary settings
        self.max_chats_for_consolidated = 50
        self.max_messages_for_consolidated = 200
        
        logger.info("🚀 AI Summarizer initialized for consolidated summaries")

    async def __aenter__(self):
        await self._ensure_session()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
    
    async def _ensure_session(self):
        """Ensure HTTP session is ready"""
        if self._session is None:
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE
            
            timeout = aiohttp.ClientTimeout(total=60)
            connector = aiohttp.TCPConnector(
                ssl=ssl_context, 
                limit=20,
                limit_per_host=5
            )
            
            self._session = aiohttp.ClientSession(
                timeout=timeout,
                connector=connector
            )

    async def close(self):
        if self._session:
            await self._session.close()
        logger.info(f"📊 Cache stats: {self.cache_hits} hits, {self.cache_misses} misses")

    def _get_cached_summary(self, cache_key: str, max_age_minutes: int = 30) -> Optional[str]:
        """Get cached summary with freshness check"""
        with self._cache_lock:
            if cache_key in self.summary_cache:
                cache_entry = self.summary_cache[cache_key]
                if time.time() - cache_entry.timestamp < max_age_minutes * 60:
                    self.cache_hits += 1
                    logger.debug(f"🎯 Cache hit for {cache_key}")
                    return cache_entry.content
            self.cache_misses += 1
            return None

    def _store_cached_summary(self, cache_key: str, summary: str, message_count: int):
        """Store summary in cache"""
        with self._cache_lock:
            self.summary_cache[cache_key] = SummaryCache(
                content=summary,
                timestamp=time.time(),
                message_count=message_count
            )

    async def generate_comprehensive_consolidated_summary(
        self, 
        all_chats_data: Dict[str, List[TelegramMessage]],
        include_hashtags: bool = True,
        include_chat_references: bool = True
    ) -> Optional[str]:
        """
        Generate ONE comprehensive consolidated summary from ALL chats with hashtags and chat references
        """
        if not all_chats_data:
            logger.warning("⚠️ No chat data provided for consolidated summary")
            return None
        
        start_time = time.time()
        total_chats = len(all_chats_data)
        total_messages = sum(len(messages) for messages in all_chats_data.values())
        
        logger.info(f"🌐 Generating SINGLE comprehensive consolidated summary from {total_chats} chats with {total_messages} total messages")
        
        # Prepare consolidated data from all chats
        consolidated_data = self._prepare_consolidated_data(all_chats_data)
        if not consolidated_data:
            logger.warning("⚠️ No meaningful data for consolidated summary")
            return None
        
        # Create cache key based on all chat content
        cache_key = self._generate_consolidated_cache_key(all_chats_data)
        cached_summary = self._get_cached_summary(cache_key, max_age_minutes=30)
        
        if cached_summary:
            logger.info("✅ Using cached consolidated summary")
            return cached_summary
        
        # Build comprehensive prompt with hashtags and chat references
        prompt = self._build_comprehensive_consolidated_prompt(
            consolidated_data, 
            include_hashtags=include_hashtags,
            include_chat_references=include_chat_references
        )
        
        # Generate the comprehensive summary
        consolidated_summary = await self._call_openai_comprehensive(prompt, consolidated_data)
        
        if consolidated_summary:
            self._store_cached_summary(cache_key, consolidated_summary, total_messages)
            processing_time = time.time() - start_time
            logger.info(f"✅ SINGLE comprehensive consolidated summary generated in {processing_time:.2f}s: {len(consolidated_summary)} chars")
            
            # Add hashtags section if requested
            if include_hashtags:
                consolidated_summary = self._add_hashtags_section(consolidated_summary, consolidated_data)
        else:
            logger.error("❌ Failed to generate comprehensive consolidated summary")
        
        return consolidated_summary

    def _prepare_consolidated_data(self, all_chats_data: Dict[str, List[TelegramMessage]]) -> Dict[str, Any]:
        """Prepare data from all chats for consolidated analysis"""
        consolidated_data = {
            'chats': [],
            'total_messages': 0,
            'total_chats': len(all_chats_data),
            'sample_messages': [],
            'active_chats': [],
            'categories': {}  # Track chat categories for hashtags
        }
        
        message_count = 0
        sample_messages = []
        
        # Process each chat and select representative messages
        for chat_id, messages in all_chats_data.items():
            if not messages:
                continue
                
            chat_title = messages[0].chat_title if messages else "Unknown Chat"
            message_count += len(messages)
            
            # Categorize chats for hashtags
            category = self._categorize_chat(chat_title, messages)
            if category not in consolidated_data['categories']:
                consolidated_data['categories'][category] = []
            consolidated_data['categories'][category].append(chat_title)
            
            # Add chat info with message count for activity ranking
            chat_info = {
                'title': chat_title,
                'message_count': len(messages),
                'recent_messages': self._get_chat_highlights(messages),
                'chat_id': chat_id,
                'category': category
            }
            consolidated_data['chats'].append(chat_info)
            
            # Track active chats for highlighting
            if len(messages) >= 3:  # Consider chats with 3+ messages as active
                consolidated_data['active_chats'].append(chat_info)
            
            # Collect sample messages from this chat (2-3 per chat) with clear chat attribution
            sample_count = min(3, len(messages))
            for i in range(sample_count):
                if i < len(messages):
                    msg = messages[-(i+1)]  # Get recent messages
                    if msg.text and len(msg.text.strip()) > 10:
                        sample_messages.append({
                            'chat': chat_title,
                            'sender': msg.sender_name,
                            'text': msg.text.strip()[:120] + '...' if len(msg.text) > 120 else msg.text.strip(),
                            'timestamp': msg.timestamp.isoformat() if hasattr(msg, 'timestamp') else 'unknown',
                            'category': category
                        })
        
        # Sort active chats by message count (most active first)
        consolidated_data['active_chats'].sort(key=lambda x: x['message_count'], reverse=True)
        
        # Limit total sample messages to avoid overwhelming the prompt
        consolidated_data['sample_messages'] = sample_messages[:50]
        consolidated_data['total_messages'] = message_count
        
        logger.info(f"📊 Consolidated data: {len(consolidated_data['chats'])} chats, {message_count} messages, {len(consolidated_data['active_chats'])} active chats")
        
        return consolidated_data

    def _categorize_chat(self, chat_title: str, messages: List[TelegramMessage]) -> str:
        """Categorize chat for hashtag generation"""
        title_lower = chat_title.lower()
        
        # Category detection based on chat title and content
        if any(keyword in title_lower for keyword in ['it', 'tech', 'programming', 'coding', 'developer', 'code']):
            return 'IT'
        elif any(keyword in title_lower for keyword in ['cyber', 'security', 'hack', 'ddos', 'security']):
            return 'CyberSecurity'
        elif any(keyword in title_lower for keyword in ['english', 'study', 'learning', 'education']):
            return 'Education'
        elif any(keyword in title_lower for keyword in ['news', 'alert', 'update', 'радар', 'air raid']):
            return 'News'
        elif any(keyword in title_lower for keyword in ['community', 'сосед', 'residence', 'жил', 'управля']):
            return 'Community'
        elif any(keyword in title_lower for keyword in ['бот', 'bot', 'automation']):
            return 'Bots'
        elif any(keyword in title_lower for keyword in ['украин', 'ukraine', 'україн']):
            return 'Ukraine'
        else:
            return 'General'

    def _get_chat_highlights(self, messages: List[TelegramMessage]) -> List[str]:
        """Get key highlights from a chat"""
        highlights = []
        message_count = min(4, len(messages))
        
        for i in range(message_count):
            msg = messages[-(i+1)]  # Get recent messages
            if msg.text and len(msg.text.strip()) > 15:
                text = msg.text.strip()
                if len(text) > 70:
                    text = text[:70] + '...'
                highlights.append(f"{msg.sender_name}: {text}")
        
        return highlights

    def _build_comprehensive_consolidated_prompt(
        self, 
        consolidated_data: Dict[str, Any], 
        include_hashtags: bool = True,
        include_chat_references: bool = True
    ) -> str:
        """Build comprehensive prompt for single consolidated summary"""
        
        # Build active chats section (most important ones)
        active_chats_text = "\n".join([
            f"- {chat['title']} ({chat['message_count']} messages) - Category: {chat['category']}"
            for chat in consolidated_data['active_chats'][:12]  # Top 12 most active
        ])
        
        # Build sample messages section with clear chat attribution
        sample_messages_text = "\n".join([
            f"[{msg['category']}] {msg['chat']} - {msg['sender']}: {msg['text']}"
            for msg in consolidated_data['sample_messages']
        ])
        
        # Build categories overview
        categories_text = "\n".join([
            f"- {category}: {len(chats)} chats"
            for category, chats in consolidated_data['categories'].items()
        ])
        
        prompt = f"""Create a SINGLE comprehensive Telegram chat summary analyzing {consolidated_data['total_chats']} chats with {consolidated_data['total_messages']} total messages.

MAIN ACTIVE CHATS:
{active_chats_text}

CHAT CATEGORIES:
{categories_text}

RECENT MESSAGES SAMPLES:
{sample_messages_text}

CRITICAL REQUIREMENTS:

1. **SINGLE COMPREHENSIVE SUMMARY**: Provide ONE cohesive 4-5 paragraph analysis, NOT multiple separate summaries.

2. **CHAT NAME REFERENCES**: When mentioning discussions, ALWAYS reference the exact chat names where they're happening.

3. **CATEGORY ORGANIZATION**: Structure your analysis by categories (IT, CyberSecurity, Education, News, Community, etc.).

4. **ACTIONABLE INSIGHTS**: Focus on what's important right now - urgent matters, active discussions, and where attention is needed.

5. **TELEGRAM-FRIENDLY**: Write in a clear, engaging style suitable for Telegram.

FORMAT REQUIREMENTS:

- Start with an overall overview paragraph
- Organize by main categories/topics
- Mention specific chat names for each key discussion
- Include urgent/important matters
- End with key takeaways

Example structure:
"Across {consolidated_data['total_chats']} Telegram chats, several key themes are emerging today... 

In IT communities like [Chat Name] and [Chat Name], developers are discussing... 

The CyberSecurity groups including [Chat Name] are focused on... 

Important community updates in [Chat Name] require attention regarding... 

Overall, the most active conversations are happening in [Chat Names] where..."

IMPORTANT: This will be posted directly to Telegram, so make it engaging and actionable!"""

        if include_hashtags:
            prompt += "\n\n6. **HASHTAGS**: I'll add relevant hashtags at the end based on the categories and topics you identify."

        return prompt

    def _add_hashtags_section(self, summary: str, consolidated_data: Dict[str, Any]) -> str:
        """Add relevant hashtags to the summary"""
        # Extract categories for hashtags
        categories = list(consolidated_data['categories'].keys())
        
        # Generate hashtags based on categories and content
        hashtags = []
        
        # Category hashtags
        for category in categories:
            if category == 'IT':
                hashtags.extend(['#IT', '#Tech', '#Programming'])
            elif category == 'CyberSecurity':
                hashtags.extend(['#CyberSecurity', '#InfoSec', '#DDOS'])
            elif category == 'Education':
                hashtags.extend(['#Education', '#Learning', '#Study'])
            elif category == 'News':
                hashtags.extend(['#News', '#Alerts', '#Updates'])
            elif category == 'Community':
                hashtags.extend(['#Community', '#Neighbors', '#Residence'])
            elif category == 'Ukraine':
                hashtags.extend(['#Ukraine', '#Україна'])
        
        # Add general hashtags
        hashtags.extend(['#Telegram', '#ChatSummary', '#DailyUpdate'])
        
        # Remove duplicates and limit to 10 hashtags
        unique_hashtags = list(dict.fromkeys(hashtags))[:10]
        
        # Add hashtags section to summary
        if unique_hashtags:
            summary += f"\n\n{' '.join(unique_hashtags)}"
        
        return summary

    def _generate_consolidated_cache_key(self, all_chats_data: Dict[str, List[TelegramMessage]]) -> str:
        """Generate cache key for consolidated summary based on chat content"""
        content_parts = []
        
        for chat_id, messages in all_chats_data.items():
            if messages:
                chat_title = messages[0].chat_title if messages else "Unknown"
                recent_count = min(3, len(messages))
                content_parts.append(f"{chat_title}:{recent_count}")
        
        content_parts.sort()
        return hashlib.md5("|".join(content_parts).encode()).hexdigest()

    async def _call_openai_comprehensive(self, prompt: str, consolidated_data: Dict[str, Any]) -> Optional[str]:
        """Call OpenAI API for comprehensive summary"""
        await self._ensure_session()
        
        if not self.config.api_key or self.config.api_key.startswith('your_'):
            logger.error("❌ OpenAI API key not configured")
            return None

        url = f"{self.config.base_url}/chat/completions"
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.config.api_key}"
        }

        data = {
            "model": self.config.model,
            "messages": [
                {
                    "role": "system", 
                    "content": """You are an expert analyst creating comprehensive Telegram chat summaries. 
                    
Create ONE cohesive summary that:
- References specific chat names for each discussion
- Organizes content by categories/topics  
- Highlights urgent/important matters
- Is engaging and Telegram-friendly
- Provides actionable insights

Always use exact chat names like "In [Chat Name] they are discussing..." """
                },
                {
                    "role": "user", 
                    "content": prompt
                }
            ],
            "max_tokens": 1500,
            "temperature": 0.4,
            "top_p": 0.9,
        }

        try:
            logger.info("🔍 Making comprehensive OpenAI API request for SINGLE summary...")
            async with self._session.post(url, headers=headers, json=data, timeout=60) as response:
                if response.status == 200:
                    result = await response.json()
                    content = result['choices'][0]['message']['content'].strip()
                    if content and len(content) > 100:
                        chat_references = self._count_chat_references(content, consolidated_data)
                        logger.info(f"✅ Comprehensive SINGLE summary generated with {chat_references} chat references")
                        return content
                    else:
                        logger.warning("⚠️ OpenAI returned empty or too short comprehensive summary")
                        return None
                else:
                    error_text = await response.text()
                    logger.warning(f"⚠️ Comprehensive OpenAI API error {response.status}: {error_text[:200]}")
                    return None
                    
        except Exception as e:
            logger.warning(f"⚠️ Comprehensive OpenAI API call failed: {e}")
            return None

    def _count_chat_references(self, summary: str, consolidated_data: Dict[str, Any]) -> int:
        """Count how many chat names are referenced in the summary"""
        chat_names = [chat['title'] for chat in consolidated_data['chats']]
        reference_count = 0
        
        for chat_name in chat_names:
            if chat_name in summary:
                reference_count += 1
        
        return reference_count

    # Remove individual chat summarization methods to ensure only consolidated summaries are generated
    async def summarize_chats_optimized(self, chats_data: Dict[str, List[TelegramMessage]]) -> Dict[str, str]:
        """Override to only generate consolidated summary"""
        logger.info("🔄 Individual chat summarization disabled - generating SINGLE consolidated summary instead")
        
        consolidated_summary = await self.generate_comprehensive_consolidated_summary(chats_data)
        
        # Return empty dict for individual summaries, only provide consolidated
        return {}

    async def summarize_single_chat(self, messages: List[TelegramMessage], chat_title: str) -> Optional[str]:
        """Override single chat summarization"""
        logger.info("🔄 Single chat summarization disabled - use consolidated summary instead")
        return None