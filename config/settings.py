import os
from dataclasses import dataclass

@dataclass
class TelegramConfig:
    # For Bot API (using python-telegram-bot)
    api_id: int  # Not used for bot, but keeping for compatibility
    api_hash: str  # This will be used as bot token
    phone: str  # Not used for bot
    session_file: str = "telegram_session.session"
    
@dataclass
class OpenAIConfig:
    api_key: str
    model: str = "gpt-4o-mini"
    base_url: str = "https://api.openai.com/v1"
    max_tokens: int = 800
    temperature: float = 0.3
    timeout: int = 45

@dataclass
class AppConfig:
    telegram: TelegramConfig
    openai: OpenAIConfig
    summary_interval: int = 2
    max_messages_per_chat: int = 100
    web_port: int = 5000
    web_host: str = "0.0.0.0"

def load_config() -> AppConfig:
    return AppConfig(
        telegram=TelegramConfig(
            api_id=int(os.getenv('TELEGRAM_API_ID', '0')),
            api_hash=os.getenv('TELEGRAM_BOT_TOKEN', os.getenv('TELEGRAM_API_HASH', '')),
            phone=os.getenv('TELEGRAM_PHONE', ''),
        ),
        openai=OpenAIConfig(
            api_key=os.getenv('OPENAI_API_KEY', ''),
            model=os.getenv('OPENAI_MODEL', 'gpt-4o-mini')
        )
    )