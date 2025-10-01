from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Any

@dataclass
class TelegramMessage:
    id: int
    text: str
    timestamp: datetime
    sender_name: str
    chat_id: str
    chat_title: str
    is_unread: bool = True 

    def to_dict(self) -> Dict[str, Any]:
        return {
            'id': self.id,
            'text': self.text,
            'timestamp': self.timestamp.isoformat(),
            'sender_name': self.sender_name,
            'chat_id': self.chat_id,
            'chat_title': self.chat_title
        }