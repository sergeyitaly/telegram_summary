"""
Compatibility module for Python 3.14
"""
import sys

# Handle imghdr removal in Python 3.14
try:
    import utils.imghdr as imghdr
except ImportError:
    # Import our dummy implementation
    from . import imghdr as imghdr
    
    # Monkey patch for any modules that might need it
    if 'telethon' in sys.modules:
        import telethon
        telethon.imghdr = imghdr