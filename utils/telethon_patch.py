"""
Patch for Telethon to work with Python 3.14
"""
import sys
import os

def patch_telethon():
    """Apply patches to make Telethon work with Python 3.14"""
    
    # Patch 1: Handle imghdr removal
    try:
        import utils.imghdr as imghdr
    except ImportError:
        # Create a minimal imghdr module
        import types
        imghdr = types.ModuleType('imghdr')
        
        def what(file, h=None):
            return None
            
        def test(file, h=None):
            return what(file, h)
            
        imghdr.what = what
        imghdr.test = test
        
        # Add to sys.modules so telethon can import it
        sys.modules['imghdr'] = imghdr
    
    # Patch 2: Handle other potential compatibility issues
    try:
        import telethon
        # Apply any additional telethon-specific patches here
    except ImportError:
        pass

# Apply patches when this module is imported
patch_telethon()