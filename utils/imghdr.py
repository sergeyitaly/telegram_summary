"""Compatibility module for imghdr which was removed in Python 3.14"""
try:
    # Try to import from standard library first (for older Python versions)
    from imghdr import what
except ImportError:
    # Fallback implementation for Python 3.14+
    import struct
    import os
    
    def what(file, h=None):
        """
        Determine the type of image contained in a file or byte string.
        """
        if h is None:
            if isinstance(file, (str, bytes, os.PathLike)):
                with open(file, 'rb') as f:
                    h = f.read(32)
            else:
                h = file.read(32)
                file.seek(0)
        
        if len(h) < 32:
            return None
            
        # Check for various image formats
        if h.startswith(b'\x89PNG\r\n\x1a\n'):
            return 'png'
        elif h.startswith(b'\xff\xd8\xff'):
            return 'jpeg'
        elif h.startswith(b'GIF8'):
            return 'gif'
        elif h.startswith(b'BM'):
            return 'bmp'
        elif h[0:4] == b'RIFF' and h[8:12] == b'WEBP':
            return 'webp'
        elif h.startswith(b'\x00\x00\x01\x00'):
            return 'ico'
        elif h.startswith(b'\x49\x49\x2a\x00') or h.startswith(b'\x4d\x4d\x00\x2a'):
            return 'tiff'
        # Add more formats as needed
        
        return None