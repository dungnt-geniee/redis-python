import os
import struct
import time
from typing import Dict, Tuple, Optional

class RDBParser:
    """Simple Redis RDB file parser and writer"""
    
    # RDB file format constants
    RDB_VERSION = 6  # Redis 2.8+
    MAGIC_STRING = b"REDIS"
    
    # Op codes
    OPCODE_EOF = 0xFF       # End of file
    OPCODE_SELECTDB = 0xFE  # Select database
    OPCODE_EXPIRETIME = 0xFD  # Expire time in seconds
    OPCODE_EXPIRETIMEMS = 0xFC  # Expire time in milliseconds
    OPCODE_STRING = 0x00    # String encoding
    
    def __init__(self):
        """Initialize RDB parser/writer"""
        pass
        
    def save_rdb(self, dir_path: str, filename: str, data_store: Dict[str, Tuple[str, Optional[int]]]) -> bool:
        """Save data to RDB file"""
        try:
            # Create directory if it doesn't exist
            os.makedirs(dir_path, exist_ok=True)
            
            file_path = os.path.join(dir_path, filename)
            
            with open(file_path, 'wb') as f:
                # Write the RDB header
                # Magic string "REDIS"
                f.write(self.MAGIC_STRING)
                
                # Version number (e.g., "0006")
                f.write(f"{self.RDB_VERSION:04d}".encode())
                
                # Select DB 0
                f.write(bytes([self.OPCODE_SELECTDB, 0]))
                
                # Write key-value pairs
                for key, (value, expiry) in data_store.items():
                    # If key has expiry, write it
                    if expiry is not None:
                        # Convert to milliseconds timestamp
                        f.write(bytes([self.OPCODE_EXPIRETIMEMS]))
                        f.write(struct.pack('<Q', expiry))  # 8-byte unsigned long long, little-endian
                    
                    # Write the key-value as strings
                    f.write(bytes([self.OPCODE_STRING]))
                    
                    # Write key length and key
                    key_bytes = key.encode('utf-8')
                    self._write_length(f, len(key_bytes))
                    f.write(key_bytes)
                    
                    # Write value length and value
                    value_bytes = value.encode('utf-8')
                    self._write_length(f, len(value_bytes))
                    f.write(value_bytes)
                
                # End of file marker
                f.write(bytes([self.OPCODE_EOF]))
                
                # Write checksum - for simplicity, using a dummy value
                checksum = 0x77DE0394AC9D23EA
                f.write(struct.pack('<Q', checksum))
                
            print(f"Successfully saved {len(data_store)} keys to {file_path}")
            return True
            
        except Exception as e:
            print(f"Error saving RDB file: {e}")
            return False
            
    def load_rdb(self, dir_path: str, filename: str) -> Dict[str, Tuple[str, Optional[int]]]:
        """Load data from RDB file"""
        data_store = {}
        file_path = os.path.join(dir_path, filename)
        
        if not os.path.exists(file_path):
            print(f"RDB file not found: {file_path}")
            return data_store
            
        try:
            with open(file_path, 'rb') as f:
                # Read and verify header
                magic = f.read(5)
                if magic != self.MAGIC_STRING:
                    print(f"Invalid RDB file header: {magic}")
                    return data_store
                    
                # Read version
                version = f.read(4)
                print(f"RDB version: {version.decode()}")
                
                # Current database - default to 0
                db = 0
                
                # Current expiry - None for no expiry
                expiry = None
                
                # Read key-value pairs until EOF
                while True:
                    byte = f.read(1)
                    if not byte:
                        break
                    
                    opcode = byte[0]
                    
                    if opcode == self.OPCODE_EOF:
                        # End of file, read checksum
                        checksum = f.read(8)
                        break
                        
                    elif opcode == self.OPCODE_SELECTDB:
                        # Select database
                        db = int.from_bytes(f.read(1), byteorder='little')
                        print(f"Selected DB: {db}")
                        expiry = None  # Reset expiry for new database
                        
                    elif opcode == self.OPCODE_EXPIRETIMEMS:
                        # Expiry time in milliseconds
                        expiry = int.from_bytes(f.read(8), byteorder='little')
                        
                    elif opcode == self.OPCODE_EXPIRETIME:
                        # Expiry time in seconds - convert to milliseconds
                        expiry_sec = int.from_bytes(f.read(4), byteorder='little')
                        expiry = expiry_sec * 1000
                        
                    elif opcode == self.OPCODE_STRING:
                        # Read key
                        key_len = self._read_length(f)
                        key = f.read(key_len).decode('utf-8')
                        
                        # Read value 
                        value_len = self._read_length(f)
                        value = f.read(value_len).decode('utf-8')
                        
                        # Store key-value pair with expiry if set
                        # Check if expiry is in the future before storing
                        current_time_ms = int(time.time() * 1000)
                        if expiry is None or expiry > current_time_ms:
                            data_store[key] = (value, expiry)
                        
                        # Reset expiry for next key
                        expiry = None
                    else:
                        print(f"Unknown opcode: {opcode}")
                        # Skip this entry and try to continue
                        pass
                    
            print(f"Successfully loaded {len(data_store)} keys from {file_path}")
            return data_store
            
        except Exception as e:
            print(f"Error loading RDB file: {e}")
            return data_store
            
    def _write_length(self, file, length: int) -> None:
        """Write a length value in the special RDB format"""
        if length < 64:
            # 6-bit length
            file.write(bytes([length]))
        elif length < 16384:
            # 14-bit length
            file.write(bytes([(length >> 8) | 0x40, length & 0xFF]))
        else:
            # 32-bit length with special encoding
            file.write(bytes([0x80]))
            file.write(struct.pack('<I', length))  # 4-byte unsigned int, little-endian
            
    def _read_length(self, file) -> int:
        """Read a length value in the special RDB format"""
        byte = file.read(1)[0]
        
        # Check the two most significant bits
        if (byte >> 6) == 0:
            # 00xxxxxx: 6-bit length
            return byte & 0x3F
        elif (byte >> 6) == 1:
            # 01xxxxxx: 14-bit length
            second_byte = file.read(1)[0]
            return ((byte & 0x3F) << 8) | second_byte
        elif (byte >> 6) == 2:
            # 10xxxxxx: 32-bit length
            return struct.unpack('<I', file.read(4))[0]  # 4-byte unsigned int, little-endian
        else:
            # 11xxxxxx: special encoding
            # For simplicity, let's assume it's an error
            raise ValueError(f"Unsupported length encoding: {byte}") 