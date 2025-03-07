from typing import List, Optional, Union, Tuple

class RESPProtocol:
    """Handle RESP (Redis Serialization Protocol) encoding and decoding"""
    
    @staticmethod
    def encode_simple_string(value: str) -> bytes:
        """Encode simple string: +OK\r\n"""
        return f"+{value}\r\n".encode()
    
    @staticmethod
    def encode_error(value: str) -> bytes:
        """Encode error: -Error message\r\n"""
        return f"-{value}\r\n".encode()
    
    @staticmethod
    def encode_integer(value: int) -> bytes:
        """Encode an integer value in RESP format"""
        return f":{value}\r\n".encode()
    
    @staticmethod
    def encode_bulk_string(value: Optional[str]) -> bytes:
        """Encode a bulk string in RESP format"""
        if value is None:
            return b"$-1\r\n"  # Null bulk string
        
        # First encode the value to bytes
        value_bytes = value.encode('utf-8')
        
        # Then construct the full response
        length_part = f"${len(value_bytes)}\r\n".encode('utf-8')
        
        # Concatenate parts with proper line endings
        return length_part + value_bytes + b"\r\n"
    
    @staticmethod
    def encode_array(items: List[str]) -> bytes:
        """Encode a list of strings as a RESP array"""
        encoded_items = [RESPProtocol.encode_bulk_string(item) for item in items]
        array_data = b"".join(encoded_items)
        return f"*{len(items)}\r\n".encode() + array_data
    
    @staticmethod
    def parse_command(message: str, remaining: Optional[list] = None) -> Tuple[Optional[str], List[str]]:
        """Parse RESP command from message and remaining buffer, return (command, args)"""
        lines = [message]
        if remaining and remaining[0]:
            lines.extend(remaining[0].split('\r\n'))
        
        if not lines[0].startswith('*'):
            # Simple command format
            parts = lines[0].strip().split()
            command = parts[0].upper() if parts else None
            args = parts[1:] if len(parts) > 1 else []
            return command, args
            
        try:
            array_length = int(lines[0][1:])
            expected_lines = 2 + (2 * array_length)
            
            if len(lines) < expected_lines:
                return None, []
                
            command = None
            args = []
            item_index = 1
            
            for i in range(array_length):
                if lines[item_index].startswith('$'):
                    item_index += 1
                    if command is None:
                        command = lines[item_index].upper()
                    else:
                        args.append(lines[item_index])
                    item_index += 1
                    
            return command, args
            
        except (ValueError, IndexError):
            return None, []
    
    @classmethod
    def decode_array(cls, data):
        """Decode a RESP array.
        Returns (parsed_array, original_data, remaining_data)
        If the array is incomplete, returns (None, None, original_data)
        """
        if not data.startswith(b'*'):
            # Not an array
            return None, None, data
        
        # Find the end of the length indicator
        idx = data.find(b'\r\n')
        if idx == -1:
            # Incomplete array length
            return None, None, data
        
        try:
            # Parse the array length
            length = int(data[1:idx])
        except ValueError:
            # Invalid length
            return None, None, data[idx+2:]
        
        # Start parsing array elements
        remaining = data[idx+2:]
        elements = []
        original_data = data
        
        for _ in range(length):
            if not remaining:
                # Incomplete array
                return None, None, original_data
            
            element_type = remaining[0:1]
            
            if element_type == b'$':  # Bulk string
                # Find string length
                len_end = remaining.find(b'\r\n')
                if len_end == -1:
                    return None, None, original_data
                
                try:
                    str_len = int(remaining[1:len_end])
                except ValueError:
                    # Skip invalid length
                    return None, None, original_data
                
                if str_len == -1:  # Null bulk string
                    elements.append(None)
                    remaining = remaining[len_end+2:]
                    continue
                
                # Check if we have enough data
                if len(remaining) < len_end + 2 + str_len + 2:
                    return None, None, original_data
                
                # Extract the string
                str_value = remaining[len_end+2:len_end+2+str_len]
                elements.append(str_value.decode('utf-8', errors='ignore'))
                remaining = remaining[len_end+2+str_len+2:]
            
            elif element_type == b':':  # Integer
                # Find integer end
                int_end = remaining.find(b'\r\n')
                if int_end == -1:
                    return None, None, original_data
                
                try:
                    int_value = int(remaining[1:int_end])
                    elements.append(int_value)
                except ValueError:
                    # Skip invalid integer
                    return None, None, original_data
                
                remaining = remaining[int_end+2:]
            
            elif element_type in (b'+', b'-'):  # Simple string or error
                # Find string end
                str_end = remaining.find(b'\r\n')
                if str_end == -1:
                    return None, None, original_data
                
                str_value = remaining[1:str_end].decode('utf-8', errors='ignore')
                elements.append(str_value)
                remaining = remaining[str_end+2:]
            
            else:
                # Unknown element type, skip this array
                return None, None, original_data
        
        # Successfully parsed the entire array
        return elements, original_data, remaining 