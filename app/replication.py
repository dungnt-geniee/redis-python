from typing import Set, Optional, Tuple, List, Dict, Any
from asyncio import StreamWriter, StreamReader
import asyncio
from resp import RESPProtocol
import gc
import time
import uuid
import random
import hashlib

class ReplicationManager:
    """Handle Redis replication logic"""
    
    EMPTY_RDB_HEX = "524544495330303131FE00FF77DE0394AC9D23EA"
    
    def __init__(self):
        self.role: str = "master"
        self.replicas: Set[StreamWriter] = set()
        self.replica_ack_offsets: Dict[StreamWriter, int] = {}  # Track ACK offsets per replica
        self.master_host: Optional[str] = None
        self.master_port: Optional[int] = None
        self.replica_port: Optional[int] = None
        self.master_replid: str = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
        self.master_repl_offset: int = 0
        # Add a counter to track bytes processed from master
        self.processed_bytes: int = 0
        self.has_pending_writes: bool = False  # Track if we have any pending write operations
        
        # Heartbeat related attributes
        self.heartbeat_task = None  # Task for sending/checking heartbeats
        self.last_master_heartbeat = time.time()  # Last time we received a heartbeat from master
        
        # Election-related fields
        self.election_state = "follower"  # follower, candidate, or leader
        self.election_timeout_task = None
        self.votes_received = 0
        self.current_term = 0
        self.voted_for = None
        self.known_nodes = set()  # Set of node_ids we know about
        
        # Generation clock for conflict resolution
        self.generation = 0
        self.highest_seen_generation = 0
        self.log_entries = []  # To store committed commands
        
        # Election mutex to prevent multiple simultaneous elections
        self.election_mutex = asyncio.Lock()
    
    def get_empty_rdb(self) -> bytes:
        """Return empty RDB file contents"""
        return bytes.fromhex(self.EMPTY_RDB_HEX)
    
    async def start_heartbeat(self) -> None:
        """Start the heartbeat mechanism based on role"""
        # Cancel any existing heartbeat task
        if self.heartbeat_task and not self.heartbeat_task.done():
            self.heartbeat_task.cancel()
            
        # Find Redis instance to get config values
        from redis_server import Redis
        redis_instance = None
        for obj in gc.get_objects():
            if isinstance(obj, Redis) and obj.replication is self:
                redis_instance = obj
                break
                
        # If we couldn't find Redis instance or cluster is disabled, don't start heartbeat
        if not redis_instance or not redis_instance.config.get("cluster_enabled", True):
            return
            
        interval = redis_instance.config.get("heartbeat_interval", 5.0)
        timeout = redis_instance.config.get("heartbeat_timeout", 15.0)
            
        if self.role == "master":
            # Masters send heartbeats to replicas
            self.heartbeat_task = asyncio.create_task(self._send_master_heartbeats(interval))
        else:
            # Slaves monitor heartbeats from master
            self.heartbeat_task = asyncio.create_task(self._monitor_master_heartbeats(interval, timeout))
        
        print(f"Started heartbeat task for {self.role} role")
        
    async def _send_master_heartbeats(self, interval: float) -> None:
        """Send periodic heartbeats to all replicas"""
        try:
            while True:
                if self.replicas:
                    # Include node ID and generation in heartbeat
                    node_id = self.get_node_id()
                    heartbeat_cmd = RESPProtocol.encode_array([
                        "PING", 
                        f"_GEN_{self.generation}", 
                        f"node_id={node_id}"
                    ])
                    
                    print(f"Sending heartbeat to {len(self.replicas)} replicas (generation {self.generation})")
                    
                    for replica in list(self.replicas):
                        try:
                            if not replica.is_closing():
                                replica.write(heartbeat_cmd)
                                await replica.drain()
                        except Exception as e:
                            print(f"Error sending heartbeat to replica: {e}")
                            self.replicas.discard(replica)
                
                # Sleep until next heartbeat
                await asyncio.sleep(interval)
        except asyncio.CancelledError:
            print("Master heartbeat task cancelled")
        except Exception as e:
            print(f"Error in master heartbeat: {e}")
            
    async def _monitor_master_heartbeats(self, interval: float, timeout: float) -> None:
        """Monitor heartbeats from master and trigger election if timeout"""
        try:
            # Update the last heartbeat time when we start monitoring
            self.last_master_heartbeat = time.time()
            
            while True:
                # Check if we've exceeded the timeout
                time_since_last = time.time() - self.last_master_heartbeat
                if time_since_last > timeout and self.election_state == "follower":
                    print(f"Master heartbeat timeout! Last heartbeat was {time_since_last:.1f} seconds ago")
                    
                    # Before starting election, check if there's already a master we can connect to
                    discovered_nodes = await self.discover_replicas()
                    found_master = False
                    
                    for host, port in discovered_nodes:
                        try:
                            print(f"Checking if {host}:{port} is a master")
                            reader, writer = await asyncio.wait_for(
                                asyncio.open_connection(host, port),
                                timeout=1.0
                            )
                            
                            # Send INFO command to check role
                            writer.write(RESPProtocol.encode_array(["INFO", "replication"]))
                            await writer.drain()
                            
                            # Read response
                            response = await asyncio.wait_for(reader.read(1024), timeout=1.0)
                            
                            # Check if it's a master
                            if b"role:master" in response:
                                print(f"Found existing master at {host}:{port}")
                                self.master_host = host
                                self.master_port = port
                                
                                # Close the current connection 
                                writer.close()
                                await writer.wait_closed()
                                
                                # Connect as a replica
                                new_reader, new_writer = await self.connect_to_master()
                                if new_reader and new_writer:
                                    # Reset heartbeat time
                                    self.last_master_heartbeat = time.time()
                                    # Start master connection handler
                                    asyncio.create_task(
                                        self.handle_master_connection(new_reader, new_writer)
                                    )
                                    found_master = True
                                    break
                            
                            # Close connection if not a master
                            writer.close()
                            await writer.wait_closed()
                            
                        except Exception as e:
                            print(f"Error checking {host}:{port}: {e}")
                    
                    # Only start election if we couldn't find an existing master
                    if not found_master:
                        # Continue with election process
                        # ... existing election code ...
                        
                        # Add randomization to election timeouts to prevent simultaneous elections
                        # ... rest of existing code ...
                        await self.start_election()
                
                # Wait before checking again
                await asyncio.sleep(interval / 2)
        except asyncio.CancelledError:
            print("Slave heartbeat monitoring task cancelled")
        except Exception as e:
            print(f"Error in slave heartbeat monitoring: {e}")
            
    async def propagate_to_replicas(self, command: str, *args: str) -> None:
        """Propagate command to all connected replicas with generation number"""
        if not self.replicas:
            return
            
        try:
            # For commands like INCR, we need to make sure args are properly converted to strings
            cmd_args = [command]
            for arg in args:
                cmd_args.append(str(arg))
                
            # Add generation as special argument with prefix
            cmd_args.append(f"_GEN_{self.generation}")
            cmd_args.append(f"node_id={self.get_node_id()}")
            
            cmd_bytes = RESPProtocol.encode_array(cmd_args)
            
            print(f"Propagating to replicas: {command} {args} (generation {self.generation})")
            
            for replica in list(self.replicas):
                try:
                    if not replica.is_closing():
                        replica.write(cmd_bytes)
                        await replica.drain()
                        self.master_repl_offset += len(cmd_bytes)
                except Exception as e:
                    print(f"Error propagating to replica: {e}")
                    self.replicas.discard(replica)
        except Exception as e:
            print(f"Error in propagate_to_replicas: {e}")
    
    async def handle_master_connection(self, reader: StreamReader, writer: StreamWriter) -> None:
        """Handle ongoing communication with master"""
        try:
            print("Starting master connection handler")
            buffer = b""
            
            while True:
                try:
                    data = await reader.read(1024)
                    if not data:
                        print("Master connection closed")
                        break
                    
                    buffer += data
                    print(f"Received {len(data)} bytes from master, buffer size: {len(buffer)}")
                    
                    # Process complete commands in buffer
                    while b'\r\n' in buffer:
                        # Try to parse as RESP protocol
                        try:
                            # The original error is because RESPProtocol doesn't have parse_resp method
                            # Instead, we need to use the appropriate parsing method for the message type
                            
                            # First, check the first byte to determine the RESP data type
                            if not buffer:
                                break
                                
                            data_type = buffer[0:1]
                            
                            if data_type == b'*':  # Array
                                # Parse array format
                                command, result, remaining = RESPProtocol.decode_array(buffer)
                                if command is None:  # Incomplete command
                                    break
                                    
                                # Extract command name and args
                                if command and len(command) > 0:
                                    cmd_name = command[0].upper() if isinstance(command[0], str) else command[0].decode().upper()
                                    args = command[1:] if len(command) > 1 else []
                                    
                                    # Check for generation info
                                    generation = None
                                    master_node_id = None
                                    for arg in args:
                                        if isinstance(arg, str):
                                            if arg.startswith("_GEN_"):
                                                try:
                                                    generation = int(arg[5:])
                                                    # Update our highest seen generation
                                                    if generation > self.highest_seen_generation:
                                                        self.highest_seen_generation = generation
                                                    print(f"Command has generation {generation}")
                                                except ValueError:
                                                    pass
                                            elif arg.startswith("node_id="):
                                                master_node_id = arg[8:]
                                    
                                    print(f"Processing master command: {cmd_name} {args}")
                                    
                                    # Handle PING from master - this is a heartbeat
                                    if cmd_name == "PING":
                                        # Update last heartbeat time
                                        self.last_master_heartbeat = time.time()
                                        print(f"Received heartbeat from master (generation {generation if generation is not None else 'unknown'})")
                                        
                                        # If this was a master announcing itself with higher generation,
                                        # we need to properly recognize it as our master
                                        if generation is not None and master_node_id is not None:
                                            # Record the master's ID and generation
                                            print(f"Master identified as node {master_node_id} with generation {generation}")
                                            
                                            # If we're in election/candidate state, revert to follower
                                            if self.election_state == "candidate":
                                                print(f"Reverting from candidate to follower due to valid master heartbeat")
                                                self.election_state = "follower"
                                        
                                        # Respond with PONG
                                        writer.write(RESPProtocol.encode_simple_string("PONG"))
                                        await writer.drain()
                                    
                                    # Handle other commands as needed...
                                    else:
                                        # Execute regular commands from master
                                        # Increment replication offset for each command
                                        command_size = len(cmd_name)
                                        for arg in args:
                                            command_size += len(arg)
                                        self.master_repl_offset += command_size
                                        
                                        try:
                                            # Find Redis instance to execute command
                                            from redis_server import Redis
                                            redis_instance = None
                                            for obj in gc.get_objects():
                                                if isinstance(obj, Redis) and obj.replication is self:
                                                    redis_instance = obj
                                                    break
                                            
                                            if redis_instance:
                                                # Process commands that modify data
                                                print(f"Executing master command: {cmd_name} {args}")
                                                
                                                # Handle SET command
                                                if cmd_name.upper() == "SET" and len(args) >= 2:
                                                    key, value = args[0], args[1]
                                                    if isinstance(key, bytes):
                                                        key = key.decode('utf-8', errors='ignore')
                                                    if isinstance(value, bytes):
                                                        value = value.decode('utf-8', errors='ignore')
                                                        
                                                    # Parse expiry if present
                                                    expiry = None
                                                    i = 2
                                                    while i < len(args):
                                                        opt = args[i].decode('utf-8', errors='ignore').upper() if isinstance(args[i], bytes) else args[i].upper()
                                                        if opt == "PX" and i+1 < len(args):
                                                            try:
                                                                px_val = int(args[i+1].decode('utf-8', errors='ignore') if isinstance(args[i+1], bytes) else args[i+1])
                                                                expiry = redis_instance.get_current_time_ms() + px_val
                                                                i += 2
                                                            except (ValueError, IndexError):
                                                                i += 2
                                                        else:
                                                            i += 1
                                                            
                                                    # Set the value in our data store
                                                    redis_instance.data_store[key] = (value, expiry)
                                                    print(f"Applied SET {key}={value} from master")
                                                    
                                                    # Save to RDB after key change
                                                    await redis_instance._save_rdb()
                                                
                                                # Handle INCR command
                                                elif cmd_name.upper() == "INCR" and len(args) >= 1:
                                                    key = args[0].decode('utf-8', errors='ignore') if isinstance(args[0], bytes) else args[0]
                                                    
                                                    # Get current value
                                                    if key in redis_instance.data_store and not redis_instance.is_key_expired(key):
                                                        current_val, expiry = redis_instance.data_store[key]
                                                        try:
                                                            new_val = int(current_val) + 1
                                                            redis_instance.data_store[key] = (str(new_val), expiry)
                                                            print(f"Applied INCR {key}={new_val} from master")
                                                        except ValueError:
                                                            print(f"Error incrementing value: {current_val} is not a number")
                                                    else:
                                                        # Key doesn't exist or is expired, set to 1
                                                        redis_instance.data_store[key] = ("1", None)
                                                        print(f"Applied INCR {key}=1 from master (new key)")
                                                
                                                # Handle DEL command
                                                elif cmd_name.upper() == "DEL" and args:
                                                    for arg in args:
                                                        key = arg.decode('utf-8', errors='ignore') if isinstance(arg, bytes) else arg
                                                        if key in redis_instance.data_store:
                                                            del redis_instance.data_store[key]
                                                            print(f"Applied DEL {key} from master")
                                                
                                                # After applying any change from master, save to RDB
                                                await redis_instance._save_rdb()
                                            else:
                                                print(f"Warning: Cannot find Redis instance to execute master command: {cmd_name}")
                                                
                                        except Exception as e:
                                            print(f"Error executing master command {cmd_name}: {e}")
                                
                                # Update buffer
                                buffer = remaining
                                self.processed_bytes += len(data) - len(remaining)
                                
                            elif data_type in (b'+', b'-', b':', b'$'):  # Simple String, Error, Integer, Bulk String
                                # Handle other RESP types
                                line_end = buffer.find(b'\r\n')
                                if line_end == -1:
                                    break  # Incomplete command
                                    
                                # For error messages, just log and skip processing
                                if data_type == b'-':
                                    error_msg = buffer[:line_end].decode('utf-8', errors='ignore')
                                    print(f"Received error from master: {error_msg}")
                                    buffer = buffer[line_end + 2:]  # Remove the processed line
                                    continue  # Skip further processing of this message
                                    
                                elif data_type == b'$':  # Bulk string
                                    # Keep the bulk string handling we just added
                                    try:
                                        bulk_length = int(buffer[1:line_end])
                                        
                                        # Check if we have enough data to read the entire string
                                        if len(buffer) < line_end + 2 + bulk_length + 2:
                                            break  # Incomplete bulk string
                                            
                                        # Extract the string value
                                        bulk_value = buffer[line_end + 2:line_end + 2 + bulk_length]
                                        
                                        # Decode the bulk string and process it
                                        value = bulk_value.decode('utf-8', errors='ignore')
                                        print(f"Received bulk string from master: {value}")
                                        
                                        # Remove the bulk string from the buffer
                                        buffer = buffer[line_end + 2 + bulk_length + 2:]
                                    except ValueError:
                                        # Invalid bulk string length
                                        print(f"Invalid bulk string length in: {buffer[:line_end]!r}")
                                        buffer = buffer[line_end + 2:]
                                else:
                                    # Simple string or Integer
                                    command = buffer[:line_end].decode('utf-8', errors='ignore')
                                    buffer = buffer[line_end + 2:]  # Remove the processed line
                                    print(f"Received simple message from master: {command}")
                            else:
                                # Unknown data type, skip a byte to avoid getting stuck
                                print(f"Unknown data type in buffer: {data_type!r}")
                                buffer = buffer[1:]
                            
                        except Exception as e:
                            print(f"Error processing command from master: {e}")
                            # Skip to next line to avoid getting stuck on corrupt data
                            next_pos = buffer.find(b'\r\n')
                            if next_pos != -1:
                                buffer = buffer[next_pos + 2:]
                            else:
                                buffer = b""
                            
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    print(f"Error reading from master: {e}")
                    await asyncio.sleep(1)  # Avoid tight loop in case of persistent errors
                
        except asyncio.CancelledError:
            print("Master connection handler cancelled")
        except Exception as e:
            print(f"Error in master connection handler: {e}")
        finally:
            print("Master connection handler finished")
            
            # If we still think we're a slave after disconnection, try to reconnect
            if self.role == "slave":
                # Wait before reconnection attempt
                await asyncio.sleep(1)
                
                # Try to reconnect to the master or find a new master
                if hasattr(self, 'discover_replicas'):
                    # Use discovery to find potential masters
                    print("Master disconnected - looking for new master to connect to")
                    discovered_nodes = await self.discover_replicas()
                    
                    for host, port in discovered_nodes:
                        try:
                            print(f"Attempting to connect to potential master at {host}:{port}")
                            new_reader, new_writer = await asyncio.open_connection(host, port)
                            
                            # Send INFO command to check role
                            new_writer.write(RESPProtocol.encode_array(["INFO", "replication"]))
                            await new_writer.drain()
                            
                            # Read response
                            response = await new_reader.read(1024)
                            if b"role:master" in response:
                                print(f"Found new master at {host}:{port}")
                                self.master_host = host
                                self.master_port = port
                                
                                # Start new master connection handler
                                asyncio.create_task(
                                    self.handle_master_connection(new_reader, new_writer)
                                )
                                return
                            
                            # Close connection if not a master
                            new_writer.close()
                            await new_writer.wait_closed()
                            
                        except Exception as e:
                            print(f"Error connecting to potential master at {host}:{port}: {e}")
                
                # If we couldn't find a new master, try to reconnect to original master
                print(f"Attempting to reconnect to original master: {self.master_host}:{self.master_port}")
                try:
                    new_reader, new_writer = await self.connect_to_master()
                    if new_reader and new_writer:
                        asyncio.create_task(self.handle_master_connection(new_reader, new_writer))
                except Exception as e:
                    print(f"Error reconnecting to master: {e}")
    
    async def connect_to_master(self) -> Tuple[Optional[StreamReader], Optional[StreamWriter]]:
        """Connect to master server and perform initial handshake"""
        
        if not self.master_host or not self.master_port:
            print("Error: Master host and port not set")
            return None, None
        
        try:
            print(f"Connecting to master at {self.master_host}:{self.master_port}")
            
            reader, writer = await asyncio.open_connection(self.master_host, self.master_port)
            
            # Make sure the replica port is set (not None)
            if self.replica_port is None:
                # Default to the "local port" of the connection if we can detect it
                sockname = writer.get_extra_info('sockname')
                if sockname and len(sockname) > 1:
                    self.replica_port = sockname[1]
                else:
                    # If we can't get the local port, use a fixed port of 6380
                    self.replica_port = 6380
                
            print(f"Using replica port: {self.replica_port}")
            
            # Send PING to check connection
            ping_cmd = RESPProtocol.encode_array(["PING"])
            writer.write(ping_cmd)
            
            # Expect PONG response
            response = await reader.readuntil(b"\r\n")
            if not response.startswith(b"+PONG"):
                print(f"Error: Expected PONG, got {response.decode().strip()}")
                writer.close()
                return None, None
            
            # Handshake step 1: Send listening port
            replconf_port = RESPProtocol.encode_array(["REPLCONF", "listening-port", str(self.replica_port)])
            writer.write(replconf_port)
            
            # Expect OK response
            response = await reader.readuntil(b"\r\n")
            if not response.startswith(b"+OK"):
                print(f"Error: Expected OK for REPLCONF listening-port, got {response.decode().strip()}")
                writer.close()
                return None, None
            
            # Continue with rest of handshake...
            
            # Send REPLCONF capa psync2
            capa_cmd = RESPProtocol.encode_array(["REPLCONF", "capa", "psync2"])
            writer.write(capa_cmd)
            await writer.drain()
            
            # Read OK for capa
            response = await reader.read(1024)
            if not response or not response.startswith(b"+OK"):
                print(f"Unexpected REPLCONF capa response: {response!r}")
                writer.close()
                return None, None
            print(f"Received REPLCONF capa response: {response!r}")
            
            # Send PSYNC
            psync_cmd = RESPProtocol.encode_array(["PSYNC", "?", "-1"])
            writer.write(psync_cmd)
            await writer.drain()
            
            # Read FULLRESYNC response
            response = await reader.read(8192)  # Larger buffer to get more data
            
            # Check for FULLRESYNC
            if not response or b"+FULLRESYNC" not in response:
                print(f"Unexpected PSYNC response: {response!r}")
                writer.close()
                return None, None
            
            print(f"Received PSYNC response: {response!r}")
            
            # Parse RDB from the response
            try:
                # Find the RDB header
                rdb_marker_pos = response.find(b"$")
                if rdb_marker_pos == -1:
                    # Wait for separate RDB message if not included in FULLRESYNC response
                    rdb_response = await reader.read(1024)
                    if not rdb_response or not rdb_response.startswith(b"$"):
                        print(f"Unexpected RDB header: {rdb_response!r}")
                        writer.close()
                        return None, None
                    rdb_data = rdb_response
                else:
                    rdb_data = response[rdb_marker_pos:]
                
                # Find length marker end
                length_end = rdb_data.find(b"\r\n")
                if length_end == -1:
                    raise ValueError("No length terminator found")
                
                # Parse RDB length
                rdb_length = int(rdb_data[1:length_end])
                print(f"Expected RDB length: {rdb_length}")
                
                # Extract RDB content so far
                rdb_content_start = length_end + 2  # Skip \r\n after length
                
                # Calculate where RDB content should end
                rdb_content_end = rdb_content_start + rdb_length
                
                # Initialize a variable to collect the complete RDB data
                complete_rdb_data = bytearray()
                
                # Check if REPLCONF command starts immediately after RDB
                remaining_data = b""
                if len(rdb_data) > rdb_content_end and rdb_data[rdb_content_end:].startswith(b"*"):
                    remaining_data = rdb_data[rdb_content_end:]
                    complete_rdb_data.extend(rdb_data[rdb_content_start:rdb_content_end])
                else:
                    # We have partial RDB data
                    complete_rdb_data.extend(rdb_data[rdb_content_start:])
                    
                    # Continue reading RDB content if needed
                    remaining_bytes = rdb_length - len(rdb_data[rdb_content_start:])
                    while remaining_bytes > 0:
                        chunk = await reader.read(min(4096, remaining_bytes))
                        if not chunk:
                            raise ValueError("Connection closed while reading RDB")
                            
                        if len(chunk) > remaining_bytes:
                            # We got more than needed, which might include commands
                            complete_rdb_data.extend(chunk[:remaining_bytes])
                            remaining_data = chunk[remaining_bytes:]
                            remaining_bytes = 0
                        else:
                            complete_rdb_data.extend(chunk)
                            remaining_bytes -= len(chunk)
                
                print(f"Received complete RDB file of length {len(complete_rdb_data)}")
                
                # Now that we have the complete RDB data, we need to load it into our Redis instance
                from redis_server import Redis
                for obj in gc.get_objects():
                    if isinstance(obj, Redis) and obj.replication is self:
                        redis_instance = obj
                        break
                
                if redis_instance:
                    # Create a temporary file to store the RDB data
                    import tempfile
                    import os
                    
                    # Create a temporary directory
                    with tempfile.TemporaryDirectory() as temp_dir:
                        temp_rdb_path = os.path.join(temp_dir, "temp.rdb")
                        
                        # Write the RDB data to the file
                        with open(temp_rdb_path, 'wb') as f:
                            f.write(complete_rdb_data)
                        
                        # Use the RDBParser to load the data
                        from rdb import RDBParser
                        parser = RDBParser()
                        data = parser.load_rdb(temp_dir, "temp.rdb")
                        
                        # Update the Redis instance's data store with the loaded data
                        redis_instance.data_store.update(data)
                        print(f"Loaded {len(data)} keys from master's RDB snapshot")
                        
                        # Save the loaded data to our own RDB file
                        await redis_instance._save_rdb()
                        print("Saved master data to local RDB file")
                
                # Check for and handle REPLCONF GETACK in the remaining data
                if remaining_data and remaining_data.startswith(b"*"):
                    print(f"Found command after RDB: {remaining_data}")
                    
                    # Process the initial command(s) in the remaining data
                    cmd_end = self.find_command_end(remaining_data)
                    if cmd_end > 0:
                        initial_cmd = remaining_data[:cmd_end]
                        
                        # Check if it's a REPLCONF GETACK command
                        if b"REPLCONF" in initial_cmd and b"GETACK" in initial_cmd:
                            print("Handling initial REPLCONF GETACK command")
                            
                            # For the very first GETACK after RDB, respond with 0
                            ack_response = RESPProtocol.encode_array(["REPLCONF", "ACK", "0"])
                            writer.write(ack_response)
                            await writer.drain()
                            
                            # Add the command bytes to the processed_bytes counter
                            self.processed_bytes = len(initial_cmd)
                            print(f"Initialized processed bytes to {self.processed_bytes}")
                            
                            # Remove the processed command from remaining data
                            remaining_data = remaining_data[cmd_end:]
                
                # Process remaining data from RDB response
                if remaining_data:
                    print(f"Found command data after RDB: {remaining_data}")
                    
                    # Process all complete commands in the remaining data
                    while remaining_data.startswith(b"*"):
                        cmd_end = self.find_command_end(remaining_data)
                        if cmd_end == 0:
                            break  # Incomplete command
                        
                        command_data = remaining_data[:cmd_end]
                        
                        # Parse the command
                        try:
                            command, args = self.parse_command_bytes(command_data)
                            print(f"Parsed command after RDB: {command} {args}")
                            
                            # Handle the command
                            if command == "REPLCONF" and len(args) >= 2 and args[0].upper() == "GETACK":
                                # For the first GETACK, respond with 0
                                print("Handling REPLCONF GETACK command after RDB")
                                ack_response = RESPProtocol.encode_array(["REPLCONF", "ACK", "0"])
                                writer.write(ack_response)
                                await writer.drain()
                            
                            # Track processed bytes for all commands
                            self.processed_bytes += len(command_data)
                            print(f"Updated processed bytes to {self.processed_bytes} after {command}")
                            
                        except Exception as e:
                            print(f"Error parsing command: {e}")
                        
                        # Move to next command
                        remaining_data = remaining_data[cmd_end:]
                
                # Try to read more commands if needed
                try:
                    cmd_data = await asyncio.wait_for(reader.read(1024), 0.2)
                    if cmd_data:
                        print(f"Received additional command data: {cmd_data}")
                        
                        # Process the data the same way as the remaining data
                        pos = 0
                        while pos < len(cmd_data):
                            if not cmd_data[pos:].startswith(b"*"):
                                break
                            
                            # Find end of this command
                            cmd_end = self.find_command_end(cmd_data[pos:])
                            if cmd_end == 0:
                                break  # Incomplete command
                            
                            command_bytes = cmd_data[pos:pos+cmd_end]
                            
                            # Parse and handle command
                            try:
                                command, args = self.parse_command_bytes(command_bytes)
                                print(f"Parsed additional command: {command} {args}")
                                
                                if command == "PING":
                                    print("Received PING from master")
                                    self.processed_bytes += len(command_bytes)
                                    print(f"Updated processed bytes to {self.processed_bytes} after PING")
                                elif command == "REPLCONF" and len(args) >= 2 and args[0].upper() == "GETACK":
                                    print(f"Received REPLCONF GETACK, responding with offset {self.processed_bytes}")
                                    ack_response = RESPProtocol.encode_array(["REPLCONF", "ACK", str(self.processed_bytes)])
                                    writer.write(ack_response)
                                    await writer.drain()
                                    
                                    # After responding, add this command's bytes to the processed bytes
                                    self.processed_bytes += len(command_bytes)
                                    print(f"Updated processed bytes to {self.processed_bytes} after GETACK")
                                else:
                                    # Process any other command
                                    self.processed_bytes += len(command_bytes)
                                    print(f"Updated processed bytes to {self.processed_bytes} after {command}")
                                
                            except Exception as e:
                                print(f"Error parsing additional command: {e}")
                            
                            # Move to next command
                            pos += cmd_end
                except asyncio.TimeoutError:
                    print("No additional commands received")
                
                # After RDB synchronization, explicitly request all keys from master
                # to make sure we have everything
                print("Requesting all keys from master for verification...")
                keys_cmd = RESPProtocol.encode_array(["KEYS", "*"])
                writer.write(keys_cmd)
                await writer.drain()
                
                # Wait a bit to make sure KEYS command is processed and any 
                # outstanding data is received
                await asyncio.sleep(0.5)
                
                # Process any data that might have been sent in response
                try:
                    extra_data = await asyncio.wait_for(reader.read(4096), 1.0)
                    if extra_data:
                        print(f"Received extra data after synchronization: {len(extra_data)} bytes")
                        
                        # Find Redis instance again to ensure we have the right one
                        from redis_server import Redis
                        redis_instance = None
                        for obj in gc.get_objects():
                            if isinstance(obj, Redis) and obj.replication is self:
                                redis_instance = obj
                                break
                        
                        if redis_instance:
                            # Log current keys in our store
                            current_keys = list(redis_instance.data_store.keys())
                            print(f"Current keys in slave: {current_keys}")
                            
                            # We also want to run a verification - get all keys from master directly
                            # through a separate connection to compare
                            print("Connecting to master directly to verify keys...")
                            try:
                                verify_reader, verify_writer = await asyncio.open_connection(
                                    self.master_host, self.master_port)
                                
                                # Send KEYS * command
                                verify_writer.write(RESPProtocol.encode_array(["KEYS", "*"]))
                                await verify_writer.drain()
                                
                                # Read the response - this will be an array of keys
                                response_data = await verify_reader.read(4096)
                                print(f"Verification data received: {len(response_data)} bytes")
                                
                                # Parse array response to extract keys
                                master_keys = []
                                
                                # Simple parsing of RESP array
                                if response_data.startswith(b"*"):
                                    try:
                                        lines = response_data.split(b"\r\n")
                                        array_size = int(lines[0][1:])
                                        print(f"Master has {array_size} keys")
                                        
                                        # Extract each key
                                        index = 1
                                        for i in range(array_size):
                                            if index + 1 < len(lines) and lines[index].startswith(b"$"):
                                                key = lines[index+1].decode("utf-8")
                                                master_keys.append(key)
                                                index += 2
                                    except Exception as e:
                                        print(f"Error parsing verification response: {e}")
                                
                                print(f"Master keys: {master_keys}")
                                
                                # For any keys in master that aren't in slave, get their values
                                missing_keys = [k for k in master_keys if k not in current_keys]
                                print(f"Keys missing in slave: {missing_keys}")
                                
                                # Fetch and set each missing key
                                for key in missing_keys:
                                    verify_writer.write(RESPProtocol.encode_array(["GET", key]))
                                    await verify_writer.drain()
                                    
                                    # Simple read for the GET response
                                    get_response = await verify_reader.read(1024)
                                    
                                    if get_response.startswith(b"$"):
                                        # It's a bulk string response
                                        lines = get_response.split(b"\r\n")
                                        if len(lines) >= 2 and lines[0].startswith(b"$"):
                                            value_len = int(lines[0][1:])
                                            if value_len > 0:
                                                value = lines[1].decode("utf-8")
                                                # Store it in our data store
                                                redis_instance.data_store[key] = (value, None)
                                                print(f"Added missing key: {key} = {value}")
                                
                                # After adding all missing keys, save to RDB
                                if missing_keys:
                                    await redis_instance._save_rdb()
                                    print("Saved missing keys to local RDB file")
                                
                                # Clean up verification connection
                                verify_writer.close()
                                await verify_writer.wait_closed()
                                
                            except Exception as e:
                                print(f"Error in key verification: {e}")
                except asyncio.TimeoutError:
                    print("No extra data received after synchronization.")
                
                print("Master-slave synchronization complete!")
                return reader, writer
                
            except (ValueError, IndexError) as e:
                print(f"Error processing RDB: {e}")
                writer.close()
                return None, None
            
        except Exception as e:
            print(f"Failed to connect to master: {e}")
            return None, None

    async def handle_replconf(self, args: list, writer: StreamWriter) -> None:
        """Handle REPLCONF command from replicas or master"""
        if not args:
            writer.write(RESPProtocol.encode_error("wrong number of arguments for 'replconf' command"))
            return
        
        subcommand = args[0].upper() if args[0] else ""
        
        if subcommand == "LISTENING-PORT" and len(args) >= 2:
            # Replica is informing us of its listening port
            try:
                port = int(args[1])
                self.replica_port = port
                # Add writer to replicas set
                self.replicas.add(writer)
                writer.write(RESPProtocol.encode_simple_string("OK"))
            except ValueError:
                writer.write(RESPProtocol.encode_error("Invalid port number"))
        
        elif subcommand == "CAPA" and len(args) >= 2:
            # Replica is informing us of its capabilities
            writer.write(RESPProtocol.encode_simple_string("OK"))
        
        elif subcommand == "GETACK" and len(args) >= 2:
            # Master is requesting acknowledgment of processed commands
            # We need to respond with the current processed bytes
            ack_response = RESPProtocol.encode_array(["REPLCONF", "ACK", str(self.processed_bytes)])
            writer.write(ack_response)
        
        elif subcommand == "ACK" and len(args) >= 2:
            # Replica is acknowledging processed commands
            try:
                offset = int(args[1])
                print(f"Received ACK for offset: {offset}")
                # Update the tracking of acknowledged offsets
                self.replica_ack_offsets[writer] = offset
            except ValueError:
                print(f"Invalid offset in ACK: {args[1]}")
        
        else:
            # Default response for other REPLCONF commands
            writer.write(RESPProtocol.encode_simple_string("OK"))
        
        # Check for generation info in REPLCONF
        generation = None
        master_node_id = None
        
        for arg in args:
            if isinstance(arg, str):
                if arg.startswith("_GEN_"):
                    try:
                        generation = int(arg[5:])
                        print(f"Detected heartbeat with generation {generation}")
                    except ValueError:
                        pass
                elif arg.startswith("node_id="):
                    master_node_id = arg[8:]
        
        if generation is not None and master_node_id is not None:
            # If we receive a heartbeat with higher generation, acknowledge sender as master
            if self.role == "master" and generation > self.generation:
                print(f"Stepping down as master in favor of node {master_node_id} with higher generation {generation}")
                self.role = "slave"
                self.election_state = "follower"
                
                # Update our generation tracking
                self.highest_seen_generation = generation
                
                # Connect to the new master
                # Find its host and port (we need to figure this out from the node_id)
                # This is challenging because we need to map node_id to host:port
                # For now, we can rely on the discover_replicas method to find it
                asyncio.create_task(self._connect_to_new_master(master_node_id))

    async def handle_psync(self, args: list, writer: StreamWriter) -> None:
        """Handle PSYNC command from replica"""
        print(f"Received PSYNC with args: {args}")
        response = f"+FULLRESYNC {self.master_replid} {self.master_repl_offset}\r\n"
        writer.write(response.encode())
        await writer.drain()
        
        # Send empty RDB file
        rdb_contents = self.get_empty_rdb()
        rdb_length = len(rdb_contents)
        writer.write(f"${rdb_length}\r\n".encode())
        writer.write(rdb_contents)
        await writer.drain()
        
        # Add this connection to replicas set if not already added
        self.replicas.add(writer)

    def count_acked_replicas(self, offset: int) -> int:
        """Count how many replicas have acknowledged up to the given offset"""
        count = 0
        for replica, acked_offset in self.replica_ack_offsets.items():
            if not replica.is_closing() and acked_offset >= offset:
                count += 1
        return count
        
    def cleanup_replicas(self) -> None:
        """Remove closed connections from replicas set"""
        closed_replicas = {w for w in self.replicas if w.is_closing()}
        
        # Remove from replicas set
        self.replicas -= closed_replicas
        
        # Remove from ack tracking
        for replica in closed_replicas:
            if replica in self.replica_ack_offsets:
                del self.replica_ack_offsets[replica]

    def find_command_end(self, buffer: bytes) -> int:
        """Find the end position of a complete RESP command in the buffer"""
        if not buffer.startswith(b"*"):
            # Not a RESP array
            return 0
        
        try:
            # Find array length
            first_line_end = buffer.find(b"\r\n")
            if first_line_end == -1:
                return 0  # Incomplete command
            
            array_length = int(buffer[1:first_line_end])
            
            # Expected number of \r\n pairs
            expected_crlf = 1 + (array_length * 2)  # 1 for array marker, 2 per item
            
            # Find all \r\n occurrences
            pos = 0
            for _ in range(expected_crlf):
                pos = buffer.find(b"\r\n", pos)
                if pos == -1:
                    return 0  # Incomplete command
                pos += 2
            
            return pos  # End position of the command
            
        except (ValueError, IndexError):
            # Invalid format
            return 0 

    def parse_command_bytes(self, data: bytes) -> Tuple[str, List[str]]:
        """Parse a RESP command from bytes"""
        command = None
        args = []
        
        try:
            lines = data.split(b"\r\n")
            array_length = int(lines[0][1:])  # Skip the * in *3
            
            idx = 1
            for i in range(array_length):
                if idx < len(lines) and lines[idx].startswith(b"$"):
                    # Skip the length line
                    idx += 1
                    
                    if idx < len(lines):
                        value = lines[idx].decode("utf-8")
                        if i == 0:
                            command = value.upper()
                        else:
                            args.append(value)
                    idx += 1
        except Exception as e:
            print(f"Error parsing command bytes: {e}")
        
        return command, args 

    async def start_election(self) -> None:
        """Start an election when master heartbeat times out"""
        # Only start election if we're a slave and not already in an election
        if self.role != "slave" or self.election_state != "follower":
            print(f"Not starting election. Role: {self.role}, State: {self.election_state}")
            return
        
        # Use mutex to prevent multiple simultaneous elections
        async with self.election_mutex:
            # Check again after acquiring lock
            if self.role != "slave" or self.election_state != "follower":
                return
            
            print("Starting election for new master")
            
            # Update state to candidate
            self.election_state = "candidate"
            self.current_term += 1
            self.votes_received = 1  # Vote for ourselves
            self.voted_for = self.get_node_id()
            
            # Find Redis instance to get configuration
            from redis_server import Redis
            redis_instance = None
            for obj in gc.get_objects():
                if isinstance(obj, Redis) and obj.replication is self:
                    redis_instance = obj
                    break
            
            if not redis_instance:
                print("Error: Could not find Redis instance for election")
                self.election_state = "follower"
                return
            
            # Get priority from config
            priority = redis_instance.config.get("priority", 100)
            
            # In a real clustered system, we would:
            # 1. Discover other replicas (through a discovery service or pre-configured list)
            # 2. Send RequestVote messages to all other replicas
            # 3. Wait for votes or timeout
            # 4. If we have majority of votes, become leader
            
            # For this implementation, we'll use an election timeout with:
            # - Priority-based voting (higher priority nodes are more likely to win)
            # - Node ID as a tiebreaker (deterministic but pseudo-random)
            election_timeout = redis_instance.config.get("election_timeout", 5.0)
            
            print(f"Running for election with priority {priority}, term {self.current_term}")
            
            # Wait for election timeout to allow other nodes to start their elections too
            await asyncio.sleep(election_timeout)
            
            # In this simplified implementation, we'll determine if we should be leader based on:
            # 1. Our priority
            # 2. Our node ID (as a tiebreaker)
            
            # In real Raft, the node would check if it got votes from majority of the cluster
            # Here, we'll simulate a deterministic election based on node characteristics
            
            # Check if we're still a candidate (we might have seen a heartbeat from another node)
            if self.election_state != "candidate":
                print(f"No longer a candidate, current state: {self.election_state}")
                return
            
            # Get our node ID as a tiebreaker
            node_id = self.get_node_id()
            
            # For demonstration purposes, we'll use a deterministic approach:
            # If our priority is high enough and our node ID hash is favorable,
            # we'll declare ourselves the winner
            
            # This number would normally be derived from cluster state (e.g., total number of nodes)
            # For simplicity, we'll just use a fixed threshold 
            # In a real implementation, this would be much more sophisticated
            min_priority_threshold = 75
            
            if priority >= min_priority_threshold:
                # Use node ID as tiebreaker - this ensures only one node wins
                # In a real system, this would be determined by actual voting
                node_hash = int(hashlib.md5(node_id.encode()).hexdigest(), 16) % 100
                
                print(f"Election decision: Priority={priority}, Node hash={node_hash}")
                
                # If node has high priority and favorable hash, become master
                if node_hash > 50:  # Arbitrary threshold for demonstration
                    print(f"Won election with priority {priority} and favorable node hash")
                    await self.become_master()
                else:
                    print(f"Lost election despite high priority, unfavorable node hash")
                    self.election_state = "follower"
            else:
                print(f"Lost election with insufficient priority {priority}")
                self.election_state = "follower"
            
    async def become_master(self) -> None:
        """Become the master after winning an election"""
        print("Becoming master node")
        
        # Update role and election state
        self.role = "master"
        self.election_state = "leader"
        
        # Increment generation when becoming leader
        self.generation += 1
        print(f"New leader with generation {self.generation}")
        
        # Reset replication-related fields
        self.master_host = None
        self.master_port = None
        
        # Generate a new replication ID
        self.master_replid = uuid.uuid4().hex
        
        # Reset offset
        self.master_repl_offset = 0
        
        # Start sending heartbeats to slaves
        await self.start_heartbeat()
        
        # Resolve conflicts
        await self.resolve_conflicts()
        
        # Announce to other nodes
        await self.announce_master()
        
        # Broadcast new role to any replicas
        print(f"This node is now the master with replid {self.master_replid} and generation {self.generation}")
        
    async def resolve_conflicts(self) -> None:
        """Resolve conflicts in data store when becoming master"""
        # This is called after becoming a master
        
        # Find Redis instance
        from redis_server import Redis
        redis_instance = None
        for obj in gc.get_objects():
            if isinstance(obj, Redis) and obj.replication is self:
                redis_instance = obj
                break
                
        if not redis_instance:
            print("Error: Could not find Redis instance for conflict resolution")
            return
            
        print("Resolving potential data conflicts after election...")
        
        # In a real system, this would involve checking log entries
        # For simplicity, we'll just use the generation info stored with each value
        
        # Log that we're now using our generation for all future writes
        print(f"All future writes will use generation {self.generation}")
        
        # Save the current data store state after resolution
        await redis_instance._save_rdb()
        print("Saved resolved state to RDB")
        
    def get_node_id(self) -> str:
        """Get the node ID for this instance"""
        # Find Redis instance
        from redis_server import Redis
        for obj in gc.get_objects():
            if isinstance(obj, Redis) and obj.replication is self:
                return obj.config.get("node_id", "unknown")
        return "unknown" 

    async def discover_replicas(self) -> List[Tuple[str, int]]:
        """Discover other replicas in the cluster"""
        # In a real system, this would use a discovery service or gossip protocol
        # For this implementation, we'll simulate by checking common ports
        
        # Find our node ID
        node_id = self.get_node_id()
        
        # Find our port
        from redis_server import Redis
        redis_instance = None
        for obj in gc.get_objects():
            if isinstance(obj, Redis) and obj.replication is self:
                redis_instance = obj
                break
        
        if not redis_instance:
            print("Error: Could not find Redis instance for replica discovery")
            return []
        
        our_port = redis_instance.port
        
        # List of common ports to check
        common_ports = [6379, 6380, 6381, 6382, 6383]
        discovered_replicas = []
        
        for port in common_ports:
            # Skip our own port
            if port == our_port:
                continue
            
            try:
                # Try to connect to potential replica
                print(f"Attempting to discover replica at localhost:{port}")
                
                # Connect with a short timeout
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection('localhost', port),
                    timeout=0.5
                )
                
                # Send INFO command to check if it's a Redis server
                writer.write(RESPProtocol.encode_array(["INFO", "replication"]))
                await writer.drain()
                
                # Read response (with timeout)
                response = await asyncio.wait_for(reader.read(1024), timeout=0.5)
                
                # Parse INFO response to check if it's a replica
                if b"role:" in response:
                    print(f"Found Redis instance at localhost:{port}")
                    discovered_replicas.append(('localhost', port))
            
                # Clean up
                writer.close()
                await writer.wait_closed()
                
            except (asyncio.TimeoutError, ConnectionRefusedError, OSError):
                # Connection failed or timed out, likely not a Redis server
                pass
        
        print(f"Discovered {len(discovered_replicas)} potential replicas: {discovered_replicas}")
        return discovered_replicas 

    async def announce_master(self) -> None:
        """Announce this node as the new master to other nodes"""
        print("Announcing this node as new master to other nodes...")
        
        # Discover other nodes in the cluster
        discovered_replicas = await self.discover_replicas()
        
        # Our node ID and generation
        node_id = self.get_node_id()
        
        for host, port in discovered_replicas:
            try:
                print(f"Announcing to node at {host}:{port}")
                
                # Connect to the other node
                reader, writer = await asyncio.open_connection(host, port)
                
                # Send CLUSTER MASTER_ANNOUNCE command
                # This is a custom command we're defining for master announcements
                announce_cmd = [
                    "CLUSTER", 
                    "MASTER_ANNOUNCE",
                    f"node_id={node_id}",
                    f"generation={self.generation}",
                    f"replid={self.master_replid}"
                ]
                
                writer.write(RESPProtocol.encode_array(announce_cmd))
                await writer.drain()
                
                # Wait for response
                response = await reader.read(1024)
                print(f"Got response from {host}:{port}: {response}")
                
                # Clean up
                writer.close()
                await writer.wait_closed()
                
            except Exception as e:
                print(f"Error announcing master to {host}:{port}: {e}") 

    async def _connect_to_new_master(self, master_node_id: str) -> None:
        """Connect to a new master after learning its node ID"""
        # Use discovery to find the node
        discovered_nodes = await self.discover_replicas()
        
        for host, port in discovered_nodes:
            try:
                print(f"Checking if {host}:{port} has node_id {master_node_id}")
                reader, writer = await asyncio.open_connection(host, port)
                
                # Send a custom command to get node ID
                writer.write(RESPProtocol.encode_array(["CONFIG", "GET", "node_id"]))
                await writer.drain()
                
                response = await reader.read(1024)
                
                # If this is the node we're looking for
                if master_node_id.encode() in response:
                    print(f"Found node {master_node_id} at {host}:{port}")
                    self.master_host = host
                    self.master_port = port
                    
                    # Close this test connection
                    writer.close()
                    await writer.wait_closed()
                    
                    # Connect as a replica
                    new_reader, new_writer = await self.connect_to_master()
                    if new_reader and new_writer:
                        # Reset heartbeat time
                        self.last_master_heartbeat = time.time()
                        # Start master connection handler
                        asyncio.create_task(
                            self.handle_master_connection(new_reader, new_writer)
                        )
                    return
                
                # Close connection if it's not the node we're looking for
                writer.close()
                await writer.wait_closed()
                
            except Exception as e:
                print(f"Error checking {host}:{port} for node_id {master_node_id}: {e}") 