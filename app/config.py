from typing import Dict, Any, List, Optional
import uuid
import socket

class Config:
    """Handles Redis server configuration"""
    
    def __init__(self):
        # Default configuration values
        self._config: Dict[str, Any] = {
            "dir": ".",
            "dbfilename": "dump.rdb",
            "heartbeat_interval": 5.0,  # Seconds between heartbeats
            "heartbeat_timeout": 15.0,  # Seconds before considering master down
            "election_timeout": 5.0,    # Seconds for election process
            "node_id": self._generate_node_id(),  # Unique ID for this node
            "priority": 100,  # Higher priority nodes are more likely to become master
            "cluster_enabled": True     # Enable cluster features
        }
    
    def _generate_node_id(self) -> str:
        """Generate a unique node ID"""
        hostname = socket.gethostname()
        return f"{hostname}-{uuid.uuid4().hex[:8]}"
    
    def set(self, param: str, value: Any) -> None:
        """Set configuration parameter"""
        self._config[param.lower()] = value
    
    def get(self, param: str, default=None) -> Any:
        """Get configuration parameter value"""
        return self._config.get(param.lower(), default)
    
    def get_multiple(self, params: List[str]) -> Dict[str, Any]:
        """Get multiple configuration parameters"""
        return {param.lower(): self.get(param) for param in params} 