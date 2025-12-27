"""
Configuration Manager for Schema Translator
Handles loading and managing configuration from YAML files
"""

import yaml
import os
from pathlib import Path
from typing import Dict, Any, Optional
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)

@dataclass
class TenantConfig:
    """Configuration for a specific tenant"""
    id: str
    display_name: str
    description: str
    schema_path: str
    field_mappings: Dict[str, Any]
    primary_table: str
    complexity: str

@dataclass
class ServerConfig:
    """Server configuration settings"""
    web_host: str
    web_port: int
    api_host: str
    api_port: int
    debug: bool
    base_url: str

@dataclass
class LLMConfig:
    """LLM configuration settings"""
    provider: str
    model: str
    temperature: float
    max_tokens: int
    timeout: int
    retry_attempts: int

class ConfigManager:
    """Manages configuration loading and access"""
    
    def __init__(self, config_dir: str = "config"):
        self.config_dir = Path(config_dir)
        self._tenant_config = None
        self._server_config = None
        self._load_configs()
    
    def _load_configs(self):
        """Load all configuration files"""
        try:
            # Load tenant configuration
            tenant_config_path = self.config_dir / "tenant_config.yaml"
            if tenant_config_path.exists():
                with open(tenant_config_path, 'r') as f:
                    self._tenant_config = yaml.safe_load(f)
            else:
                logger.warning(f"Tenant config not found: {tenant_config_path}")
                self._tenant_config = {}
            
            # Load server configuration
            server_config_path = self.config_dir / "server_config.yaml"
            if server_config_path.exists():
                with open(server_config_path, 'r') as f:
                    self._server_config = yaml.safe_load(f)
            else:
                logger.warning(f"Server config not found: {server_config_path}")
                self._server_config = {}
                
        except Exception as e:
            logger.error(f"Error loading configuration: {e}")
            self._tenant_config = {}
            self._server_config = {}
    
    def get_tenant_config(self, tenant_id: str) -> Optional[TenantConfig]:
        """Get configuration for a specific tenant"""
        if not self._tenant_config or 'tenants' not in self._tenant_config:
            return None
            
        tenant_data = self._tenant_config['tenants'].get(tenant_id)
        if not tenant_data:
            return None
            
        return TenantConfig(
            id=tenant_id,
            display_name=tenant_data.get('display_name', tenant_id),
            description=tenant_data.get('description', ''),
            schema_path=tenant_data.get('schema_path', ''),
            field_mappings=tenant_data.get('field_mappings', {}),
            primary_table=tenant_data.get('primary_table', ''),
            complexity=tenant_data.get('complexity', 'medium')
        )
    
    def get_all_tenants(self) -> Dict[str, TenantConfig]:
        """Get all tenant configurations"""
        tenants = {}
        if self._tenant_config and 'tenants' in self._tenant_config:
            for tenant_id in self._tenant_config['tenants']:
                config = self.get_tenant_config(tenant_id)
                if config:
                    tenants[tenant_id] = config
        return tenants
    
    def get_demo_customers(self) -> Dict[str, Dict[str, str]]:
        """Get demo customer configurations"""
        if self._tenant_config and 'demo_customers' in self._tenant_config:
            return self._tenant_config['demo_customers']
        return {}
    
    def get_server_config(self) -> ServerConfig:
        """Get server configuration"""
        server_data = self._server_config.get('server', {})
        web_data = server_data.get('web_dashboard', {})
        api_data = server_data.get('api_server', {})
        
        return ServerConfig(
            web_host=web_data.get('host', '0.0.0.0'),
            web_port=web_data.get('port', 8080),
            api_host=api_data.get('host', '0.0.0.0'),
            api_port=api_data.get('port', 8001),
            debug=web_data.get('debug', True),
            base_url=server_data.get('base_url', 'http://localhost')
        )
    
    def get_llm_config(self) -> LLMConfig:
        """Get LLM configuration"""
        llm_data = self._server_config.get('llm', {})
        
        return LLMConfig(
            provider=llm_data.get('provider', 'openai'),
            model=llm_data.get('model', 'gpt-5.2'),
            temperature=llm_data.get('temperature', 0.1),
            max_tokens=llm_data.get('max_tokens', 4000),
            timeout=llm_data.get('timeout', 30),
            retry_attempts=llm_data.get('retry_attempts', 3)
        )
    
    def get_field_mapping(self, tenant_id: str, canonical_field: str) -> Optional[Dict[str, Any]]:
        """Get field mapping for a specific tenant and canonical field"""
        tenant_config = self.get_tenant_config(tenant_id)
        if not tenant_config:
            return None
            
        return tenant_config.field_mappings.get(canonical_field)
    
    def get_default_settings(self) -> Dict[str, Any]:
        """Get default settings"""
        return self._tenant_config.get('defaults', {})
    
    def reload_config(self):
        """Reload configuration from files"""
        self._load_configs()
    
    def is_valid_tenant(self, tenant_id: str) -> bool:
        """Check if a tenant ID is valid"""
        if not self._tenant_config:
            return False
            
        return (tenant_id in self._tenant_config.get('tenants', {}) or 
                tenant_id in self._tenant_config.get('demo_customers', {}))
    
    def get_tenant_display_name(self, tenant_id: str) -> str:
        """Get display name for a tenant"""
        # Check regular tenants
        tenant_config = self.get_tenant_config(tenant_id)
        if tenant_config:
            return tenant_config.display_name
            
        # Check demo customers
        demo_customers = self.get_demo_customers()
        if tenant_id in demo_customers:
            return demo_customers[tenant_id].get('display_name', tenant_id)
            
        return tenant_id
    
    def get_performance_settings(self) -> Dict[str, Any]:
        """Get performance settings"""
        return self._server_config.get('performance', {})
    
    def get_logging_config(self) -> Dict[str, Any]:
        """Get logging configuration"""
        return self._server_config.get('logging', {})

# Global configuration manager instance
config_manager = ConfigManager()
