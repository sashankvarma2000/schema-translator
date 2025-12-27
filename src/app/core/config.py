"""Configuration management using Pydantic Settings."""

from pathlib import Path
from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings."""
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False
    )
    
    # Paths
    project_root: Path = Field(default_factory=lambda: Path.cwd())
    canonical_schema_path: Path = Field(
        default_factory=lambda: Path.cwd() / "canonical_schema_original.yaml"
    )
    customer_schemas_dir: Path = Field(
        default_factory=lambda: Path.cwd() / "customer_schemas"
    )
    customer_samples_dir: Path = Field(
        default_factory=lambda: Path.cwd() / "customer_samples"
    )
    prompts_dir: Path = Field(
        default_factory=lambda: Path.cwd() / "prompts"
    )
    output_dir: Path = Field(
        default_factory=lambda: Path.cwd() / "output"
    )
    
    # LLM Configuration
    openai_api_key: Optional[str] = Field(default=None, alias="OPENAI_API_KEY")
    openai_model: str = Field(default="gpt-5.2")
    openai_temperature: float = Field(default=0.1)
    openai_max_tokens: int = Field(default=2000)
    
    # Mapping Thresholds
    auto_accept_threshold: float = Field(default=0.75)
    hitl_threshold: float = Field(default=0.5)
    
    # Scoring Weights
    weight_llm: float = Field(default=0.5)
    weight_name: float = Field(default=0.2)
    weight_type: float = Field(default=0.2)
    weight_profile: float = Field(default=0.1)
    
    # API Configuration
    api_host: str = Field(default="0.0.0.0")
    api_port: int = Field(default=8000)
    api_reload: bool = Field(default=False)
    
    # Logging
    log_level: str = Field(default="INFO")
    
    # Profiling
    max_sample_values: int = Field(default=10)
    max_cooccurrence_columns: int = Field(default=5)
    
    def model_post_init(self, __context) -> None:
        """Create directories if they don't exist."""
        for path_field in [
            "customer_schemas_dir",
            "customer_samples_dir", 
            "prompts_dir",
            "output_dir"
        ]:
            path = getattr(self, path_field)
            if path:
                path.mkdir(parents=True, exist_ok=True)


# Global settings instance
settings = Settings()

