"""Logging configuration for schema translator."""

import logging
import sys
from typing import Optional

from rich.console import Console
from rich.logging import RichHandler


def setup_logging(
    level: str = "INFO",
    format_string: Optional[str] = None,
    use_rich: bool = True
) -> logging.Logger:
    """Set up logging with optional Rich formatting."""
    
    # Remove existing handlers
    root_logger = logging.getLogger()
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Set level
    log_level = getattr(logging, level.upper(), logging.INFO)
    root_logger.setLevel(log_level)
    
    if use_rich:
        console = Console(stderr=True)
        handler = RichHandler(
            console=console,
            show_time=True,
            show_path=True,
            rich_tracebacks=True
        )
    else:
        handler = logging.StreamHandler(sys.stderr)
        if format_string is None:
            format_string = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        formatter = logging.Formatter(format_string)
        handler.setFormatter(formatter)
    
    root_logger.addHandler(handler)
    
    # Return logger for this module
    return logging.getLogger("schema_translator")


# Default logger
logger = setup_logging()

