from pydantic import Field
from typing import Literal
from pydantic_settings import BaseSettings, SettingsConfigDict

_LEVEL = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]


class LoggingSettings(BaseSettings):
    """
    Settings for logging configuration in the worker.
    """

    base_level: _LEVEL = Field(
        default="INFO", description="Base logging level for the worker"
    )

    collector_enabled: bool = Field(
        default=False, description="Enable or disable the log collector service"
    )
    collector_host: str = Field(
        default="http://localhost:8001",
        description="Host for the log collector service",
    )
    collector_level: _LEVEL = Field(
        default="INFO",
        description="Logging level to send to the collector service",
    )

    model_config = SettingsConfigDict(env_prefix="LOG_")
