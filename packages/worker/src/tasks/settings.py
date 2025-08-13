from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class WorkerSettings(BaseSettings):
    """
    Settings for the worker.
    """
    redis_host: str = Field(default="localhost")
    redis_port: int = Field(default=6379)

    model_config = SettingsConfigDict(env_prefix="WORKER_")
