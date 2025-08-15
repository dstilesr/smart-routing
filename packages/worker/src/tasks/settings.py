from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class WorkerSettings(BaseSettings):
    """
    Settings for the worker.
    """

    redis_host: str = Field(
        default="localhost", description="Redis server host"
    )
    redis_port: int = Field(default=6379, description="Redis server port")
    max_labels: int = Field(
        default=2,
        gt=0,
        description="Maximum number of labels to store per worker",
    )
    result_ttl: int = Field(
        default=1800,  # 30 minutes
        gt=0,
        description="Time to live for results in seconds",
    )

    model_config = SettingsConfigDict(env_prefix="WORKER_")
