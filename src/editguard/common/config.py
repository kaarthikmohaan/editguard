"""Settings read from environment variables and .env, validated at startup."""

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from editguard import __version__

TARGET_WIKIS = (
    "enwiki",
    "bnwiki",
    "hiwiki",
    "knwiki",
    "mlwiki",
    "mrwiki",
    "tawiki",
    "tewiki",
)


class Settings(BaseSettings):
    """All runtime settings. Every field can be overridden by an env var of the same name."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore", frozen=True)

    kafka_bootstrap_servers: str = "127.0.0.1:9092,127.0.0.1:9094,127.0.0.1:9096"
    schema_registry_url: str = "http://127.0.0.1:8081"
    contact_email: str = Field(min_length=3, pattern=r".+@.+")
    log_level: str = "INFO"

    @property
    def user_agent(self) -> str:
        """Identifies us to Wikimedia, as their User-Agent policy requires."""
        return (
            f"EditGuard/{__version__} "
            f"(https://github.com/kaarthikmohaan/editguard; {self.contact_email})"
        )


@lru_cache
def get_settings() -> Settings:
    """Load settings once per process."""
    return Settings()
