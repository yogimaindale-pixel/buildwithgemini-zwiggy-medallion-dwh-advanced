"""Configuration Management for Zwiggy Medallion Data Warehouse.

Provides typed, validated configuration loading from environment variables or .env files
with secret masking for logs and CLI displays.
"""

from dataclasses import dataclass
import os
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv


class ConfigError(Exception):
    """Raised when required configuration settings are missing or invalid."""
    pass


@dataclass(frozen=True)
class DbTarget:
    """Database target credentials and connection configuration."""
    host: str
    port: int
    dbname: str
    user: str
    password: str

    def masked_repr(self) -> str:
        """Return connection URI string with password safely masked."""
        return f"postgresql://{self.user}:***@{self.host}:{self.port}/{self.dbname}"

    def __repr__(self) -> str:
        return self.masked_repr()


class Settings:
    """Application settings loaded from environment variables and .env files."""

    def __init__(self, env_file: Optional[str] = None) -> None:
        paths_to_check = []
        if env_file:
            paths_to_check.append(Path(env_file))
        if os.getenv("ZWIGGY_ENV_FILE"):
            paths_to_check.append(Path(os.getenv("ZWIGGY_ENV_FILE")))
        
        root_dir = Path(__file__).resolve().parent.parent.parent
        paths_to_check.extend([
            root_dir / ".env",
            root_dir.parent / ".env",
            Path(".env")
        ])

        for path in paths_to_check:
            if path.exists():
                load_dotenv(dotenv_path=path, override=False)
                break

        # Source Database Credentials
        self.pg_host: str = os.getenv("PG_HOST", "localhost")
        self.pg_port: int = self._parse_int("PG_PORT", 5432)
        self.pg_database: str = os.getenv("PG_DATABASE", "zwiggy_db")
        self.pg_user: str = os.getenv("PG_USER", "postgres")
        self.pg_password: str = os.getenv("PG_PASSWORD", "postgres")

        # Warehouse Database Credentials (defaults to source database if omitted)
        self.warehouse_host: str = os.getenv("WAREHOUSE_HOST", self.pg_host)
        self.warehouse_port: int = self._parse_int("WAREHOUSE_PORT", self.pg_port)
        self.warehouse_database: str = os.getenv("WAREHOUSE_DATABASE", self.pg_database)
        self.warehouse_user: str = os.getenv("WAREHOUSE_USER", self.pg_user)
        self.warehouse_password: str = os.getenv("WAREHOUSE_PASSWORD", self.pg_password)

        # Operational & Pipeline Constants
        self.default_lookback_days: int = self._parse_int("DEFAULT_LOOKBACK_DAYS", 7)
        self.batch_size: int = self._parse_int("BATCH_SIZE", 10000)
        self.business_timezone: str = os.getenv("BUSINESS_TIMEZONE", "UTC")
        self.code_version: str = os.getenv("CODE_VERSION", "1.0.0")
        self.statement_timeout_ms: int = self._parse_int("STATEMENT_TIMEOUT_MS", 60000)

        # Resilience & Quality Thresholds
        self.max_retries: int = self._parse_int("MAX_RETRIES", 3)
        self.retry_backoff_seconds: float = self._parse_float("RETRY_BACKOFF_SECONDS", 2.0)
        self.unknown_sk_block_threshold: float = self._parse_float("UNKNOWN_SK_BLOCK_THRESHOLD", 0.05)

        # Directory Paths
        self.root_dir: Path = root_dir
        self.sql_dir: Path = root_dir / "src" / "zwiggy_dwh" / "sql"

    def _parse_int(self, env_var: str, default: int) -> int:
        val = os.getenv(env_var)
        if val is None:
            return default
        try:
            return int(val)
        except ValueError:
            raise ConfigError(f"Environment variable '{env_var}' must be an integer, got '{val}'.")

    def _parse_float(self, env_var: str, default: float) -> float:
        val = os.getenv(env_var)
        if val is None:
            return default
        try:
            return float(val)
        except ValueError:
            raise ConfigError(f"Environment variable '{env_var}' must be a float, got '{val}'.")

    @property
    def source_target(self) -> DbTarget:
        """Get DbTarget for source OLTP database."""
        return DbTarget(
            host=self.pg_host,
            port=self.pg_port,
            dbname=self.pg_database,
            user=self.pg_user,
            password=self.pg_password
        )

    @property
    def warehouse_target(self) -> DbTarget:
        """Get DbTarget for warehouse database."""
        return DbTarget(
            host=self.warehouse_host,
            port=self.warehouse_port,
            dbname=self.warehouse_database,
            user=self.warehouse_user,
            password=self.warehouse_password
        )


_settings_instance: Optional[Settings] = None


def settings(reload: bool = False) -> Settings:
    """Singleton getter for Settings instance.

    Args:
        reload: If True, force reloads settings from environment.

    Returns:
        Settings: Configured settings instance.
    """
    global _settings_instance
    if _settings_instance is None or reload:
        _settings_instance = Settings()
    return _settings_instance
