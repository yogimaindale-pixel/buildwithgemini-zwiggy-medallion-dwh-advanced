"""
===============================================================================
ZWIGGY MEDALLION DATA WAREHOUSE - CONFIGURATION MANAGEMENT MODULE (config.py)
===============================================================================
Goal & Purpose:
---------------
This module manages all configuration settings, database connection details,
pipeline execution thresholds, and retry policies for the Zwiggy Data Warehouse.

Why is this module critical for Junior Developers?
1. Centralized Settings: Instead of hardcoding database passwords or ports across
   multiple files, all configuration values are loaded from environment variables (.env).
2. Secret Masking: Prevents sensitive database passwords from leaking in logs.
3. Type Validation: Automatically checks that integers (like ports) and floats
   (like retry backoffs) are formatted correctly.
4. Singleton Pattern: Ensures that configuration files are read only once per run,
   improving pipeline performance.
===============================================================================
"""

from dataclasses import dataclass
import os
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv


# -----------------------------------------------------------------------------
# CUSTOM EXCEPTION CLASS
# -----------------------------------------------------------------------------
class ConfigError(Exception):
    """
    Custom exception raised when mandatory environment variables are missing
    or contain invalid data types (e.g., passing string 'abc' for database port).
    """
    pass


# -----------------------------------------------------------------------------
# DATABASE TARGET CREDENTIALS DATACLASS
# -----------------------------------------------------------------------------
@dataclass(frozen=True)
class DbTarget:
    """
    Immutable data class storing database host, port, database name, user, and password.
    
    Attributes:
        host (str): Database host server IP or hostname (e.g., 'localhost').
        port (int): Database network port (e.g., 5432 for PostgreSQL).
        dbname (str): Target database name (e.g., 'zwiggy_dwh').
        user (str): Authorized database username.
        password (str): Secret password for database authentication.
    """
    host: str
    port: int
    dbname: str
    user: str
    password: str

    def masked_repr(self) -> str:
        """
        Safely generates a connection URI with the password masked as '***'.
        This prevents accidental credential exposure in application logs.
        """
        return f"postgresql://{self.user}:***@{self.host}:{self.port}/{self.dbname}"

    def __repr__(self) -> str:
        """Standard representation calls the masked string for safety."""
        return self.masked_repr()


# -----------------------------------------------------------------------------
# SETTINGS MANAGEMENT CLASS
# -----------------------------------------------------------------------------
class Settings:
    """
    Loads, parses, and validates settings from environment variables or .env files.
    """

    def __init__(self, env_file: Optional[str] = None) -> None:
        """
        Initializes the settings container by searching for local .env files.
        
        Parameters:
            env_file (Optional[str]): Explicit path to a .env configuration file.
        """
        paths_to_check = []
        
        # 1. Check explicitly passed env file path
        if env_file:
            paths_to_check.append(Path(env_file))
            
        # 2. Check environment variable ZWIGGY_ENV_FILE
        if os.getenv("ZWIGGY_ENV_FILE"):
            paths_to_check.append(Path(os.getenv("ZWIGGY_ENV_FILE")))
        
        # 3. Locate the project root folder dynamically relative to this file
        root_dir = Path(__file__).resolve().parent.parent.parent
        paths_to_check.extend([
            root_dir / ".env",
            root_dir.parent / ".env",
            Path(".env")
        ])

        # Load the first existing .env file found in the search order
        for path in paths_to_check:
            if path.exists():
                load_dotenv(dotenv_path=path, override=False)
                break

        # ---------------------------------------------------------------------
        # SOURCE OLTP DATABASE CREDENTIALS (Where raw data comes from)
        # ---------------------------------------------------------------------
        self.pg_host: str = os.getenv("PG_HOST", "localhost")
        self.pg_port: int = self._parse_int("PG_PORT", 5432)
        self.pg_database: str = os.getenv("PG_DATABASE", "zwiggy_db")
        self.pg_user: str = os.getenv("PG_USER", "postgres")
        self.pg_password: str = os.getenv("PG_PASSWORD", "postgres")

        # ---------------------------------------------------------------------
        # WAREHOUSE DATABASE CREDENTIALS (Where Medallion tables live)
        # ---------------------------------------------------------------------
        self.warehouse_host: str = os.getenv("WAREHOUSE_HOST", self.pg_host)
        self.warehouse_port: int = self._parse_int("WAREHOUSE_PORT", self.pg_port)
        self.warehouse_database: str = os.getenv("WAREHOUSE_DATABASE", self.pg_database)
        self.warehouse_user: str = os.getenv("WAREHOUSE_USER", self.pg_user)
        self.warehouse_password: str = os.getenv("WAREHOUSE_PASSWORD", self.pg_password)

        # ---------------------------------------------------------------------
        # PIPELINE BATCH & TIMEOUT CONSTANTS
        # ---------------------------------------------------------------------
        self.default_lookback_days: int = self._parse_int("DEFAULT_LOOKBACK_DAYS", 7)
        self.batch_size: int = self._parse_int("BATCH_SIZE", 10000)
        self.business_timezone: str = os.getenv("BUSINESS_TIMEZONE", "UTC")
        self.code_version: str = os.getenv("CODE_VERSION", "1.0.0")
        self.statement_timeout_ms: int = self._parse_int("STATEMENT_TIMEOUT_MS", 60000)

        # ---------------------------------------------------------------------
        # RESILIENCE, RETRY & DATA QUALITY THRESHOLDS
        # ---------------------------------------------------------------------
        self.max_retries: int = self._parse_int("MAX_RETRIES", 3)
        self.retry_backoff_seconds: float = self._parse_float("RETRY_BACKOFF_SECONDS", 2.0)
        self.unknown_sk_block_threshold: float = self._parse_float("UNKNOWN_SK_BLOCK_THRESHOLD", 0.05)

        # ---------------------------------------------------------------------
        # DIRECTORY PATHS
        # ---------------------------------------------------------------------
        self.root_dir: Path = root_dir
        self.sql_dir: Path = root_dir / "src" / "zwiggy_dwh" / "sql"

    def _parse_int(self, env_var: str, default: int) -> int:
        """Helper to convert environment variable string to integer safely."""
        val = os.getenv(env_var)
        if val is None:
            return default
        try:
            return int(val)
        except ValueError:
            raise ConfigError(f"Environment variable '{env_var}' must be an integer, got '{val}'.")

    def _parse_float(self, env_var: str, default: float) -> float:
        """Helper to convert environment variable string to float safely."""
        val = os.getenv(env_var)
        if val is None:
            return default
        try:
            return float(val)
        except ValueError:
            raise ConfigError(f"Environment variable '{env_var}' must be a float, got '{val}'.")

    @property
    def source_target(self) -> DbTarget:
        """Helper property returning DbTarget instance for the Source Database."""
        return DbTarget(
            host=self.pg_host,
            port=self.pg_port,
            dbname=self.pg_database,
            user=self.pg_user,
            password=self.pg_password
        )

    @property
    def warehouse_target(self) -> DbTarget:
        """Helper property returning DbTarget instance for the Data Warehouse."""
        return DbTarget(
            host=self.warehouse_host,
            port=self.warehouse_port,
            dbname=self.warehouse_database,
            user=self.warehouse_user,
            password=self.warehouse_password
        )


# -----------------------------------------------------------------------------
# SINGLETON INSTANCE HOLDER
# -----------------------------------------------------------------------------
_settings_instance: Optional[Settings] = None


def settings(reload: bool = False) -> Settings:
    """
    Global accessor for application settings using the Singleton pattern.
    
    Parameters:
        reload (bool): If True, re-instantiates Settings from environment.
        
    Returns:
        Settings: Configured settings singleton object.
    """
    global _settings_instance
    if _settings_instance is None or reload:
        _settings_instance = Settings()
    return _settings_instance
