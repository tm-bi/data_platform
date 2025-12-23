from __future__ import annotations

from pathlib import Path

from dotenv import load_dotenv
from pydantic_settings import BaseSettings, SettingsConfigDict


def _env_file() -> str:
    # data_platform/src/common/settings.py -> data_platform/config/.env
    base_dir = Path(__file__).resolve().parents[2]
    return str(base_dir / "config" / ".env")


load_dotenv(_env_file())


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
    env_file=_env_file(),
    env_file_encoding="utf-8",
    extra="ignore",
    )

    # PostgreSQL
    pg_host: str
    pg_port: int = 5432
    pg_db: str
    pg_user: str
    pg_password: str

    # Firebird
    firebird_host: str
    firebird_port: int = 3050
    firebird_db: str
    firebird_user: str
    firebird_password: str
    firebird_charset: str = "UTF8"

    # SQL Server
    mssql_host: str
    mssql_port: int = 1433
    mssql_db: str
    mssql_user: str
    mssql_password: str
    mssql_driver: str = "ODBC Driver 18 for SQL Server"
    mssql_encrypt: str = "yes"
    mssql_trust_cert: str = "yes"

    app_tz: str = "America/Sao_Paulo"

    def pg_dsn(self) -> str:
        # psycopg3 DSN
        return (
            f"host={self.pg_host} port={self.pg_port} dbname={self.pg_db} "
            f"user={self.pg_user} password={self.pg_password}"
        )


settings = Settings()
