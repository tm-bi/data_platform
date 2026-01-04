from __future__ import annotations

from pathlib import Path

from dotenv import load_dotenv
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


def _env_file() -> str:
    base_dir = Path(__file__).resolve().parents[2]
    return str(base_dir / "config" / ".env")


load_dotenv(_env_file())


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=_env_file(),          # volta a usar o caminho absoluto certo
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,          # permite APP_TZ -> app_tz etc
    )

    # PostgreSQL (aceita PG_HOST do .env)
    pg_host: str = Field(default="localhost", alias="pg_host")
    pg_port: int = Field(default=5432, alias="pg_port")
    pg_db: str = Field(default="postgres", alias="pg_db")
    pg_user: str = Field(alias="pg_user")
    pg_password: str = Field(alias="pg_password")

    # Firebird
    firebird_host: str = Field(alias="firebird_host")
    firebird_port: int = Field(default=3050, alias="firebird_port")
    firebird_db: str = Field(alias="firebird_db")
    firebird_user: str = Field(alias="firebird_user")
    firebird_password: str = Field(alias="firebird_password")
    firebird_charset: str = Field(default="UTF8", alias="firebird_charset")

    # SQL Server
    mssql_host: str = Field(alias="mssql_host")
    mssql_port: int = Field(default=1433, alias="mssql_port")
    mssql_db: str = Field(alias="mssql_db")
    mssql_user: str = Field(alias="mssql_user")
    mssql_password: str = Field(alias="mssql_password")
    mssql_driver: str = Field(default="ODBC Driver 18 for SQL Server", alias="mssql_driver")
    mssql_encrypt: str = Field(default="yes", alias="mssql_encrypt")
    mssql_trust_cert: str = Field(default="yes", alias="mssql_trust_cert")

    app_tz: str = Field(default="America/Sao_Paulo", alias="app_tz")
    quality_terminal_ids: str = Field(alias="quality_terminal_ids")

    force_run: bool = Field(default=False, alias="force_run")

    def pg_dsn(self) -> str:
        return (
            f"host={self.pg_host} port={self.pg_port} dbname={self.pg_db} "
            f"user={self.pg_user} password={self.pg_password}"
        )


settings = Settings()
