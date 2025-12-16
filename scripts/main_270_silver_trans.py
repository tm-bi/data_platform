from __future__ import annotations

import logging
import os
from dataclasses import dataclass
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine, URL
from pathlib import Path
from datetime import date
from dotenv import load_dotenv
from typing import Optional

#-----------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger("silver_270")

#-----------------------------------------------------------------

@dataclass(frozen=True)
class AppConfig:
    db_schema: str
    src_table_270: str
    ods_base_path: Path
    target_table: str
    load_date: str


def load_env(env_path: Optional[Path] = None) -> None:
    if env_path is None:
        # mantendo o seu padrão original: /root/data_platform/.env (um nível acima de scripts)
        env_path = Path(__file__).resolve().parents[1] /"config"/".env"

    if not env_path.exists():
        raise FileNotFoundError(f"Arquivo .env não encontrado em: {env_path}")

    # override=True evita “pegar” variáveis antigas do ambiente
    load_dotenv(dotenv_path=env_path, override=True)
    logger.info("Variáveis de ambiente carregadas de %s", env_path)

def _required_env(name: str) -> str:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        raise RuntimeError(f"Variável obrigatória ausente ou vazia no .env: {name}")
    return value.strip()


def get_engine() -> Engine:
    host = _required_env("PG_HOST")
    port = int(_required_env("PG_PORT"))
    db_name = _required_env("PG_DB")
    user = _required_env("PG_USER")
    password = _required_env("PG_PASSWORD")

    url_object = URL.create(
        drivername="postgresql+psycopg2",
        username=user,
        password=password,
        host=host,
        port=port,
        database=db_name,
    )

    logger.info("Criando engine para %s", url_object.render_as_string(hide_password=True))

    # pool_pre_ping ajuda a evitar conexões “mortas”
    return create_engine(url_object, pool_pre_ping=True)


def get_app_config() -> AppConfig:
    db_schema = _required_env("DB_SCHEMA")
    src_table_270_name = _required_env("SRC_TABLE_270")
    ods_base_path = Path(_required_env("ODS_BASE_PATH"))

    target_table = "novaxs_270_silver_trans"
    load_date_str = date.today().isoformat()

    return AppConfig(
        db_schema=db_schema,
        src_table_270=f"{db_schema}.{src_table_270_name}",
        ods_base_path=ods_base_path,
        target_table=target_table,
        load_date=load_date_str,
    )


def build_output_paths(cfg: AppConfig) -> tuple[Path, Path]:
    out_dir = cfg.ods_base_path / cfg.target_table / f"load_date={cfg.load_date}"
    out_dir.mkdir(parents=True, exist_ok=True)

    out_file = out_dir / f"{cfg.target_table}.parquet"
    return out_dir, out_file


# ----------------------------- transform -----------------------------

def transform_270(df: pd.DataFrame, load_date_str: str) -> pd.DataFrame:
    df = df.copy()

    df.rename(
        columns={
            "un": "valor_un",
            "total": "valor_total",
            "quantidade_de_parcelas": "qtd_parcelas",
            "prazo_estimado_do_recebimento": "prazo_estimado_recebimento",
        },
        inplace=True,
    )

    # Inteiros
    int_cols = ["id_stg", "conta", "autorizacao", "nsu", "qtd", "qtd_parcelas"]
    for col in int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    # Textos
    text_cols = [
        "cliente", "endereco", "cidade", "celular", "telefone", "email", "agencia",
        "produto", "status", "forma_de_pagamento", "usuario", "prazo_estimado_recebimento",
    ]
    for col in text_cols:
        if col in df.columns:
            df[col] = df[col].astype("string").str.strip()

    if "email" in df.columns:
        df["email"] = df["email"].str.lower()

    # Datas BR -> date
    date_cols = ["data", "data_utilizacao", "data_encerramento"]
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], format="%d/%m/%Y", errors="coerce").dt.date

    # Timestamp "criado" -> data_venda + hora_venda
    if "criado" in df.columns:
        criado_ts = pd.to_datetime(df["criado"], format="%d/%m/%Y %H:%M:%S", errors="coerce")
        df["data_venda"] = criado_ts.dt.date
        df["hora_venda"] = criado_ts.dt.strftime("%H:%M:%S")
        df.drop(columns=["criado"], inplace=True)

    # Hora utilização
    if "hora_utilizacao" in df.columns:
        df["hora_utilizacao"] = df["hora_utilizacao"].astype("string").str.strip()
        df.loc[df["hora_utilizacao"] == "", "hora_utilizacao"] = pd.NA

    # Monetários (pt-BR)
    for col in ["valor_un", "valor_total"]:
        if col in df.columns:
            s = df[col].astype("string").str.strip()
            s = s.str.replace(".", "", regex=False)   # milhar
            s = s.str.replace(",", ".", regex=False)  # decimal
            df[col] = pd.to_numeric(s, errors="coerce")

    # Coluna técnica
    df["load_date"] = pd.to_datetime(load_date_str).date()

    return df

# ----------------------------- main -----------------------------

def main() -> None:
    load_env()
    cfg = get_app_config()
    _, out_file = build_output_paths(cfg)

    engine = get_engine()

    logger.info("Lendo tabela origem: %s", cfg.src_table_270)
    df = pd.read_sql_query(f"SELECT * FROM {cfg.src_table_270}", engine)

    logger.info("Transformando %d linhas...", len(df))
    df_silver = transform_270(df, cfg.load_date)

    logger.info("Gravando Parquet em: %s", out_file)
    df_silver.to_parquet(out_file, engine="pyarrow", index=False)

    logger.info("✅ Silver Transacional criada com sucesso em: %s", out_file)


if __name__ == "__main__":
    main()

