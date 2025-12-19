from __future__ import annotations

import logging
import os
import re
import csv
from io import StringIO
from pathlib import Path
from typing import Optional

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, URL

# ------------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] %(message)s",
)

HEADER_PREFIX = "Id;Tipo;Nome;Data Venda;"

STOP_MARKERS = (
    '"Total Geral"',
    '"Total";'
)

FFILL_COLS_664 = [
    "Id",
    "Tipo",
    "Nome",
    "Data Venda",
    "Checkin",
    "Checkout",
    "Valor",
    "Cupom",
    "Nome do Cupom",
    "Status",
    "Data inicial de utilização",
    "Data final de utilização",
]

SILVER_COLUMNS = [
    "id_venda",
    "tipo_produto",
    "cliente",
    "dt_venda",
    "dt_utilizacao",
    "dt_saida",
    "vlr_total",
    "cod_cupom",
    "nome_cupom",
    "status",
    "nome_desconto",
    "produto",
    "dt_inicial_uso_cupom",
    "dt_final_uso_cupom",
    "ingested_at",
    "fonte_tabela_bronze",
]

# ------------------------------------------------------------------------------

def load_env(env_path: Optional[Path] = None) -> None:
    if env_path is None:
        env_path = Path(__file__).resolve().parents[1] / "config" / ".env"

    if not env_path.exists():
        raise FileNotFoundError(f"Arquivo .env não encontrado em: {env_path}")

    load_dotenv(dotenv_path=env_path)
    logging.info("Variáveis de ambiente carregadas de %s", env_path)


def get_engine() -> Engine:
    host = os.getenv("PG_HOST")
    port = os.getenv("PG_PORT", "5432")
    db_name = os.getenv("PG_DB")
    user = os.getenv("PG_USER")
    password = os.getenv("PG_PASSWORD")

    if not all([host, port, db_name, user, password]):
        raise RuntimeError("Variáveis de ambiente de banco incompletas. Verifique o .env")

    url_object = URL.create(
        drivername="postgresql+psycopg2",
        username=user,
        password=password,
        host=host,
        port=int(port),
        database=db_name,
    )

    logging.info("Criando engine para %s", url_object.render_as_string(hide_password=True))
    return create_engine(url_object, pool_pre_ping=True)


def list_bronze_tables_664(engine: Engine) -> list[str]:
    sql = text(
        """
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = '_bronze'
          AND tablename LIKE '%\\_664' ESCAPE '\\'
        ORDER BY tablename;
        """
    )
    with engine.connect() as conn:
        return [r[0] for r in conn.execute(sql).fetchall()]


def derive_ano_mes_from_table(table_name: str) -> str:
    m = re.search(r"t_(\d{6})_664$", table_name)
    return m.group(1) if m else ""


def extract_main_csv_from_raw_lines(df_lines: pd.DataFrame) -> str:
    lines = df_lines["raw_line"].fillna("").astype(str).tolist()

    header_idx = None
    for i, line in enumerate(lines):
        if line.lstrip("\ufeff").startswith(HEADER_PREFIX):
            header_idx = i
            break

    if header_idx is None:
        raise ValueError(f"Cabeçalho não encontrado: prefixo {HEADER_PREFIX!r}")

    selected: list[str] = []
    for line in lines[header_idx:]:
        s = line.strip()

        if not s:
            continue

        if any(s.startswith(m) for m in STOP_MARKERS):
            break

        selected.append(line)

    if len(selected) <= 1:
        raise ValueError("Planilha principal vazia após o corte (sem dados).")

    return "\n".join(selected) + "\n"


def _is_empty_or_zero(series: pd.Series) -> pd.Series:
    s = series.astype("string").str.strip()

    s = s.replace(
        {
            "": pd.NA,
            "nan": pd.NA,
            "NaN": pd.NA,
            "None": pd.NA,
            "NULL": pd.NA,
            "null": pd.NA,
        }
    )

    num_txt = (
        s.str.replace("%", "", regex=False)
        .str.replace(".", "", regex=False)
        .str.replace(",", ".", regex=False)
    )
    num = pd.to_numeric(num_txt, errors="coerce")

    is_zero = num.fillna(1).eq(0)
    return s.isna() | is_zero


def fill_and_filter_664(df_raw: pd.DataFrame) -> pd.DataFrame:
    missing = [c for c in FFILL_COLS_664 if c not in df_raw.columns]
    if missing:
        raise ValueError(f"Colunas esperadas não encontradas no CSV: {missing}")

    df = df_raw.copy()
    df[FFILL_COLS_664] = df[FFILL_COLS_664].ffill()

    drop_mask = _is_empty_or_zero(df["Percentual de Desconto"]) | _is_empty_or_zero(df["Produtos aplicados"])
    df = df.loc[~drop_mask].copy()

    return df


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    rename_map = {
        "Id": "id_venda",
        "Tipo": "tipo_produto",
        "Nome": "cliente",
        "Data Venda": "dt_venda",
        "Checkin": "dt_utilizacao",
        "Checkout": "dt_saida",
        "Valor": "vlr_total",
        "Cupom": "cod_cupom",
        "Nome do Cupom": "nome_cupom",
        "Status": "status",
        "Percentual de Desconto": "nome_desconto",
        "Produtos aplicados": "produto",
        "Data inicial de utilização": "dt_inicial_uso_cupom",
        "Data final de utilização": "dt_final_uso_cupom"
    }

    df = df.rename(columns=rename_map)
    df = df.loc[:, [c for c in df.columns if not str(c).lower().startswith("unnamed")]]
    return df


def to_decimal_br(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.strip()
    s = s.replace({"": None, "nan": None, "None": None})
    s = s.str.replace(".", "", regex=False).str.replace(",", ".", regex=False)
    return pd.to_numeric(s, errors="coerce")


def cast_types(df: pd.DataFrame) -> pd.DataFrame:
    for c in df.columns:
        df[c] = df[c].astype("string")

    for col in ["dt_venda", "dt_utilizacao", "dt_saida", "dt_inicial_uso_cupom", "dt_final_uso_cupom"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], dayfirst=True, errors="coerce").dt.date

    if "vlr_total" in df.columns:
        df["vlr_total"] = to_decimal_br(df["vlr_total"]).round(2)

    return df


def align_df_to_silver(df: pd.DataFrame) -> pd.DataFrame:
    for col in SILVER_COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA
    return df[SILVER_COLUMNS]


def ensure_silver_table(engine: Engine, schema: str, table: str) -> None:
    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}";'))

        conn.execute(
            text(
                f'''
                CREATE TABLE IF NOT EXISTS "{schema}"."{table}" (
                    id_venda TEXT,
                    tipo_produto TEXT,
                    cliente TEXT,
                    dt_venda DATE,
                    dt_utilizacao DATE,
                    dt_saida DATE,
                    vlr_total NUMERIC(18,2),
                    cod_cupom TEXT,
                    nome_cupom TEXT,
                    status TEXT,
                    nome_desconto TEXT,
                    produto TEXT,
                    dt_inicial_uso_cupom DATE,
                    dt_final_uso_cupom DATE,
                    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    fonte_tabela_bronze TEXT
                );
                '''
            )
        )

        # conn.execute(text(f'ALTER TABLE "{schema}"."{table}" ADD COLUMN IF NOT EXISTS ingested_at TIMESTAMPTZ;'))
        # conn.execute(text(f'ALTER TABLE "{schema}"."{table}" ALTER COLUMN ingested_at SET DEFAULT now();'))


def truncate_silver_table(engine: Engine, schema: str, table: str) -> None:
    with engine.begin() as conn:
        conn.execute(text(f'TRUNCATE TABLE "{schema}"."{table}";'))


def write_to_silver_copy(engine: Engine, df: pd.DataFrame, schema: str, table: str) -> None:
    df2 = df.dropna(how="all").copy()
    df2 = align_df_to_silver(df2)

    if "ingested_at" in df2.columns:
        df2 = df2.drop(columns=["ingested_at"])

    buf = StringIO()
    writer = csv.writer(buf, delimiter="\t", quoting=csv.QUOTE_MINIMAL, lineterminator="\n")

    for row in df2.itertuples(index=False, name=None):
        writer.writerow(["" if pd.isna(v) else v for v in row])

    buf.seek(0)

    raw = engine.raw_connection()
    try:
        with raw.cursor() as cur:
            col_list = ", ".join(f'"{c}"' for c in df2.columns)
            cur.copy_expert(
                f'''
                COPY "{schema}"."{table}" ({col_list})
                FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t', NULL '', QUOTE '"')
                ''',
                buf,
            )
        raw.commit()
    finally:
        raw.close()


def process_one_bronze_table(engine: Engine, bronze_table: str) -> pd.DataFrame:
    sql = text(f'SELECT line_no, raw_line FROM _bronze."{bronze_table}" ORDER BY line_no;')
    df_lines = pd.read_sql(sql, engine)

    csv_text = extract_main_csv_from_raw_lines(df_lines)

    df_raw = pd.read_csv(
        StringIO(csv_text),
        sep=";",
        dtype="string",
        quotechar='"',
        engine="python",
    )

    df_raw = fill_and_filter_664(df_raw)

    df = normalize_columns(df_raw)
    df = cast_types(df)

    # auditoria na silver
    df["fonte_tabela_bronze"] = bronze_table
    df["ingested_at"] = pd.NA  # deixa o DEFAULT do banco preencher

    # remove linhas totalmente vazias
    df = df.dropna(how="all") 

    return df


def main() -> None:
    load_env()
    engine = get_engine()

    silver_schema = os.getenv("SILVER_SCHEMA", "_silver-transacional").strip()
    silver_table = os.getenv("SILVER_TABLE_664", "novaxs_664").strip()
    write_mode = os.getenv("WRITE_MODE", "append").strip().lower()

    ensure_silver_table(engine, silver_schema, silver_table)

    bronze_tables = list_bronze_tables_664(engine)
    if not bronze_tables:
        logging.warning("Nenhuma tabela _664 encontrada no schema _bronze.")
        return

    if write_mode == "overwrite":
        logging.info("WRITE_MODE=overwrite -> DROP+CREATE %s.%s", silver_schema, silver_table)
        with engine.begin() as conn:
            conn.execute(text(f'DROP TABLE IF EXISTS "{silver_schema}"."{silver_table}";'))
        ensure_silver_table(engine, silver_schema, silver_table)

    logging.info("Encontradas %d tabelas _664 para consolidar.", len(bronze_tables))

    for t in bronze_tables:
        logging.info("[START] Processando bronze=%s", t)
        try:
            df = process_one_bronze_table(engine, t)
            if df.empty:
                logging.warning("[SKIP] bronze=%s sem linhas úteis", t)
                continue

            logging.info("[WRITE] bronze=%s linhas=%d cols=%d", t, len(df), len(df.columns))
            write_to_silver_copy(engine, df, silver_schema, silver_table)
            logging.info("[DONE] bronze=%s OK", t)

        except Exception as e:
            logging.exception("[ERRO] Falha ao processar %s: %s", t, e)

    logging.info("Finalizado.")


if __name__ == "__main__":
    main()
