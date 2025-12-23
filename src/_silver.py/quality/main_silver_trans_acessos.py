from __future__ import annotations

import csv
import logging
import os
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

# Cabeçalho esperado no RAW
HEADER_PREFIX = "idEmpresaRelacionamento;Data/Hora Entrada;Data/Hora Saída;"

# Se você tiver rodapé/total nessa planilha, inclua aqui (como no _418).
# Por enquanto deixo vazio para não cortar nada indevidamente.
STOP_MARKERS: tuple[str, ...] = ()

SILVER_COLUMNS = [
    "idEmpresaRelacionamento",
    "dt_entrada",
    "hr_entrada",
    "dt_saida",
    "hr_saida",
    "associado_ingresso",
    "categoria",
    "tipo_ingresso",
    "num_ingresso",
    "terminal_entrada",
    "terminal_saida",
    "tipo_acesso",
    "ingested_at",
    "fonte_tabela_bronze",
]

# ------------------------------------------------------------------------------
def load_env(env_path: Optional[Path] = None) -> None:
    if env_path is None:
        env_path = Path(__file__).resolve().parents[2] / "config" / ".env"

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

        if STOP_MARKERS and any(s.startswith(m) for m in STOP_MARKERS):
            break

        selected.append(line)

    if len(selected) <= 1:
        raise ValueError("Planilha principal vazia após o corte (sem dados).")

    return "\n".join(selected) + "\n"


# ------------------------------------------------------------------------------
# Normalização e transformações (Pontos 1..4)
# ------------------------------------------------------------------------------
def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ponto 4: renomeia colunas para padrão silver.
    """
    rename_map = {
        "Sócio / Ingresso": "associado_ingresso",
        "Numero do ingresso": "num_ingresso",
        "Terminal Entrada": "terminal_entrada",
        "Terminal Saída": "terminal_saida",
        "Tipo de Acesso": "tipo_acesso",
    }

    df = df.rename(columns=rename_map)
    df = df.loc[:, [c for c in df.columns if not str(c).lower().startswith("unnamed")]]
    return df


def _normalize_day_use(series: pd.Series) -> pd.Series:
    """
    Normaliza qualquer variação de 'day use' -> 'DAY-USE' (case-insensitive, aceita espaços).
    """
    s = series.astype("string").str.strip()
    s = s.replace({"": pd.NA, "nan": pd.NA, "None": pd.NA, "NULL": pd.NA, "null": pd.NA})
    return s.str.replace(r"\bday\s*use\b", "DAY-USE", regex=True, case=False)


def cast_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica Pontos 1..3:
    - Ponto 1: divide Data/Hora Entrada e Saída em dt_* (DATE) e hr_* (TIME)
    - Ponto 2: Categoria / Tipo de Ingresso: trim + normaliza day use + split '-' => categoria, tipo_ingresso
    - Ponto 3: associado_ingresso: substitui DAY USE -> DAY-USE (mesma normalização)
    """
    df = df.copy()

    # tudo como string primeiro (igual referência)
    for c in df.columns:
        df[c] = df[c].astype("string")

    # ------------------------
    # Ponto 1: split datetime
    # ------------------------
    def split_datetime(src_col: str, dt_col: str, hr_col: str) -> None:
        if src_col not in df.columns:
            df[dt_col] = pd.NA
            df[hr_col] = pd.NA
            return

        s = df[src_col].astype("string").str.strip()
        s = s.replace({"": pd.NA, "nan": pd.NA, "None": pd.NA, "NULL": pd.NA, "null": pd.NA})

        # dayfirst=True pra padrão BR
        ts = pd.to_datetime(s, errors="coerce", dayfirst=True)

        df[dt_col] = ts.dt.date
        # TIME puro (sem timezone)
        df[hr_col] = ts.dt.time

    split_datetime("Data/Hora Entrada", "dt_entrada", "hr_entrada")
    split_datetime("Data/Hora Saída", "dt_saida", "hr_saida")

    # ------------------------
    # Ponto 2: Categoria/Tipo
    # ------------------------
    cat_col = "Categoria / Tipo de Ingresso"
    if cat_col in df.columns:
        s = _normalize_day_use(df[cat_col])  # também remove espaços e normaliza day use
        parts = s.str.split("-", n=1, expand=True)

        df["categoria"] = parts[0].astype("string").str.strip()

        if parts.shape[1] > 1:
            df["tipo_ingresso"] = parts[1].astype("string").str.strip()
        else:
            df["tipo_ingresso"] = pd.NA
    else:
        df["categoria"] = pd.NA
        df["tipo_ingresso"] = pd.NA

    # ------------------------
    # Ponto 3: associado_ingresso
    # ------------------------
    if "associado_ingresso" in df.columns:
        df["associado_ingresso"] = _normalize_day_use(df["associado_ingresso"])

    return df


def align_df_to_silver(df: pd.DataFrame) -> pd.DataFrame:
    for col in SILVER_COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA
    return df[SILVER_COLUMNS]


# ------------------------------------------------------------------------------
# Silver DDL + Write
# ------------------------------------------------------------------------------
def ensure_silver_table(engine: Engine, schema: str, table: str) -> None:
    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}";'))

        conn.execute(
            text(
                f'''
                CREATE TABLE IF NOT EXISTS "{schema}"."{table}" (
                    "idEmpresaRelacionamento" TEXT,
                    dt_entrada DATE,
                    hr_entrada TIME,
                    dt_saida DATE,
                    hr_saida TIME,
                    associado_ingresso TEXT,
                    categoria TEXT,
                    tipo_ingresso TEXT,
                    num_ingresso TEXT,
                    terminal_entrada TEXT,
                    terminal_saida TEXT,
                    tipo_acesso TEXT,
                    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    fonte_tabela_bronze TEXT
                );
                '''
            )
        )

        conn.execute(text(f'ALTER TABLE "{schema}"."{table}" ADD COLUMN IF NOT EXISTS ingested_at TIMESTAMPTZ;'))
        conn.execute(text(f'ALTER TABLE "{schema}"."{table}" ALTER COLUMN ingested_at SET DEFAULT now();'))


def write_to_silver_copy(engine: Engine, df: pd.DataFrame, schema: str, table: str) -> None:
    df2 = df.dropna(how="all").copy()
    df2 = align_df_to_silver(df2)

    # não manda ingested_at no COPY -> banco aplica DEFAULT now()
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


# ------------------------------------------------------------------------------
# Bronze -> Silver
# ------------------------------------------------------------------------------
def process_one_bronze_table(engine: Engine, bronze_table: str) -> pd.DataFrame:
    sql = text(f'SELECT line_no, raw_line FROM _bronze."{bronze_table}" ORDER BY line_no;')
    df_lines = pd.read_sql(sql, engine)

    csv_text = extract_main_csv_from_raw_lines(df_lines)

    df = pd.read_csv(
        StringIO(csv_text),
        sep=";",
        dtype="string",
        quotechar='"',
        engine="python",
    )

    df = normalize_columns(df)
    df = cast_types(df)

    # auditoria na silver
    df["fonte_tabela_bronze"] = bronze_table

    # remove linhas totalmente vazias
    df = df.dropna(how="all")

    return df


def main() -> None:
    load_env()
    engine = get_engine()

    silver_schema = os.getenv("SILVER_SCHEMA", "_silver-transacional").strip()
    bronze_table = os.getenv("SRC_TABLE_ACESSO", "quality_acesso_raw").strip()
    silver_table = os.getenv("SILVER_TABLE_ACESSO", "s_quality_acesso").strip()
    write_mode = os.getenv("WRITE_MODE", "append").strip().lower()

    if write_mode == "overwrite":
        logging.info("WRITE_MODE=overwrite -> DROP+CREATE %s.%s", silver_schema, silver_table)
        with engine.begin() as conn:
            conn.execute(text(f'DROP TABLE IF EXISTS "{silver_schema}"."{silver_table}";'))

    ensure_silver_table(engine, silver_schema, silver_table)

    logging.info("[START] Processando bronze=%s", bronze_table)
    try:
        df = process_one_bronze_table(engine, bronze_table)

        if df.empty:
            logging.warning("[SKIP] bronze=%s sem linhas úteis", bronze_table)
            return

        logging.info("[WRITE] bronze=%s linhas=%d cols=%d", bronze_table, len(df), len(df.columns))
        write_to_silver_copy(engine, df, silver_schema, silver_table)
        logging.info("[DONE] bronze=%s OK", bronze_table)

    except Exception as e:
        logging.exception("[ERRO] Falha ao processar %s: %s", bronze_table, e)

    logging.info("Finalizado.")


if __name__ == "__main__":
    main()
