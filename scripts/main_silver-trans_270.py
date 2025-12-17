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

HEADER_PREFIX = "Conta;Autorização;Cliente;"

STOP_MARKERS = (
    '"";"";',                   # linha vazia com "";""
    '"Total Geral"',             # total geral
    '"Total";',                  # total do bloco (às vezes aparece)
    "Produtos/Combos vendidos",  # título repetido
    "Produto;Qtd;Total",         # início da segunda tabela
)

SILVER_COLUMNS = [
    "id_venda",
    "autorizacao",
    "cliente",
    "endereco",
    "cidade",
    "celular",
    "telefone",
    "email",
    "teste",
    "agencia",
    "data",
    "dt_venda",
    "hr_venda",
    "qtd",
    "produto",
    "vlr_unid",
    "vlr_total",
    "status",
    "entregue",
    "forma_pagamento",
    "usuario",
    "dt_utilizacao",
    "hr_utilizacao",
    "nsu",
    "qtd_parcelas",
    "prazo_recebimento",
    "dt_encerramento",
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


def list_bronze_tables_270(engine: Engine) -> list[str]:
    sql = text(
        """
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = '_bronze'
          AND tablename LIKE '%\\_270' ESCAPE '\\'
        ORDER BY tablename;
        """
    )
    with engine.connect() as conn:
        return [r[0] for r in conn.execute(sql).fetchall()]


def derive_ano_mes_from_table(table_name: str) -> str:
    m = re.search(r"t_(\d{6})_270$", table_name)
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


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    rename_map = {
        "Conta": "id_venda",
        "Autorização": "autorizacao",
        "Cliente": "cliente",
        "Endereço": "endereco",
        "Cidade": "cidade",
        "Celular": "celular",
        "Telefone": "telefone",
        "Email": "email",
        "Agencia": "agencia",           # às vezes vem sem acento
        "Agência": "agencia",           # se vier com acento
        "Data": "data",
        "Criado": "criado",
        "Qtd": "qtd",
        "Produto": "produto",
        "Un.": "vlr_unid",
        "Total": "vlr_total",
        "Status": "status",
        "Entregue": "entregue",
        "Forma de Pagamento": "forma_pagamento",
        "Usuário": "usuario",
        "Data Utilização": "dt_utilizacao",
        "Hora Utilização": "hr_utilizacao",
        "NSU": "nsu",
        "Quantidade de Parcelas": "qtd_parcelas",
        "Prazo estimado do recebimento": "prazo_recebimento",
        "Data Encerramento": "dt_encerramento",
    }

    df = df.rename(columns=rename_map)
    df = df.loc[:, [c for c in df.columns if not str(c).lower().startswith("unnamed")]]
    return df


def to_decimal_br(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.strip()
    s = s.replace({"": None, "nan": None, "None": None})

    # remove separador de milhar "." e troca decimal "," por "."
    s = s.str.replace(".", "", regex=False).str.replace(",", ".", regex=False)
    return pd.to_numeric(s, errors="coerce")


def cast_types(df: pd.DataFrame) -> pd.DataFrame:
    # tudo como string primeiro
    for c in df.columns:
        df[c] = df[c].astype("string")

    # datas base
    if "data" in df.columns:
        df["data"] = pd.to_datetime(df["data"], dayfirst=True, errors="coerce").dt.date

    # Criado -> dt_venda (date) e hr_venda (time)
    if "criado" in df.columns:
        ts = pd.to_datetime(df["criado"], dayfirst=True, errors="coerce")
        df["dt_venda"] = ts.dt.date
        df["hr_venda"] = ts.dt.time
        df = df.drop(columns=["criado"])

    # dt_utilizacao (DATE)
    if "dt_utilizacao" in df.columns:
        df["dt_utilizacao"] = pd.to_datetime(df["dt_utilizacao"], dayfirst=True, errors="coerce").dt.date

    # hr_utilizacao (TIME) - pode não existir nos meses antigos
    if "hr_utilizacao" in df.columns:
        # Alguns arquivos podem ter hora vazia ou formato estranho; coerce e pega .time
        df["hr_utilizacao"] = pd.to_datetime(df["hr_utilizacao"], errors="coerce").dt.time

    # dt_encerramento
    if "dt_encerramento" in df.columns:
        df["dt_encerramento"] = pd.to_datetime(df["dt_encerramento"], dayfirst=True, errors="coerce").dt.date

    # inteiros
    if "qtd" in df.columns:
        df["qtd"] = pd.to_numeric(df["qtd"], errors="coerce").astype("Int64")

    if "qtd_parcelas" in df.columns:
        df["qtd_parcelas"] = pd.to_numeric(df["qtd_parcelas"], errors="coerce").astype("Int64")

    # decimais
    if "vlr_total" in df.columns:
        df["vlr_total"] = to_decimal_br(df["vlr_total"]).round(2)

    if "vlr_unid" in df.columns:
        df["vlr_unid"] = to_decimal_br(df["vlr_unid"]).round(2)

    # normalização email
    if "email" in df.columns:
        df["email"] = df["email"].str.strip().str.lower()
        df["teste"] = df["email"].eq("carlos.nishiyama@thermasdamata.com.br").map(
            {True: "SIM", False: "NÃO"}
        )
    else:
        df["teste"] = "NÃO"
        
    return df


def align_df_to_silver(df: pd.DataFrame) -> pd.DataFrame:
    # cria colunas faltantes com NA (ex.: hr_utilizacao antes de 2025)
    for col in SILVER_COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA
    # garante ordem e subset
    return df[SILVER_COLUMNS]


def ensure_silver_table(engine: Engine, schema: str, table: str) -> None:
    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}";'))

        conn.execute(
            text(
                f'''
                CREATE TABLE IF NOT EXISTS "{schema}"."{table}" (
                    id_venda TEXT,
                    autorizacao TEXT,
                    cliente TEXT,
                    endereco TEXT,
                    cidade TEXT,
                    celular TEXT,
                    telefone TEXT,
                    email TEXT,
                    teste TEXT,
                    agencia TEXT,
                    data DATE,
                    dt_venda DATE,
                    hr_venda TIME,
                    qtd BIGINT,
                    produto TEXT,
                    vlr_unid NUMERIC(18,2),
                    vlr_total NUMERIC(18,2),
                    status TEXT,
                    entregue TEXT,
                    forma_pagamento TEXT,
                    usuario TEXT,
                    dt_utilizacao DATE,
                    hr_utilizacao TIME,
                    nsu TEXT,
                    qtd_parcelas BIGINT,
                    prazo_recebimento TEXT,
                    dt_encerramento DATE,
                    ano_mes TEXT,
                    fonte_tabela_bronze TEXT
                );
                '''
            )
        )


def truncate_silver_table(engine: Engine, schema: str, table: str) -> None:
    with engine.begin() as conn:
        conn.execute(text(f'TRUNCATE TABLE "{schema}"."{table}";'))


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
    silver_table = os.getenv("SILVER_TABLE_270", "novaxs_270").strip()
    write_mode = os.getenv("WRITE_MODE", "append").strip().lower()

    ensure_silver_table(engine, silver_schema, silver_table)

    if write_mode == "overwrite":
        logging.info("WRITE_MODE=overwrite -> DROP+CREATE %s.%s", silver_schema, silver_table)
        with engine.begin() as conn:
            conn.execute(text(f'DROP TABLE IF EXISTS "{silver_schema}"."{silver_table}";'))
        ensure_silver_table(engine, silver_schema, silver_table)

        bronze_tables = list_bronze_tables_270(engine)
        if not bronze_tables:
            logging.warning("Nenhuma tabela _270 encontrada no schema _bronze.")
            return

    logging.info("Encontradas %d tabelas _270 para consolidar.", len(bronze_tables))

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
