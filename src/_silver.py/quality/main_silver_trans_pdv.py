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

HEADER_PREFIX = "idVenda;idContasAReceber;tipoProduto;"
STOP_MARKERS: tuple[str, ...] = ()

SILVER_COLUMNS = [
    "id_venda",
    "id_contas_receber",
    "tipo_produto",
    "produto",
    "dt_lancamento",
    "dt_recebimento",
    "dt_venda_pdv",
    "funcionario",
    "estacao",
    "pos",
    "id_pdv_compra_cupom",
    "num_mesa",
    "status_cupom",
    "status_compra",
    "id_produto_servico",
    "ext_ipi",
    "cliente",
    "categoria",
    "num_titulo",
    "ativo",
    "qtd",
    "vlr_ttl_item",
    "vlr_unit_item",
    "desconto"
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
    url_object = URL.create(
        drivername="postgresql+psycopg2",
        username=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        host=os.getenv("PG_HOST"),
        port=int(os.getenv("PG_PORT", "5432")),
        database=os.getenv("PG_DB"),
    )
    return create_engine(url_object, pool_pre_ping=True)


# ------------------------------------------------------------------------------
def find_header_line_no(engine: Engine, bronze_table: str) -> int:
    sql = text(
        f"""
        SELECT line_no
        FROM _bronze."{bronze_table}"
        WHERE raw_line LIKE :pfx
        ORDER BY line_no
        LIMIT 1
        """
    )
    with engine.connect() as conn:
        row = conn.execute(sql, {"pfx": HEADER_PREFIX + "%"}).first()
        if not row:
            raise RuntimeError("Cabeçalho não encontrado no RAW")
        return int(row[0])


# ------------------------------------------------------------------------------
def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    rename_map = {
        "idVenda": "id_venda",
        "idContasAReceber": "id_contas_receber",
        "tipoProduto": "tipo_produto",
        "dataLancamento": "dt_lancamento",
        "dataUltimoRecebimento": "dt_recebimento",
        "dataVendaPdv": "dt_venda_pdv",
        "idPdvCompraCupom": "id_pdv_compra_cupom",
        "numeroMesa": "num_mesa",
        "statusCupom": "status_cupom",
        "statusCompra": "status_compra",
        "idProdutoServico": "id_produto_servico",
        "extIpi": "ext_ipi",
        "Cliente": "cliente",
        "numeroTitulo": "num_titulo",
        "quantidade": "qtd",
        "valor total do item": "vlr_ttl_item" ,
        "valor unitario do item": "vlr_unit_item"
    }
    df = df.rename(columns=rename_map)
    return df[[c for c in df.columns if not str(c).lower().startswith("unnamed")]]

# ------------------------------------------------------------------------------
def cast_types(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    for c in df.columns:
        df[c] = df[c].astype("string").str.strip()

    iso_fmt = "%Y-%m-%d %H:%M:%S.%f"

    for col in ("dt_lancamento", "dt_recebimento", "dt_venda_pdv"):
        if col in df.columns:
            s = df[col].replace({"": pd.NA, "NULL": pd.NA})
            dt = pd.to_datetime(s, format=iso_fmt, errors="coerce")
            m = dt.isna() & s.notna()
            if m.any():
                dt.loc[m] = pd.to_datetime(s[m], errors="coerce")
            df[col] = dt.dt.date

    if "qtd" in df.columns:
        df["qtd"] = pd.to_numeric(df["qtd"], errors="coerce").astype("Int64")

    for col in (
        "vlr_ttl_item"
        "vlr_unit_item",
        "desconto",
    ):
        if col in df.columns:
            x = (
                df[col]
                .str.replace(".", "", regex=False)
                .str.replace(",", ".", regex=False)
            )
            df[col] = pd.to_numeric(x, errors="coerce").round(2)

    return df


# ------------------------------------------------------------------------------
def ensure_silver_table(engine: Engine, schema: str, table: str) -> None:
    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))
        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS "{schema}"."{table}" (                  
                    id_venda TEXT,
                    id_contas_receber TEXT,
                    tipo_produto TEXT,
                    produto TEXT,
                    dt_lancamento DATE,
                    dt_recebimento DATE,
                    dt_venda_pdv DATE,
                    funcionario TEXT,
                    estacao TEXT,
                    pos TEXT,
                    id_pdv_compra_cupom TEXT,
                    num_mesa TEXT,
                    status_cupom TEXT,
                    status_compra TEXT,
                    id_produto_servico TEXT,
                    ext_ipi TEXT,
                    cliente TEXT,
                    categoria TEXT,
                    num_titulo TEXT,
                    ativo TEXT,
                    qtd BIGINT,
                    vlr_ttl_item NUMERIC(18,2),
                    vlr_unit_item NUMERIC(18,2),
                    desconto NUMERIC(18,2),
                    ingested_at TIMESTAMPTZ DEFAULT now(),
                    fonte_tabela_bronze TEXT
                )
                """
            )
        )


# ------------------------------------------------------------------------------
def write_to_silver_copy(engine: Engine, df: pd.DataFrame, schema: str, table: str) -> None:
    df = df.dropna(how="all")
    if df.empty:
        return

    if "ingested_at" in df.columns:
        df = df.drop(columns=["ingested_at"])

    buf = StringIO()
    writer = csv.writer(buf, delimiter="\t", lineterminator="\n")
    for row in df.itertuples(index=False, name=None):
        writer.writerow(["" if pd.isna(v) else v for v in row])

    buf.seek(0)
    raw = engine.raw_connection()
    try:
        with raw.cursor() as cur:
            cols = ", ".join(f'"{c}"' for c in df.columns)
            cur.copy_expert(
                f'''COPY "{schema}"."{table}" ({cols})
                    FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t', NULL '')''',
                buf,
            )
        raw.commit()
    finally:
        raw.close()


# ------------------------------------------------------------------------------
def process_one_bronze_table(engine: Engine, bronze_table: str, schema: str, table: str) -> None:
    header_line_no = find_header_line_no(engine, bronze_table)

    raw = engine.raw_connection()
    try:
        with raw.cursor(name="cur_stream") as cur:
            cur.itersize = 50_000
            cur.execute(
                f'''
                SELECT raw_line
                FROM _bronze."{bronze_table}"
                WHERE line_no >= %s
                ORDER BY line_no
                ''',
                (header_line_no,),
            )

            buf = StringIO()
            header_line: str | None = None

            def flush_buffer() -> None:
                nonlocal buf, header_line

                # se só tem header ou está vazio, não tenta parsear
                if buf.tell() == 0:
                    return

                buf.seek(0)  # ✅ CRÍTICO: volta o cursor pro início

                try:
                    for chunk in pd.read_csv(
                        buf,
                        sep=";",
                        dtype="string",
                        chunksize=200_000,
                        engine="python",
                    ):
                        chunk = cast_types(normalize_columns(chunk))
                        chunk["fonte_tabela_bronze"] = bronze_table
                        write_to_silver_copy(engine, chunk, schema, table)
                except pd.errors.EmptyDataError:
                    # buffer sem dados parseáveis (ex.: só header)
                    return
                finally:
                    # ✅ recria buffer e reescreve header para o próximo bloco
                    new_buf = StringIO()
                    if header_line is not None:
                        new_buf.write(header_line)
                    buf = new_buf

            for (line,) in cur:
                if not line:
                    continue

                # captura header uma vez (primeira linha retornada deve ser o header)
                if header_line is None:
                    header_line = line.rstrip("\n") + "\n"
                    buf.write(header_line)
                    continue

                s = line.strip()
                if not s:
                    continue

                if STOP_MARKERS and any(s.startswith(m) for m in STOP_MARKERS):
                    break

                buf.write(line.rstrip("\n") + "\n")

                # flush por tamanho (8MB)
                if buf.tell() > 8 * 1024 * 1024:
                    flush_buffer()

            # flush final
            flush_buffer()

    finally:
        raw.close()


# ------------------------------------------------------------------------------
def main() -> None:
    load_env()
    engine = get_engine()

    silver_schema = os.getenv("SILVER_SCHEMA", "_silver-transacional")
    bronze_table = os.getenv("SRC_TABLE_PDV")
    silver_table = os.getenv("SILVER_TABLE_PDV")
    write_mode = os.getenv("WRITE_MODE", "append")

    if write_mode == "overwrite":
        with engine.begin() as conn:
            conn.execute(text(f'DROP TABLE IF EXISTS "{silver_schema}"."{silver_table}"'))

    ensure_silver_table(engine, silver_schema, silver_table)

    logging.info("[START] bronze=%s", bronze_table)
    process_one_bronze_table(engine, bronze_table, silver_schema, silver_table)
    logging.info("[DONE] bronze=%s", bronze_table)


if __name__ == "__main__":
    main()
