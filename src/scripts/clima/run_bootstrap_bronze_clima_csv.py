import os
import glob
import logging
import psycopg2
import pandas as pd
from datetime import datetime

from src.common.settings import settings 
from datetime import datetime, UTC

LOGGER = logging.getLogger("bootstrap_clima_csv")
LOGGER.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
LOGGER.addHandler(handler)

CSV_DIR = "/root/db_medallion/bronze/scraping"
TABLE = "_bronze.clima_scraping_raw"

EXPECTED_COLS = [
    "id", "data_scrap", "hora_scrap", "origem", "cidade", "total_dias", "bloco",
    "data_previsao", "tempmin", "tempmax", "sensacao_termica", "sensacao_sombra",
    "ind_max_uv", "vento", "probab_chuva", "relatorio"
]


def connect():
    return psycopg2.connect(
        host=settings.PG_HOST,
        port=settings.PG_PORT,
        user=settings.PG_USER,
        password=settings.PG_PASSWORD,
        dbname=settings.PG_DB,
    )


def load_file(conn, csv_path: str):
    batch = os.path.basename(csv_path)

    df = pd.read_csv(
        csv_path,
        sep=";",
        quotechar='"',
        dtype=str,          # lê tudo como texto primeiro; depois o Postgres converte onde possível
        keep_default_na=False,
        encoding="utf-8",
    )

    missing = [c for c in EXPECTED_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Arquivo {batch} sem colunas esperadas: {missing}")

    # Renomeia id -> source_id (vamos gravar em coluna source_id)
    df = df[EXPECTED_COLS].copy()
    df.rename(columns={"id": "source_id"}, inplace=True)

    # Adiciona metadados
    df["ingestion_type"] = "bootstrap_csv"
    df["ingestion_batch"] = batch
    df["ingested_at"] = datetime.now(UTC).isoformat()

    # Grava via INSERT em lote (simples e confiável)
    # Se preferir performance máxima, trocamos pra COPY com staging CSV.
    cols = list(df.columns)
    values_template = "(" + ",".join(["%s"] * len(cols)) + ")"

    sql = f"INSERT INTO {TABLE} ({','.join(cols)}) VALUES {values_template}"

    with conn.cursor() as cur:
        rows = df.values.tolist()
        cur.executemany(sql, rows)
    conn.commit()

    LOGGER.info(f"OK: {batch} -> {len(df)} linhas inseridas.")


def main():
    files = sorted(glob.glob(os.path.join(CSV_DIR, "*.csv")))
    if not files:
        LOGGER.warning(f"Nenhum CSV encontrado em {CSV_DIR}")
        return

    conn = connect()
    try:
        for f in files:
            LOGGER.info(f"Carregando: {f}")
            load_file(conn, f)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
