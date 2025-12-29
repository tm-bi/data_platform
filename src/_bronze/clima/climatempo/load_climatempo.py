import logging
from datetime import datetime, UTC
import psycopg2

from src.common.settings import settings


LOGGER = logging.getLogger(__name__)
TABLE = "_bronze.clima_scraping_raw"


def _connect():
    return psycopg2.connect(
        host=settings.PG_HOST,
        port=settings.PG_PORT,
        user=settings.PG_USER,
        password=settings.PG_PASSWORD,
        dbname=settings.PG_DB,
    )


def load_climatempo(rows: list[dict]) -> int:
    if not rows:
        LOGGER.warning("[Climatempo] Nenhuma linha para inserir.")
        return 0

    sql = f"""
        INSERT INTO {TABLE} (
            data_scrap, hora_scrap, origem, cidade, total_dias, bloco,
            data_previsao, tempmin, tempmax, sensacao_termica,
            sensacao_sombra, ind_max_uv, vento, probab_chuva, relatorio,
            ingestion_type, ingested_at
        )
        VALUES (
            %(data_scrap)s, %(hora_scrap)s, %(origem)s, %(cidade)s,
            %(total_dias)s, %(bloco)s, %(data_previsao)s,
            %(tempmin)s, %(tempmax)s, %(sensacao_termica)s,
            %(sensacao_sombra)s, %(ind_max_uv)s, %(vento)s,
            %(probab_chuva)s, %(relatorio)s,
            'scrape_run', %(ingested_at)s
        )
    """

    now = datetime.now(UTC)
    for r in rows:
        r["ingested_at"] = now

    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.executemany(sql, rows)
        conn.commit()
    finally:
        conn.close()

    LOGGER.info(f"[Climatempo] {len(rows)} linhas inseridas na Bronze.")
    return len(rows)
