import logging
from datetime import datetime, UTC

import psycopg2
from psycopg2.extras import execute_batch

from src.common.settings import settings

LOGGER = logging.getLogger(__name__)
TABLE = '_bronze.scraping_clima_raw'


def _connect():
    # Corrigido: nomes são minúsculos no Settings (pg_host, pg_port, ...)
    return psycopg2.connect(
        host=settings.PG_HOST,
        port=settings.PG_PORT,
        user=settings.PG_USER,
        password=settings.PG_PASSWORD,
        dbname=settings.PG_DB,
    )


def load_accuweather(rows: list[dict]) -> int:
    if not rows:
        LOGGER.warning("[AccuWeather] Nenhuma linha para inserir.")
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
            %(ingestion_type)s, %(ingested_at)s
        )
        -- Se você tiver uma UNIQUE constraint para evitar duplicidade,
        -- dá para habilitar isso:
        -- ON CONFLICT DO NOTHING
    """

    now = datetime.now(UTC)
    for r in rows:
        r["ingested_at"] = now
        r["ingestion_type"] = "scrape_run"

    conn = _connect()
    try:
        with conn.cursor() as cur:
            # Mais eficiente e confiável que executemany em grandes lotes
            execute_batch(cur, sql, rows, page_size=200)
        conn.commit()
    except Exception:
        conn.rollback()
        LOGGER.exception("[AccuWeather] Falha ao inserir na Bronze (rollback executado).")
        raise
    finally:
        conn.close()

    LOGGER.info(f"[AccuWeather] {len(rows)} linhas inseridas na Bronze.")
    return len(rows)
