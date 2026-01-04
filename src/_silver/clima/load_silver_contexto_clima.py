import logging

import psycopg2
from psycopg2.extras import execute_values

from src.common.settings import settings

LOGGER = logging.getLogger(__name__)

SILVER_TRANS = '"_silver-transacional".s_clima'
SILVER_CTX = '"_silver-contexto".fato_clima_contexto'


def _connect():
    return psycopg2.connect(
        host=settings.PG_HOST,
        port=settings.PG_PORT,
        user=settings.PG_USER,
        password=settings.PG_PASSWORD,
        dbname=settings.PG_DB,
    )


def load_silver_contexto_clima() -> int:
    sql_latest = f"""
        WITH ranked AS (
            SELECT
                id_silver,
                origem_dado,
                cidade,
                uf,
                dt_forecast,
                dt_hr_scraping,
                ingested_at,
                ttl_dias,
                bloco,
                temp_min_c,
                temp_max_c,
                sensacao_termica,
                sensacao_termica_sombra,
                uv_index_max,
                vento,
                probabilidade_chuva,
                relato,
                bronze_id,
                ingestion_type,
                ingestion_batch,
                ROW_NUMBER() OVER (
                    PARTITION BY origem_dado, cidade, uf, dt_forecast
                    ORDER BY dt_hr_scraping DESC, ingested_at DESC, id_silver DESC
                ) AS rn
            FROM {SILVER_TRANS}
        )
        SELECT
            id_silver,
            origem_dado,
            cidade,
            uf,
            dt_forecast,
            dt_hr_scraping,
            ingested_at,
            ttl_dias,
            bloco,
            temp_min_c,
            temp_max_c,
            sensacao_termica,
            sensacao_termica_sombra,
            uv_index_max,
            vento,
            probabilidade_chuva,
            relato,
            bronze_id,
            ingestion_type,
            ingestion_batch
        FROM ranked
        WHERE rn = 1
    """

    sql_upsert = f"""
        INSERT INTO {SILVER_CTX} (
            origem_dado, cidade, uf, dt_forecast,
            dt_hr_scraping, ingested_at,
            ttl_dias, bloco,
            temp_min_c, temp_max_c,
            sensacao_termica, sensacao_termica_sombra, uv_index_max,
            vento, probabilidade_chuva, relato,
            silver_id, bronze_id, ingestion_type, ingestion_batch
        )
        VALUES %s
        ON CONFLICT (origem_dado, cidade, uf, dt_forecast)
        DO UPDATE SET
            dt_hr_scraping = EXCLUDED.dt_hr_scraping,
            ingested_at = EXCLUDED.ingested_at,
            ttl_dias = EXCLUDED.ttl_dias,
            bloco = EXCLUDED.bloco,
            temp_min_c = EXCLUDED.temp_min_c,
            temp_max_c = EXCLUDED.temp_max_c,
            sensacao_termica = EXCLUDED.sensacao_termica,
            sensacao_termica_sombra = EXCLUDED.sensacao_termica_sombra,
            uv_index_max = EXCLUDED.uv_index_max,
            vento = EXCLUDED.vento,
            probabilidade_chuva = EXCLUDED.probabilidade_chuva,
            relato = EXCLUDED.relato,
            silver_id = EXCLUDED.silver_id,
            bronze_id = EXCLUDED.bronze_id,
            ingestion_type = EXCLUDED.ingestion_type,
            ingestion_batch = EXCLUDED.ingestion_batch
    """

    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(sql_latest)
            rows = cur.fetchall()

            if not rows:
                LOGGER.info("[Silver Contexto Clima] Nada para atualizar.")
                return 0

            payload = []
            for r in rows:
                (
                    id_silver, origem_dado, cidade, uf, dt_forecast,
                    dt_hr_scraping, ingested_at,
                    ttl_dias, bloco,
                    temp_min_c, temp_max_c,
                    sens_term, sens_sombra, uv,
                    vento, prob_chuva, relato,
                    bronze_id, ingestion_type, ingestion_batch
                ) = r

                payload.append(
                    (
                        origem_dado, cidade, uf, dt_forecast,
                        dt_hr_scraping, ingested_at,
                        ttl_dias, bloco,
                        temp_min_c, temp_max_c,
                        sens_term, sens_sombra, uv,
                        vento, prob_chuva, relato,
                        id_silver, bronze_id, ingestion_type, ingestion_batch
                    )
                )

            execute_values(cur, sql_upsert, payload, page_size=3000)

        conn.commit()
        LOGGER.info(f"[Silver Contexto Clima] Upsert concluído ({len(payload)} chaves).")
        return len(payload)

    except Exception:
        conn.rollback()
        LOGGER.exception("[Silver Contexto Clima] Falha (rollback executado).")
        raise
    finally:
        conn.close()
