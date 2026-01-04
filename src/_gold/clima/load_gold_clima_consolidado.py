from __future__ import annotations

import logging
import psycopg2

from src.common.settings import settings

LOGGER = logging.getLogger(__name__)

SILVER_CTX = '"_silver-contexto".fato_clima_contexto'
GOLD = '"_gold".fato_clima'

PREFERRED = "AccuWeather"  # ou 'Climatempo'


def _connect():
    return psycopg2.connect(
        host=settings.pg_host,
        port=settings.pg_port,
        user=settings.pg_user,
        password=settings.pg_password,
        dbname=settings.pg_db,
    )


def load_gold_clima_consolidado() -> None:
    """
    Consolida fato_clima_contexto (último por fonte) em _gold.fato_clima:
      - 1 linha por (cidade, uf, dt_forecast)
      - preferindo AccuWeather com fallback Climatempo (campo a campo)
      - last_updated_at = maior dt_hr_scraping entre fontes
      - ingested_at = preferir a fonte PREFERRED quando existir
    """

    sql = f"""
    WITH base AS (
      SELECT
        CASE
          WHEN lower(origem_dado) LIKE 'accu%%' THEN 'AccuWeather'
          WHEN lower(origem_dado) LIKE 'clima%%' THEN 'Climatempo'
          ELSE origem_dado
        END AS origem_norm,

        TRIM(REPLACE(REPLACE(cidade, ', SP', ''), ',SP', '')) AS cidade_norm,

        -- ⚠️ padroniza UF para evitar NULL em chave
        COALESCE(UPPER(NULLIF(TRIM(uf), '')), 'SP') AS uf_norm,

        dt_forecast,
        dt_hr_scraping,
        ingested_at,

        temp_min_c,
        temp_max_c,
        sensacao_termica,
        sensacao_termica_sombra,
        uv_index_max,
        vento,
        probabilidade_chuva,
        relato
      FROM {SILVER_CTX}
    ),
    pivot AS (
      SELECT
        cidade_norm AS cidade,
        uf_norm AS uf,
        dt_forecast,

        MAX(CASE WHEN origem_norm = 'AccuWeather' THEN dt_hr_scraping END) AS acc_dt_hr_scraping,
        MAX(CASE WHEN origem_norm = 'Climatempo' THEN dt_hr_scraping END) AS cli_dt_hr_scraping,

        MAX(CASE WHEN origem_norm = 'AccuWeather' THEN ingested_at END) AS acc_ingested_at,
        MAX(CASE WHEN origem_norm = 'Climatempo' THEN ingested_at END) AS cli_ingested_at,

        MAX(CASE WHEN origem_norm = 'AccuWeather' THEN temp_min_c END) AS acc_temp_min_c,
        MAX(CASE WHEN origem_norm = 'AccuWeather' THEN temp_max_c END) AS acc_temp_max_c,
        MAX(CASE WHEN origem_norm = 'AccuWeather' THEN sensacao_termica END) AS acc_sensacao_termica,
        MAX(CASE WHEN origem_norm = 'AccuWeather' THEN sensacao_termica_sombra END) AS acc_sensacao_termica_sombra,
        MAX(CASE WHEN origem_norm = 'AccuWeather' THEN uv_index_max END) AS acc_uv_index_max,
        MAX(CASE WHEN origem_norm = 'AccuWeather' THEN vento END) AS acc_vento,
        MAX(CASE WHEN origem_norm = 'AccuWeather' THEN probabilidade_chuva END) AS acc_probabilidade_chuva,
        MAX(CASE WHEN origem_norm = 'AccuWeather' THEN relato END) AS acc_relato,

        MAX(CASE WHEN origem_norm = 'Climatempo' THEN temp_min_c END) AS cli_temp_min_c,
        MAX(CASE WHEN origem_norm = 'Climatempo' THEN temp_max_c END) AS cli_temp_max_c,
        MAX(CASE WHEN origem_norm = 'Climatempo' THEN sensacao_termica END) AS cli_sensacao_termica,
        MAX(CASE WHEN origem_norm = 'Climatempo' THEN sensacao_termica_sombra END) AS cli_sensacao_termica_sombra,
        MAX(CASE WHEN origem_norm = 'Climatempo' THEN uv_index_max END) AS cli_uv_index_max,
        MAX(CASE WHEN origem_norm = 'Climatempo' THEN vento END) AS cli_vento,
        MAX(CASE WHEN origem_norm = 'Climatempo' THEN probabilidade_chuva END) AS cli_probabilidade_chuva,
        MAX(CASE WHEN origem_norm = 'Climatempo' THEN relato END) AS cli_relato

      FROM base
      GROUP BY 1,2,3
    ),
    cons AS (
      SELECT
        cidade,
        uf,
        dt_forecast,

        COALESCE(acc_temp_min_c, cli_temp_min_c) AS temp_min_c,
        COALESCE(acc_temp_max_c, cli_temp_max_c) AS temp_max_c,
        COALESCE(NULLIF(acc_sensacao_termica,''), NULLIF(cli_sensacao_termica,'')) AS sensacao_termica,
        COALESCE(NULLIF(acc_sensacao_termica_sombra,''), NULLIF(cli_sensacao_termica_sombra,'')) AS sensacao_termica_sombra,
        COALESCE(NULLIF(acc_uv_index_max,''), NULLIF(cli_uv_index_max,'')) AS uv_index_max,
        COALESCE(NULLIF(acc_vento,''), NULLIF(cli_vento,'')) AS vento,
        COALESCE(NULLIF(acc_probabilidade_chuva,''), NULLIF(cli_probabilidade_chuva,'')) AS probabilidade_chuva,
        COALESCE(NULLIF(acc_relato,''), NULLIF(cli_relato,'')) AS relato,

        GREATEST(acc_dt_hr_scraping, cli_dt_hr_scraping) AS last_updated_at,

        CASE
          WHEN '{PREFERRED}' = 'AccuWeather' THEN COALESCE(acc_ingested_at, cli_ingested_at, now())
          WHEN '{PREFERRED}' = 'Climatempo'  THEN COALESCE(cli_ingested_at, acc_ingested_at, now())
          ELSE COALESCE(acc_ingested_at, cli_ingested_at, now())
        END AS ingested_at
      FROM pivot
    )
    INSERT INTO {GOLD} (
      cidade, uf, dt_forecast,
      temp_min_c, temp_max_c,
      sensacao_termica, sensacao_termica_sombra,
      uv_index_max, vento, probabilidade_chuva, relato,
      last_updated_at, ingested_at
    )
    SELECT
      cidade, uf, dt_forecast,
      temp_min_c, temp_max_c,
      sensacao_termica, sensacao_termica_sombra,
      uv_index_max, vento, probabilidade_chuva, relato,
      COALESCE(last_updated_at, now()),
      ingested_at
    FROM cons
    ON CONFLICT (cidade, uf, dt_forecast)
    DO UPDATE SET
      temp_min_c = EXCLUDED.temp_min_c,
      temp_max_c = EXCLUDED.temp_max_c,
      sensacao_termica = EXCLUDED.sensacao_termica,
      sensacao_termica_sombra = EXCLUDED.sensacao_termica_sombra,
      uv_index_max = EXCLUDED.uv_index_max,
      vento = EXCLUDED.vento,
      probabilidade_chuva = EXCLUDED.probabilidade_chuva,
      relato = EXCLUDED.relato,
      last_updated_at = EXCLUDED.last_updated_at,
      ingested_at = EXCLUDED.ingested_at
    ;
    """

    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
        LOGGER.info("[Gold Clima] Upsert executado com sucesso em _gold.fato_clima.")
    except Exception:
        conn.rollback()
        LOGGER.exception("[Gold Clima] Falha (rollback executado).")
        raise
    finally:
        conn.close()
