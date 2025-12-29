import logging
import psycopg2

from src.common.settings import settings

LOGGER = logging.getLogger(__name__)

SILVER_CTX = '"_silver-contexto".clima_contexto'
GOLD_TABLE = '"_gold".clima_unificado'


def _connect():
    return psycopg2.connect(
        host=settings.PG_HOST,
        port=settings.PG_PORT,
        user=settings.PG_USER,
        password=settings.PG_PASSWORD,
        dbname=settings.PG_DB,
    )


def load_gold_clima_unificado() -> None:
    """
    Unifica o 'último' (silver_contexto) de AccuWeather + Climatempo
    por (cidade_normalizada, forecast_date), mantendo as fontes lado a lado.
    """

    sql = f"""
    WITH base AS (
        SELECT
            -- normaliza origem (resolve Accuweather vs AccuWeather)
            CASE
              WHEN lower(origem_dado) LIKE 'accu%%' THEN 'AccuWeather'
              WHEN lower(origem_dado) LIKE 'clima%%' THEN 'Climatempo'
              ELSE origem_dado
            END AS origem_norm,

            -- normaliza cidade (remove sufixos tipo ", SP")
            TRIM(
              REPLACE(REPLACE(cidade, ', SP', ''), ',SP', '')
            ) AS cidade_norm,

            forecast_date,
            scraped_at,

            temp_min_c,
            temp_max_c,
            probabilidade_chuva,
            vento,
            relato
        FROM {SILVER_CTX}
    ),
    agg AS (
        SELECT
            cidade_norm AS cidade,
            forecast_date,

            -- timestamps por fonte
            MAX(CASE WHEN origem_norm = 'AccuWeather' THEN scraped_at END) AS acc_scraped_at,
            MAX(CASE WHEN origem_norm = 'Climatempo' THEN scraped_at END) AS cli_scraped_at,

            -- last_updated_at geral
            GREATEST(
                MAX(CASE WHEN origem_norm = 'AccuWeather' THEN scraped_at END),
                MAX(CASE WHEN origem_norm = 'Climatempo' THEN scraped_at END)
            ) AS last_updated_at,

            -- AccuWeather valores
            MAX(CASE WHEN origem_norm = 'AccuWeather' THEN temp_min_c END) AS acc_temp_min_c,
            MAX(CASE WHEN origem_norm = 'AccuWeather' THEN temp_max_c END) AS acc_temp_max_c,
            MAX(CASE WHEN origem_norm = 'AccuWeather' THEN probabilidade_chuva END) AS acc_prob_chuva,
            MAX(CASE WHEN origem_norm = 'AccuWeather' THEN vento END) AS acc_vento,
            MAX(CASE WHEN origem_norm = 'AccuWeather' THEN relato END) AS acc_relato,

            -- Climatempo valores
            MAX(CASE WHEN origem_norm = 'Climatempo' THEN temp_min_c END) AS cli_temp_min_c,
            MAX(CASE WHEN origem_norm = 'Climatempo' THEN temp_max_c END) AS cli_temp_max_c,
            MAX(CASE WHEN origem_norm = 'Climatempo' THEN probabilidade_chuva END) AS cli_prob_chuva,
            MAX(CASE WHEN origem_norm = 'Climatempo' THEN vento END) AS cli_vento,
            MAX(CASE WHEN origem_norm = 'Climatempo' THEN relato END) AS cli_relato

        FROM base
        GROUP BY 1,2
    )

    INSERT INTO "_gold".clima_unificado (
    cidade, forecast_date, last_updated_at,

    tem_accuweather, tem_climatempo, origem_atualizacao, origens_disponiveis,

    acc_temp_min_c, acc_temp_max_c, acc_prob_chuva, acc_vento, acc_relato, acc_scraped_at,
    cli_temp_min_c, cli_temp_max_c, cli_prob_chuva, cli_vento, cli_relato, cli_scraped_at
    )
    SELECT
    cidade,
    forecast_date,
    COALESCE(last_updated_at, now()),

    (acc_scraped_at IS NOT NULL) AS tem_accuweather,
    (cli_scraped_at IS NOT NULL) AS tem_climatempo,

    CASE
        WHEN acc_scraped_at IS NULL AND cli_scraped_at IS NULL THEN NULL
        WHEN cli_scraped_at IS NULL THEN 'AccuWeather'
        WHEN acc_scraped_at IS NULL THEN 'Climatempo'
        WHEN acc_scraped_at >= cli_scraped_at THEN 'AccuWeather'
        ELSE 'Climatempo'
    END AS origem_atualizacao,

    TRIM(BOTH '|' FROM
        (CASE WHEN acc_scraped_at IS NOT NULL THEN 'AccuWeather|' ELSE '' END) ||
        (CASE WHEN cli_scraped_at IS NOT NULL THEN 'Climatempo|' ELSE '' END)
    ) AS origens_disponiveis,

    acc_temp_min_c, acc_temp_max_c, acc_prob_chuva, acc_vento, acc_relato, acc_scraped_at,
    cli_temp_min_c, cli_temp_max_c, cli_prob_chuva, cli_vento, cli_relato, cli_scraped_at
    FROM agg

    ON CONFLICT (cidade, forecast_date)
    DO UPDATE SET
        last_updated_at = EXCLUDED.last_updated_at,
        
        tem_accuweather = EXCLUDED.tem_accuweather,
        tem_climatempo = EXCLUDED.tem_climatempo,
        origem_atualizacao = EXCLUDED.origem_atualizacao,
        origens_disponiveis = EXCLUDED.origens_disponiveis,

        acc_temp_min_c = EXCLUDED.acc_temp_min_c,
        acc_temp_max_c = EXCLUDED.acc_temp_max_c,
        acc_prob_chuva  = EXCLUDED.acc_prob_chuva,
        acc_vento       = EXCLUDED.acc_vento,
        acc_relato      = EXCLUDED.acc_relato,
        acc_scraped_at  = EXCLUDED.acc_scraped_at,

        cli_temp_min_c = EXCLUDED.cli_temp_min_c,
        cli_temp_max_c = EXCLUDED.cli_temp_max_c,
        cli_prob_chuva  = EXCLUDED.cli_prob_chuva,
        cli_vento       = EXCLUDED.cli_vento,
        cli_relato      = EXCLUDED.cli_relato,
        cli_scraped_at  = EXCLUDED.cli_scraped_at
    ;
    """

    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
        LOGGER.info("[Gold Clima] Upsert executado com sucesso.")
    finally:
        conn.close()
