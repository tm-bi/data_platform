import logging
from datetime import datetime
from zoneinfo import ZoneInfo

import psycopg2
from psycopg2.extras import execute_values

from src.common.settings import settings

LOGGER = logging.getLogger(__name__)

# ⚠️ Ajuste: alinhar com o TABLE usado em load_accuweather/load_climatempo
BRONZE_TABLE = '_bronze.scraping_clima_raw'

SILVER_TABLE = '"_silver-transacional".s_clima'
TZ_SP = ZoneInfo("America/Sao_Paulo")


def _connect():
    return psycopg2.connect(
        host=settings.pg_host,
        port=settings.pg_port,
        user=settings.pg_user,
        password=settings.pg_password,
        dbname=settings.pg_db,
    )


def load_silver_trans_clima(since_ingested_at: datetime | None = None) -> int:
    where = ""
    params = {}
    if since_ingested_at is not None:
        where = "WHERE ingested_at > %(since)s"
        params["since"] = since_ingested_at

    sql_select = f"""
        SELECT
            bronze_id,
            origem,
            cidade,
            data_scrap,
            hora_scrap,
            total_dias,
            bloco,
            data_previsao,
            tempmin,
            tempmax,
            sensacao_termica,
            sensacao_sombra,
            ind_max_uv,
            vento,
            probab_chuva,
            relatorio,
            ingested_at,
            ingestion_type,
            ingestion_batch
        FROM {BRONZE_TABLE}
        {where}
        ORDER BY ingested_at ASC
    """

    def _parse_date_any(d: str | None):
        if not d:
            return None
        s = str(d).strip()

        try:
            return datetime.fromisoformat(s).date()
        except Exception:
            pass

        try:
            return datetime.strptime(s, "%d/%m/%Y").date()
        except Exception:
            return None

    def parse_scraped_at(data_scrap: str | None, hora_scrap: str | None) -> datetime:
        if not data_scrap:
            return datetime.now(TZ_SP)

        hs = (hora_scrap or "00:00:00").strip()
        ds = str(data_scrap).strip()

        try:
            dt = datetime.fromisoformat(f"{ds}T{hs}")
            return dt.replace(tzinfo=TZ_SP)
        except Exception:
            pass

        try:
            d = datetime.strptime(ds, "%d/%m/%Y").date()
            t = datetime.strptime(hs, "%H:%M:%S").time()
            dt = datetime.combine(d, t)
            return dt.replace(tzinfo=TZ_SP)
        except Exception:
            return datetime.now(TZ_SP)

    def to_int(v):
        if v is None:
            return None
        try:
            if isinstance(v, int):
                return v
            s = str(v).strip().replace("°", "").replace("/", "")
            digits = "".join(ch for ch in s if ch.isdigit() or ch == "-")
            return int(digits) if digits else None
        except Exception:
            return None

    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(sql_select, params)
            rows = cur.fetchall()

            if not rows:
                LOGGER.info("[Silver Trans Clima] Nenhuma linha nova na Bronze.")
                return 0

            payload = []
            for r in rows:
                (
                    bronze_id, origem, cidade, data_scrap, hora_scrap, total_dias, bloco,
                    data_previsao, tempmin, tempmax, sens_term, sens_sombra, ind_uv,
                    vento, prob_chuva, relatorio, ingested_at, ingestion_type, ingestion_batch
                ) = r

                dt_hr_scraping = parse_scraped_at(data_scrap, hora_scrap)
                dt_forecast = _parse_date_any(data_previsao)
                if dt_forecast is None:
                    continue

                origem_norm = str(origem).strip() if origem else "unknown"

                cidade_raw = str(cidade).strip() if cidade else "unknown"
                if ", " in cidade_raw:
                    cidade_nome, uf = cidade_raw.split(", ", 1)
                    cidade_nome = cidade_nome.strip()
                    uf = uf.strip()
                else:
                    cidade_nome = cidade_raw
                    uf = None

                ttl_dias = int(total_dias) if total_dias is not None and str(total_dias).isdigit() else None
                bloco_int = int(bloco) if bloco is not None and str(bloco).isdigit() else None

                temp_min_c = to_int(tempmin)
                temp_max_c = to_int(tempmax)

                payload.append(
                    (
                        origem_norm,
                        cidade_nome,
                        dt_hr_scraping,
                        dt_forecast,
                        ttl_dias,
                        bloco_int,
                        temp_min_c,
                        temp_max_c,
                        sens_term if sens_term is not None else None,
                        sens_sombra if sens_sombra is not None else None,
                        ind_uv if ind_uv is not None else None,
                        vento if vento is not None else None,
                        prob_chuva if prob_chuva is not None else None,
                        relatorio if relatorio is not None else None,
                        bronze_id,
                        ingested_at,
                        ingestion_type,
                        ingestion_batch,
                        uf,
                    )
                )

            if not payload:
                LOGGER.warning("[Silver Trans Clima] Nenhuma linha válida para inserir.")
                return 0

            sql_insert = f"""
                INSERT INTO {SILVER_TABLE} (
                    origem_dado,
                    cidade,
                    dt_hr_scraping,
                    dt_forecast,
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
                    ingested_at,
                    ingestion_type,
                    ingestion_batch,
                    uf
                )
                VALUES %s
                -- Se existir UNIQUE (ex.: unique(bronze_id)), habilite:
                -- ON CONFLICT (bronze_id) DO NOTHING
            """

            execute_values(cur, sql_insert, payload, page_size=5000)

        conn.commit()
        inserted = len(payload)
        LOGGER.info(f"[Silver Trans Clima] Inseridas {inserted} linhas.")
        return inserted

    except Exception:
        conn.rollback()
        LOGGER.exception("[Silver Trans Clima] Falha (rollback executado).")
        raise
    finally:
        conn.close()

