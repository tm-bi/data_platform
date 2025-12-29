import logging
from datetime import datetime, UTC
from zoneinfo import ZoneInfo

import psycopg2
from psycopg2.extras import execute_values

from src.common.settings import settings

LOGGER = logging.getLogger(__name__)

BRONZE_TABLE = "_bronze.scraping_clima_raw"
SILVER_TABLE = '"_silver-transacional".s_clima'

TZ_SP = ZoneInfo("America/Sao_Paulo") 

def _connect():
    return psycopg2.connect(
        host=settings.PG_HOST,
        port=settings.PG_PORT,
        user=settings.PG_USER,
        password=settings.PG_PASSWORD,
        dbname=settings.PG_DB,
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
        """
        Aceita:
        - 'YYYY-MM-DD'
        - 'DD/MM/YYYY'
        """
        if not d:
            return None
        s = str(d).strip()

        # ISO
        try:
            return datetime.fromisoformat(s).date()
        except Exception:
            pass

        # BR
        try:
            return datetime.strptime(s, "%d/%m/%Y").date()
        except Exception:
            return None


    def parse_scraped_at(data_scrap: str | None, hora_scrap: str | None) -> datetime:
        """
        Aceita data_scrap:
        - 'YYYY-MM-DD'
        - 'DD/MM/YYYY'
        hora_scrap:
        - 'HH:MM:SS' (ou vazio)
        Retorna timezone-aware (SP). Se preferir UTC, converta com astimezone(UTC).
        """
        if not data_scrap:
            return datetime.now(TZ_SP)

        if not hora_scrap:
            hora_scrap = "00:00:00"

        ds = str(data_scrap).strip()
        hs = str(hora_scrap).strip()

        # tenta ISO primeiro
        try:
            dt = datetime.fromisoformat(f"{ds}T{hs}")
            return dt.replace(tzinfo=TZ_SP)
        except Exception:
            pass

        # tenta BR
        try:
            d = datetime.strptime(ds, "%d/%m/%Y").date()
            t = datetime.strptime(hs, "%H:%M:%S").time()
            dt = datetime.combine(d, t)
            return dt.replace(tzinfo=TZ_SP)
        except Exception:
            # fallback final
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
    inserted = 0
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

                # split cidade -> cidade/uf
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

                # normaliza textos (opcional, mas bom)
                sensacao_termica = sens_term if sens_term is not None else None
                sensacao_termica_sombra = sens_sombra if sens_sombra is not None else None
                uv_index_max = ind_uv if ind_uv is not None else None
                vento_txt = vento if vento is not None else None
                probabilidade_chuva = prob_chuva if prob_chuva is not None else None
                relato = relatorio if relatorio is not None else None

                payload.append((
                    origem_norm,
                    cidade_nome,
                    dt_hr_scraping,
                    dt_forecast,
                    ttl_dias,
                    bloco_int,
                    temp_min_c,
                    temp_max_c,
                    sensacao_termica,
                    sensacao_termica_sombra,
                    uv_index_max,
                    vento_txt,
                    probabilidade_chuva,
                    relato,
                    bronze_id,
                    ingested_at,
                    ingestion_type,
                    ingestion_batch,
                    uf
                ))




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
            """



            execute_values(cur, sql_insert, payload, page_size=5000)
            inserted = len(payload)

        conn.commit()
        LOGGER.info(f"[Silver Trans Clima] Inseridas {inserted} linhas.")
        return inserted

    finally:
        conn.close()
