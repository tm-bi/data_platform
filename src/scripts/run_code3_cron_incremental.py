from __future__ import annotations

from datetime import date, datetime, time
from zoneinfo import ZoneInfo

from _bootstrap import setup_sys_path
from dotenv import load_dotenv
from psycopg import connect as pg_connect

setup_sys_path()

from common.settings import settings

# LIMBER
from _bronze.limber.extract_limber import extract_limber_snapshot
from _bronze.limber.load_limber import load_limber_rows
from _silver.limber.load_silver_trans_limber import bronze_to_silver_trans_limber
from _silver.limber.load_silver_contexto_limber import (
    silver_trans_to_silver_contexto_fato_limber,
)

# QUALITY (somente bronze por enquanto)
from _bronze.quality.extract_quality import extract_quality
from _bronze.quality.load_quality import load_quality_rows
from _silver.quality.load_silver_trans_quality import bronze_to_silver_trans_quality
from _silver.quality.load_silver_contexto_quality import silver_trans_to_silver_contexto_fato_quality
from _gold.load_gold_fato_acessos import silver_contexto_to_gold_fato_acessos

import traceback

def log_exception(prefix: str, exc: Exception) -> None:
    print(f"[{prefix}] ERRO: {type(exc).__name__}: {exc}")
    print(traceback.format_exc())


def now_local(tz: str) -> datetime:
    return datetime.now(ZoneInfo(tz))


def within_daily_window(dt: datetime) -> bool:
    # Janela padrão: 07:00 <= agora <= 20:00
    return time(7, 0, 0) <= dt.time() <= time(20, 0, 0)


def within_new_year_event_window(dt: datetime) -> bool:
    """
    Janela Réveillon:
    - 31/12: 20:00 -> 23:59
    - 01/01: 00:00 -> 03:00
    """
    if dt.month == 12 and dt.day == 31:
        return dt.time() >= time(20, 0, 0)
    if dt.month == 1 and dt.day == 1:
        return dt.time() <= time(3, 0, 0)
    return False


def should_run(dt: datetime) -> bool:
    return within_daily_window(dt) or within_new_year_event_window(dt)


def run_limber_pipeline(today: date) -> None:
    print(f"[LIMBER] Início (dia={today.isoformat()})")

    # Incremental simples e robusto: lê o DIA TODO e insere só o novo (dedupe por NRVOUCHER)
    rows = extract_limber_snapshot(start_date=today, end_date=today)
    inserted_bronze = load_limber_rows(rows)
    print(f"[LIMBER] Bronze _bronze.limber_acessos_raw: +{inserted_bronze}")

    # Auto-healing: roda sempre (se tiver backlog, ele recupera)
    inserted_trans = bronze_to_silver_trans_limber()
    print(f"[LIMBER] Silver-trans s_limber_acesso: +{inserted_trans}")

    inserted_ctx = silver_trans_to_silver_contexto_fato_limber(source_file="firebird:limber")
    print(f"[LIMBER] Silver-contexto fato_acesso_limber: +{inserted_ctx}")

    print("[LIMBER] Fim")


def run_quality_pipeline(today: date) -> None:
    print(f"[QUALITY] Início (dia={today.isoformat()})")

    rows = extract_quality(start_date=today, end_date=today, min_id_acesso=None)
    inserted_bronze = load_quality_rows(rows)
    print(f"[QUALITY] Bronze _bronze.quality_acessos_raw: +{inserted_bronze}")

    inserted_trans = bronze_to_silver_trans_quality()
    print(f"[QUALITY] Silver-trans s_quality_acesso: +{inserted_trans}")

    inserted_ctx = silver_trans_to_silver_contexto_fato_quality(source_file="sqlserver:quality")
    print(f"[QUALITY] Silver-contexto fato_acesso_quality: +{inserted_ctx}")


    # ---- métricas de validação (auditoria) ----
    with pg_connect(settings.pg_dsn()) as conn:
        with conn.cursor() as cur:
            # total classificados como OUTROS na transacional
            cur.execute(
                """
                select count(*)
                from "_silver-transacional".s_quality_acesso
                where tipo_ingresso = 'OUTROS';
                """
            )
            total_outros = cur.fetchone()[0]

            # total ainda fora do contexto
            cur.execute(
                """
                select count(*)
                from "_silver-transacional".s_quality_acesso s
                left join "_silver-contexto".fato_acesso_quality f
                  on f.id_acesso = s.id_acesso::text
                where f.id_acesso is null;
                """
            )
            faltantes_ctx = cur.fetchone()[0]
    print(
        "[QUALITY] Validação | "
        f"OUTROS(trans)={total_outros} | "
        f"fora_do_contexto={faltantes_ctx}"
    )

    print("[QUALITY] Fim")


def main() -> int:
    now = now_local(settings.app_tz)

    if not should_run(now) and not settings.force_run:
        print(f"[CODE3] fora da janela (agora={now.isoformat()}). Saindo.")
        return 0

    today = now.date()
    print(f"[CODE3] Execução incremental (dia={today.isoformat()})")

    limber_ok = True
    quality_ok = True

    # ---- LIMBER ----
    try:
        run_limber_pipeline(today)
    except Exception as exc:
        limber_ok = False
        log_exception("LIMBER", exc)

    # ---- QUALITY ----
    try:
        run_quality_pipeline(today)
    except Exception as exc:
        quality_ok = False
        log_exception("QUALITY", exc)

    # ---- GOLD (auto-healing) ----
    try:
        inserted_gold = silver_contexto_to_gold_fato_acessos()
        print(f"[GOLD] Inseridos em _gold.fato_acessos: +{inserted_gold}")
    except Exception as exc:
        log_exception("GOLD", exc)
        # Gold falhar não impede retornar código != 0 se preferir; aqui vamos sinalizar falha.
        return 2

    if not limber_ok or not quality_ok:
        print("[CODE3] Finalizado com falhas parciais (ver logs acima).")
        return 1

    print("[CODE3] Finalizado com sucesso.")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
