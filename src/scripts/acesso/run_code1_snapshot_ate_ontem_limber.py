from __future__ import annotations

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from _bootstrap import setup_sys_path

setup_sys_path()

from _bronze.limber.extract_limber import extract_limber_snapshot
from _bronze.limber.load_limber import load_limber_rows
from common.settings import settings


def yesterday_date(tz: str) -> date:
    now = datetime.now(ZoneInfo(tz))
    return (now - timedelta(days=1)).date()


def main() -> int:
    start_date = date(2025, 1, 1)  # ajuste se quiser outro início histórico
    end_date = yesterday_date(settings.app_tz)

    print(f"[CODE1] Snapshot Limber: {start_date.isoformat()} -> {end_date.isoformat()}")

    rows = extract_limber_snapshot(start_date=start_date, end_date=end_date)
    inserted = load_limber_rows(rows)

    print(f"[CODE1] Inseridos em stg.limber_acessos_raw: {inserted}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
