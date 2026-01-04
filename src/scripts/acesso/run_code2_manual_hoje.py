from __future__ import annotations

from datetime import date, datetime
from zoneinfo import ZoneInfo

from _bootstrap import setup_sys_path

setup_sys_path()

from _bronze.limber.extract_limber import extract_limber_snapshot
from _bronze.limber.load_limber import load_limber_rows
from common.settings import settings


def today_date(tz: str) -> date:
    return datetime.now(ZoneInfo(tz)).date()


def main() -> int:
    today = today_date(settings.app_tz)

    print(f"[CODE2] Incremental manual Limber (hoje): {today.isoformat()}")

    rows = extract_limber_snapshot(start_date=today, end_date=today)
    inserted = load_limber_rows(rows)

    print(f"[CODE2] Inseridos hoje em stg.limber_acessos_raw: {inserted}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
