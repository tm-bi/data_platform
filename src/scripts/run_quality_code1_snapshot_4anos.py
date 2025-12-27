from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from _bootstrap import setup_sys_path

setup_sys_path()

from _bronze.quality.extract_quality import extract_quality
from _bronze.quality.load_quality import load_quality_rows
from common.settings import settings


def main() -> int:
    now = datetime.now(ZoneInfo(settings.app_tz))
    end_date = now.date()
    start_date = (now - timedelta(days=365 * 4)).date()  # últimos 4 anos

    print(f"[QUALITY][CODE1] Snapshot: {start_date.isoformat()} -> {end_date.isoformat()}")

    rows = extract_quality(start_date=start_date, end_date=end_date, min_id_acesso=None)
    inserted = load_quality_rows(rows)

    print(f"[QUALITY][CODE1] Inseridos em _bronze.quality_acessos_raw: {inserted}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
