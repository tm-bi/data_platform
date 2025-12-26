from __future__ import annotations

from datetime import date, datetime, time
from zoneinfo import ZoneInfo

from _bootstrap import setup_sys_path

setup_sys_path()

from _bronze.limber.extract_limber import extract_limber_snapshot
from _bronze.limber.load_limber import load_limber_rows
from common.settings import settings


def now_local(tz: str) -> datetime:
    return datetime.now(ZoneInfo(tz))


def within_daily_window(dt: datetime) -> bool:
    # Horário de atualização: 08:00 <= agora <= 20:00
    return time(8, 0, 0) <= dt.time() <= time(20, 0, 0)


def within_new_year_event_window(dt: datetime) -> bool:
    """
    Horário Rèveillon:
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


def main() -> int:
    now = now_local(settings.app_tz)

    if not should_run(now):
        print(f"[CODE3] fora da janela (agora={now.isoformat()}). Saindo.")
        return 0

    today: date = now.date()
    print(f"[CODE3] Incremental Limber (hoje={today.isoformat()})")

    rows = extract_limber_snapshot(start_date=today, end_date=today)
    inserted = load_limber_rows(rows)

    print(f"[CODE3] Inseridos em _bronze.limber_acessos_raw: {inserted}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
