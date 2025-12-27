from __future__ import annotations

from psycopg import connect as pg_connect
from psycopg.rows import tuple_row

from common.settings import settings


def get_watermark(source_system: str, entity: str, watermark_key: str) -> str:
    sql = """
        select watermark_value
        from _control.etl_watermark
        where source_system = %s and entity = %s and watermark_key = %s
    """
    with pg_connect(settings.pg_dsn(), row_factory=tuple_row) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (source_system, entity, watermark_key))
            row = cur.fetchone()
            if row is None:
                return "0"
            return str(row[0])


def set_watermark(source_system: str, entity: str, watermark_key: str, watermark_value: str) -> None:
    sql = """
        insert into _control.etl_watermark (source_system, entity, watermark_key, watermark_value, updated_at)
        values (%s, %s, %s, %s, now())
        on conflict (source_system, entity, watermark_key)
        do update set watermark_value = excluded.watermark_value, updated_at = now()
    """
    with pg_connect(settings.pg_dsn(), row_factory=tuple_row) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (source_system, entity, watermark_key, watermark_value))
        conn.commit()
