from __future__ import annotations

import json
from typing import Iterable

from psycopg import connect as pg_connect
from psycopg.rows import tuple_row

from common.settings import settings
from _bronze.limber.extract_limber import LimberRow


def upsert_watermark(
    source_system: str,
    entity: str,
    last_value: str | None,
) -> None:
    """
    Atualiza a tabela de controle etl_watermark.
    """
    sql = """
        INSERT INTO stg.etl_watermark (source_system, entity, last_value)
        VALUES (%s, %s, %s)
        ON CONFLICT (source_system, entity)
        DO UPDATE
           SET last_value = EXCLUDED.last_value,
               updated_at = now();
    """

    with pg_connect(settings.pg_dsn(), row_factory=tuple_row) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (source_system, entity, last_value))
        conn.commit()


def load_limber_rows(rows: Iterable[LimberRow]) -> int:
    """
    Insere dados brutos do Limber na camada bronze (append-only),
    garantindo idempotência por NRVOUCHER.
    """
    insert_sql = """
        INSERT INTO stg.limber_acessos_raw (nrvoucher, payload)
        VALUES (%s, %s::jsonb)
        ON CONFLICT (nrvoucher) DO NOTHING;
    """

    inserted = 0
    last_nrvoucher: str | None = None

    with pg_connect(settings.pg_dsn(), row_factory=tuple_row) as conn:
        with conn.cursor() as cur:
            for row in rows:
                cur.execute(
                    insert_sql,
                    (row.nrvoucher, json.dumps(row.payload, default=str)),
                )
                inserted += cur.rowcount  # 1 se inseriu, 0 se já existia
                last_nrvoucher = row.nrvoucher

        conn.commit()

    # Atualiza watermark apenas após commit bem-sucedido
    upsert_watermark(
        source_system="limber",
        entity="acessos_raw",
        last_value=last_nrvoucher,
    )

    return inserted
