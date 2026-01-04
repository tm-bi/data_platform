from __future__ import annotations

import json
from typing import Iterable

from psycopg import connect as pg_connect
from psycopg.rows import tuple_row

from common.settings import settings
from _bronze.quality.extract_quality import QualityRow


UPSERT_SQL = """
insert into _bronze.quality_acessos_raw (idAcesso, extracted_at, payload)
values (%s, now(), %s::jsonb)
on conflict (idAcesso) do nothing;
"""


def load_quality_rows(rows: Iterable[QualityRow]) -> int:
    """
    Idempotente:
    - insere somente se idAcesso ainda não existe na bronze (append-only sem duplicar)
    """
    inserted = 0
    with pg_connect(settings.pg_dsn(), row_factory=tuple_row) as conn:
        with conn.cursor() as cur:
            for r in rows:
                payload_json = json.dumps(r.payload, default=str, ensure_ascii=False)
                cur.execute(UPSERT_SQL, (r.id_acesso, payload_json))
                # rowcount = 1 quando inseriu, 0 quando já existia (DO NOTHING)
                inserted += cur.rowcount
        conn.commit()
    return inserted
