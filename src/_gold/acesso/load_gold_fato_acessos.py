from __future__ import annotations

from psycopg import connect as pg_connect
from psycopg.rows import tuple_row

from src.common.settings import settings

SILVER_TO_GOLD_SQL = """
insert into _gold.fato_acessos (
    origem,
    id_acesso,
    num_ingresso,
    tipo_ingresso,
    terminal_entrada,
    dt_entrada,
    hr_entrada,
    tipo_publico,
    source_file,
    ingested_at
)
select
    f.origem,
    f.id_acesso,
    f.num_ingresso,
    f.tipo_ingresso,
    f.terminal_entrada,
    f.dt_entrada,
    f.hr_entrada,
    f.tipo_publico,
    f.source_file,
    f.ingested_at
from (
    -- LIMBER
    select
        'LIMBER'::text as origem,
        id_acesso,
        num_ingresso,
        tipo_ingresso,
        terminal_entrada,
        dt_entrada,
        hr_entrada,
        tipo_publico,
        source_file,
        ingested_at
    from "_silver-contexto".fato_acesso_limber

    union all

    -- QUALITY
    select
        'QUALITY'::text as origem,
        id_acesso,
        num_ingresso,
        tipo_ingresso,
        terminal_entrada,
        dt_entrada,
        hr_entrada,
        tipo_publico,
        source_file,
        ingested_at
    from "_silver-contexto".fato_acesso_quality
) f
left join _gold.fato_acessos g
  on g.id_acesso = f.id_acesso
where g.id_acesso is null;
"""


def silver_contexto_to_gold_fato_acessos() -> int:
    """
    Incremental e idempotente:
    Insere na gold apenas id_acesso que ainda não existe.

    Estratégia:
    - UNION ALL das fatos Limber + Quality
    - LEFT JOIN contra a gold
    - Insere apenas novos acessos
    """
    with pg_connect(settings.pg_dsn(), row_factory=tuple_row) as conn:
        with conn.cursor() as cur:
            print("[GOLD] SQL preview:\n", SILVER_TO_GOLD_SQL[:800])
            cur.execute(SILVER_TO_GOLD_SQL)
            inserted = cur.rowcount
        conn.commit()

    return inserted
