from __future__ import annotations

from psycopg import connect as pg_connect
from psycopg.rows import tuple_row

from common.settings import settings


SILVER_CTX_TO_GOLD_SQL = """
insert into "_gold".fato_acessos (
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
  src.origem,
  src.id_acesso,
  src.num_ingresso,
  src.tipo_ingresso,
  src.terminal_entrada,
  src.dt_entrada,
  src.hr_entrada,
  src.tipo_publico,
  src.source_file,
  src.ingested_at
from (
  select
    'LIMBER'::text as origem,
    f.id_acesso::text as id_acesso,
    f.num_ingresso,
    f.tipo_ingresso,
    f.terminal_entrada,
    f.dt_entrada,
    f.hr_entrada,
    f.tipo_publico,
    f.source_file,
    f.ingested_at
  from "_silver-contexto".fato_acesso_limber f

  union all

  select
    'QUALITY'::text as origem,
    f.id_acesso::text as id_acesso,
    f.num_ingresso,
    f.tipo_ingresso,
    f.terminal_entrada,
    f.dt_entrada,
    f.hr_entrada,
    f.tipo_publico,
    f.source_file,
    f.ingested_at
  from "_silver-contexto".fato_acesso_quality f
) as src
left join "_gold".fato_acessos g
  on g.origem = src.origem
 and g.id_acesso = src.id_acesso
where g.id_acesso is null
;
"""


def silver_contexto_to_gold_fato_acessos() -> int:
    """
    Incremental e idempotente:
    insere na _gold somente (origem,id_acesso) ainda inexistente.
    """
    with pg_connect(settings.pg_dsn(), row_factory=tuple_row) as conn:
        with conn.cursor() as cur:
            cur.execute(SILVER_CTX_TO_GOLD_SQL)
            inserted = cur.rowcount
        conn.commit()
    return inserted
