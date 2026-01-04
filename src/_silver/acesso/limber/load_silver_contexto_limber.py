from __future__ import annotations

from psycopg import connect as pg_connect
from psycopg.rows import tuple_row

from common.settings import settings


SILVER_TRANS_TO_CONTEXTO_SQL = """
insert into "_silver-contexto".fato_acesso_limber (
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
  s.id_acesso,
  s.num_ingresso,
  s.tipo_ingresso,
  s.terminal_entrada,
  s.dt_entrada,
  s.hr_entrada,

  -- regra:
  -- 1) não achou no de/para => CHECK
  -- 2) achou => usa o valor do de/para (exceto OUTROS que será filtrado no WHERE)
case
  when dp.tipo_publico is null then 'CHECK'
  --when dp.tipo_publico = 'OUTROS' then 'CHECK'
  else dp.tipo_publico
end as tipo_publico,

  %(source_file)s as source_file,
  coalesce(s.bronze_extracted_at, now()) as ingested_at
from "_silver-transacional".s_limber_acesso s
left join "_silver-contexto".dim_depara_tipo_publico_limber dp
  on dp.tipo_ingresso = s.tipo_ingresso
 and dp.active = true
left join "_silver-contexto".fato_acesso_limber f
  on f.id_acesso = s.id_acesso
where f.id_acesso is null
  and (dp.tipo_publico IS NULL OR dp.tipo_publico <> 'OUTROS');;
"""


def silver_trans_to_silver_contexto_fato_limber(source_file: str = "firebird:limber") -> int:
    """
    Incremental e idempotente:
    insere na fato apenas id_acesso que ainda não existe.
    """
    with pg_connect(settings.pg_dsn(), row_factory=tuple_row) as conn:
        with conn.cursor() as cur:
            cur.execute(SILVER_TRANS_TO_CONTEXTO_SQL, {"source_file": source_file})
            inserted = cur.rowcount
        conn.commit()
    return inserted
