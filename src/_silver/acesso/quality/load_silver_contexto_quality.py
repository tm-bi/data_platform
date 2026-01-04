from __future__ import annotations

from psycopg import connect as pg_connect
from psycopg.rows import tuple_row

from common.settings import settings


SILVER_TRANS_TO_CONTEXTO_QUALITY_SQL = """
insert into "_silver-contexto".fato_acesso_quality (
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
  s.id_acesso::text as id_acesso,
  s.num_ingresso,
  s.tipo_ingresso,
  s.terminal_entrada,
  s.dt_entrada,
  s.hr_entrada,

  case
    when dp.tipo_publico is null then 'CHECK'
    else dp.tipo_publico
  end as tipo_publico,

  %(source_file)s as source_file,
  coalesce(s.bronze_extracted_at, now()) as ingested_at
from "_silver-transacional".s_quality_acesso s
left join "_silver-contexto".dim_depara_tipo_publico_quality dp
  on dp.tipo_ingresso = s.tipo_ingresso
 and dp.ativo = true
left join "_silver-contexto".fato_acesso_quality f
  on f.id_acesso = s.id_acesso::text
where f.id_acesso is null
  -- não inserir OUTROS
  and s.tipo_ingresso <> 'OUTROS'
  and coalesce(dp.tipo_publico, 'CHECK') <> 'OUTROS'
;
"""


def silver_trans_to_silver_contexto_fato_quality(source_file: str = "sqlserver:quality") -> int:
    with pg_connect(settings.pg_dsn(), row_factory=tuple_row) as conn:
        with conn.cursor() as cur:
            cur.execute(SILVER_TRANS_TO_CONTEXTO_QUALITY_SQL, {"source_file": source_file})
            inserted = cur.rowcount
        conn.commit()
    return inserted
