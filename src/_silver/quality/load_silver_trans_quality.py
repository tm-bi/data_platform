from __future__ import annotations

from psycopg import connect as pg_connect
from psycopg.rows import tuple_row

from common.settings import settings


BRONZE_TO_SILVER_QUALITY_SQL = """
insert into "_silver-transacional".s_quality_acesso (
  id_acesso,

  dt_hr_entrada,
  dt_entrada,
  hr_entrada,

  dt_hr_saida,
  dt_saida,
  hr_saida,

  terminal_entrada,
  terminal_saida,

  tipo_acesso,
  cliente,

  id_desc_tipo_ingresso,
  id_tipo_ingresso,
  tipo_ingresso,

  num_ingresso,
  id_emp_relac,

  email,
  telefone,
  celular,

  source_table,
  bronze_extracted_at,
  payload
)
select
  (b."idacesso")::bigint as id_acesso,

  nullif(b.payload->>'data_hora_entrada','')::timestamp as dt_hr_entrada,
  (nullif(b.payload->>'data_hora_entrada','')::timestamp)::date as dt_entrada,
  (nullif(b.payload->>'data_hora_entrada','')::timestamp)::time as hr_entrada,

  nullif(b.payload->>'data_hora_saida','')::timestamp as dt_hr_saida,
  (nullif(b.payload->>'data_hora_saida','')::timestamp)::date as dt_saida,
  (nullif(b.payload->>'data_hora_saida','')::timestamp)::time as hr_saida,

  nullif(b.payload->>'terminal_entrada','') as terminal_entrada,
  nullif(b.payload->>'terminal_saida','') as terminal_saida,

  nullif(b.payload->>'tipo_acesso','') as tipo_acesso,
  nullif(b.payload->>'socio_ou_ingresso','') as cliente,

  nullif(b.payload->>'categoria_tipo_ingresso','') as id_desc_tipo_ingresso,

  -- id_tipo_ingresso: só converte se a parte antes do " - " for numérica
  case
    when position(' - ' in coalesce(b.payload->>'categoria_tipo_ingresso','')) > 0
     and regexp_replace(trim(split_part(b.payload->>'categoria_tipo_ingresso', ' - ', 1)), '\\s+', '', 'g') ~ '^\\d+$'
      then regexp_replace(trim(split_part(b.payload->>'categoria_tipo_ingresso', ' - ', 1)), '\\s+', '', 'g')::bigint
    else null
  end as id_tipo_ingresso,

  -- tipo_ingresso (desc) com regra OUTROS se inválido/nulo/sem delimitador
  case
    when b.payload->>'categoria_tipo_ingresso' is null then 'OUTROS'
    when trim(b.payload->>'categoria_tipo_ingresso') = '' then 'OUTROS'
    when position(' - ' in b.payload->>'categoria_tipo_ingresso') = 0 then 'OUTROS'
    when not (regexp_replace(trim(split_part(b.payload->>'categoria_tipo_ingresso', ' - ', 1)), '\\s+', '', 'g') ~ '^\\d+$')
      then 'OUTROS'
    else nullif(trim(split_part(b.payload->>'categoria_tipo_ingresso', ' - ', 2)), '')
  end as tipo_ingresso,

  nullif(b.payload->>'numero_ingresso','') as num_ingresso,
  nullif(b.payload->>'idEmpresaRelacionamento','')::bigint as id_emp_relac,

  nullif(b.payload->>'email','') as email,
  nullif(b.payload->>'telefone','') as telefone,
  nullif(b.payload->>'celular','') as celular,

  '_bronze.quality_acessos_raw' as source_table,
  b.extracted_at as bronze_extracted_at,
  b.payload as payload
from _bronze.quality_acessos_raw b
left join "_silver-transacional".s_quality_acesso s
  on s.id_acesso = (b."idacesso")::bigint
where s.id_acesso is null
;
"""


def bronze_to_silver_trans_quality() -> int:
    with pg_connect(settings.pg_dsn(), row_factory=tuple_row) as conn:
        with conn.cursor() as cur:
            cur.execute(BRONZE_TO_SILVER_QUALITY_SQL)
            inserted = cur.rowcount
        conn.commit()
    return inserted
