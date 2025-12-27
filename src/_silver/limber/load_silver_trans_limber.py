from __future__ import annotations

from psycopg import connect as pg_connect
from psycopg.rows import tuple_row

from common.settings import settings


BRONZE_TO_SILVER_SQL = """
insert into "_silver-transacional".s_limber_acesso (
  id_acesso,
  dt_acesso_sys,

  dt_hr_voucher,
  dt_voucher,
  hr_voucher,

  num_ingresso,

  dt_hr_baixa,
  dt_entrada,
  hr_entrada,

  terminal_entrada,
  cod_grupo,
  nome_grupo,

  tipo_bilhete,
  cod_bilhete,
  tipo_ingresso,
  categoria,
  tipo,

  qtd,
  vlr_unit,

  src_nrvoucher,
  bronze_extracted_at,
  payload
)
select
  b.nrvoucher as id_acesso,

  nullif(b.payload->>'DATA_ACESSO','')::date as dt_acesso_sys,

  nullif(b.payload->>'DT_HR_VOUCHER','')::timestamp as dt_hr_voucher,
  (nullif(b.payload->>'DT_HR_VOUCHER','')::timestamp)::date as dt_voucher,
  (nullif(b.payload->>'DT_HR_VOUCHER','')::timestamp)::time as hr_voucher,

  b.payload->>'QRCODE' as num_ingresso,

  nullif(b.payload->>'DTBAIXA','')::timestamp as dt_hr_baixa,

  -- regra escolhida: acesso = baixa
  (nullif(b.payload->>'DTBAIXA','')::timestamp)::date as dt_entrada,
  (nullif(b.payload->>'DTBAIXA','')::timestamp)::time as hr_entrada,

  b.payload->>'PONTO_VENDA' as terminal_entrada,
  b.payload->>'CODIGO_GRUPO' as cod_grupo,
  b.payload->>'NOME_GRUPO' as nome_grupo,

  b.payload->>'TIPO_BILHETE' as tipo_bilhete,
  b.payload->>'CODIGO_BILHETE' as cod_bilhete,
  b.payload->>'BILHETE' as tipo_ingresso,
  b.payload->>'CATEGORIA' as categoria,
  b.payload->>'TIPO' as tipo,

  nullif(b.payload->>'QTDE','')::numeric as qtd,
  nullif(b.payload->>'VLR_UNITARIO','')::numeric as vlr_unit,

  b.nrvoucher as src_nrvoucher,
  b.extracted_at as bronze_extracted_at,
  b.payload as payload
from _bronze.limber_acessos_raw b
left join "_silver-transacional".s_limber_acesso s
  on s.id_acesso = b.nrvoucher
where s.id_acesso is null;
"""

def bronze_to_silver_trans_limber() -> int:
    """
    Incremental e idempotente:
    Insere na silver-transacional apenas NRVOUCHER que ainda não existe em s_limber_acesso.
    """
    with pg_connect(settings.pg_dsn(), row_factory=tuple_row) as conn:
        with conn.cursor() as cur:
            cur.execute(BRONZE_TO_SILVER_SQL)
            inserted = cur.rowcount
        conn.commit()
    return inserted
