from __future__ import annotations

from psycopg import connect as pg_connect
from psycopg.rows import tuple_row

from common.settings import settings


BRONZE_TO_SILVER_SQL = """
insert into "_silver-transacional".s_limber_acesso (
  id_acesso,
  data_acesso,
  dt_hr_voucher,
  qrcode,
  dtbaixa,
  ponto_venda,
  codigo_grupo,
  nome_grupo,
  tipo_bilhete,
  codigo_bilhete,
  bilhete,
  categoria,
  tipo,
  qtde,
  vlr_unitario,
  src_nrvoucher,
  bronze_extracted_at,
  payload
)
select
  b.nrvoucher as id_acesso,
  nullif(b.payload->>'DATA_ACESSO','')::date as data_acesso,
  nullif(b.payload->>'DT_HR_VOUCHER','')::timestamp as dt_hr_voucher,
  b.payload->>'QRCODE' as qrcode,
  nullif(b.payload->>'DTBAIXA','')::timestamp as dtbaixa,
  b.payload->>'PONTO_VENDA' as ponto_venda,
  b.payload->>'CODIGO_GRUPO' as codigo_grupo,
  b.payload->>'NOME_GRUPO' as nome_grupo,
  b.payload->>'TIPO_BILHETE' as tipo_bilhete,
  b.payload->>'CODIGO_BILHETE' as codigo_bilhete,
  b.payload->>'BILHETE' as bilhete,
  b.payload->>'CATEGORIA' as categoria,
  b.payload->>'TIPO' as tipo,
  nullif(b.payload->>'QTDE','')::numeric as qtde,
  nullif(b.payload->>'VLR_UNITARIO','')::numeric as vlr_unitario,
  b.nrvoucher as src_nrvoucher,
  b.extracted_at as bronze_extracted_at,
  b.payload as payload
from _bronze.limber_acessos_raw b
left join "_silver-transacional".s_limber_acesso s
  on s.id_acesso = b.nrvoucher
where s.id_acesso is null
;
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
