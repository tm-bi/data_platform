from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Iterable

from firebird.driver import connect as fb_connect

from common.settings import settings


@dataclass(frozen=True)
class LimberRow:
    nrvoucher: str
    payload: dict[str, Any]


def extract_limber_snapshot(start_date: date, end_date: date) -> Iterable[LimberRow]:
    """
    Extrai do Firebird registros entre start_date e end_date (inclusive),
    retornando NRVOUCHER como chave + payload bruto.
    """
    dsn = f"{settings.firebird_host}/{settings.firebird_port}:{settings.firebird_db}"

    sql = """
        SELECT
            T.DTVENDA AS DT_HR_VOUCHER,
            B.DATA AS DATA_ACESSO,
            CAST(T.NRVOUCHER AS VARCHAR(32)) AS NRVOUCHER,
            VP.QRCODE,
            VP.DTBAIXA,
            B.DATA AS DATA_VENDA_BILHETERIA,
            B.HORA AS HORA_VENDA_BILHETERIA,
            B.PONTO_VENDA,
            GB.CODIGO AS CODIGO_GRUPO,
            GB.NOME AS NOME_GRUPO,
            CB.TIPO_BILHETE,
            CB.CODIGO AS CODIGO_BILHETE,
            CB.NOME AS BILHETE,
            CC.NOME AS CATEGORIA,
            GB.NOME AS TIPO,
            I.QUANTIDADE AS QTDE,
            I.VLR_UNITARIO
        FROM BCA_BILHETE B
        JOIN BCA_BILHETE_ITEM I
            ON I.EMPRESA = B.EMPRESA
            AND I.CODIGO = B.CODIGO
        JOIN BCA_BILHETE_ITEM_CARTAO IC
            ON IC.EMPRESA = I.EMPRESA
            AND IC.CODIGO = I.CODIGO
            AND IC.SEQUENCIA = I.SEQUENCIA
            AND IC.STATUS = 1
        LEFT JOIN TBVENVENDASPRODUTOS VP
            ON VP.EMPRESA = I.EMPRESA
            AND VP.IDVENDA = I.VOUCHER
            AND VP.SEQUENCIA = I.VOUCHER_SEQ
        LEFT JOIN TBVENVENDAS T
            ON T.EMPRESA = VP.EMPRESA
            AND T.IDVENDA = VP.IDVENDA
        LEFT JOIN BCA_CAD_BILHETE CB
            ON CB.EMPRESA = I.EMPRESA
            AND CB.CODIGO = I.BILHETE
        LEFT JOIN BCA_CAD_CATEGORIA CC
            ON CC.EMPRESA = I.EMPRESA
            AND CC.CODIGO = I.CATEGORIA
        LEFT JOIN BCA_CAD_GRUPO GB
            ON GB.EMPRESA = CB.EMPRESA
            AND GB.CODIGO = CB.GRUPO
        WHERE
            B.EMPRESA = 1
            AND B.DATA BETWEEN ? AND ?
            AND COALESCE(B.CANCELADO, 'N') = 'N'
        ORDER BY CAST(T.NRVOUCHER AS VARCHAR(32))
    """

    with fb_connect(
        database=dsn,
        user=settings.firebird_user,
        password=settings.firebird_password,
        charset=settings.firebird_charset,
    ) as conn:
        cur = conn.cursor()
        cur.execute(sql, (start_date, end_date))
        col_names = [d[0].strip() for d in cur.description]

        for row in cur:
            data = dict(zip(col_names, row))
            nrvoucher = str(data.get("NRVOUCHER", "")).strip()
            if not nrvoucher:
                continue
            yield LimberRow(nrvoucher=nrvoucher, payload=data)
