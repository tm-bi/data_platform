from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Iterable

import pyodbc

from common.settings import settings


@dataclass(frozen=True)
class QualityRow:
    id_acesso: str
    payload: dict[str, Any]


def _parse_terminal_ids(value: str) -> list[int]:
    ids: list[int] = []
    for part in value.split(","):
        part = part.strip()
        if not part:
            continue
        ids.append(int(part))
    if not ids:
        raise ValueError("QUALITY_TERMINAL_IDS vazio/invalid no .env")
    return ids


def _mssql_conn() -> pyodbc.Connection:
    conn_str = (
        f"DRIVER={{{settings.mssql_driver}}};"
        f"SERVER={settings.mssql_host},{settings.mssql_port};"
        f"DATABASE={settings.mssql_db};"
        f"UID={settings.mssql_user};"
        f"PWD={settings.mssql_password};"
        f"Encrypt={settings.mssql_encrypt};"
        f"TrustServerCertificate={settings.mssql_trust_cert};"
    )
    return pyodbc.connect(conn_str, timeout=30)


def extract_quality(
    start_date: date,
    end_date: date,
    min_id_acesso: int | None = None,
) -> Iterable[QualityRow]:
    """
    Extrai acessos do SQL Server (Quality).

    - Snapshot: passe min_id_acesso=None (traz pelo range de datas)
    - Incremental: passe min_id_acesso=último_id (traz somente idAcesso > min)

    Observação: usamos tipos nativos (datetime), sem FORMAT(), para não virar string.
    """
    terminal_ids = _parse_terminal_ids(settings.quality_terminal_ids)

    # placeholders para IN (?, ?, ?, ...)
    terminals_placeholders = ",".join(["?"] * len(terminal_ids))

    # filtro incremental opcional
    incremental_filter = ""
    params: list[Any] = []
    params.extend([start_date, end_date])  # datas
    params.extend(terminal_ids)            # terminais entrada
    params.extend(terminal_ids)            # terminais saida

    if min_id_acesso is not None:
        incremental_filter = " and acesso.idAcesso > ? "
        params.append(min_id_acesso)

    sql = f"""
    SELECT
        acesso.idAcesso,
        acesso.idEmpresaRelacionamento,
        acesso.dataEntrada as data_hora_entrada,
        acesso.dataSaida as data_hora_saida,

        CASE
            WHEN ingresso.idIngresso IS NOT NULL AND ingresso.idIngresso > 0
            THEN ltrim(rtrim(pessoaIngresso.nomeRazaoSocial))
            ELSE ltrim(rtrim(pessoa.nomeRazaoSocial))
        END as socio_ou_ingresso,

        empresaRelacionamento.email,
        empresaRelacionamento.telefone,
        empresaRelacionamento.celular,

        CASE
            WHEN ingresso.idIngresso IS NOT NULL AND ingresso.idIngresso > 0
            THEN CONCAT('Ingresso - ', tp.descricao)
            ELSE IsNull(CONCAT(ct.numero, ' - ', ct.descricao), 'S/N')
        END as categoria_tipo_ingresso,

        ingresso.numero as numero_ingresso,
        terminalEntrada.descricao as terminal_entrada,
        terminalSaida.descricao as terminal_saida,

        CASE
            WHEN ingresso.idIngresso IS NOT NULL AND ingresso.idIngresso > 0
            THEN 'Ingresso'
            ELSE 'Sócio'
        END as tipo_acesso

    from acesso
    inner join (
        select
            erv.ativo,
            erv.nomeRazaoSocial,
            erv.cpfCnpj,
            erv.pfpj,
            erv.idEmpresaRelacionamento,
            erv.idPessoaRelacionamento,
            ts.idTituloSocio,
            titulo.idTitulo,
            titulo.numero as titulo_numero,
            ct.idCategoriaTitulo,
            ct.descricao as categoria,
            tre.idTipoRelacionamentoEmpresa,
            tre.descricao as tipoRelacionamentoEmpresa,
            erv.dataInicio,
            erv.dataTermino,
            contatos.celular,
            contatos.telefone,
            contatos.email
        from EmpresaRelacionamentoView erv
        join tipoRelacionamentoEmpresa tre on tre.idTipoRelacionamentoEmpresa = erv.idTipoRelacionamentoEmpresa
        left join TituloSocio ts on ts.idEmpresaRelacionamentoTitular = erv.idEmpresaRelacionamento
        left join titulo on titulo.idTitulo = ts.idTitulo
        left join categoriaTitulo ct on ct.idCategoriaTitulo = ts.idCategoriaTitulo
        left join (
            select
                pessoa.idPessoa,
                email.informacao as email,
                celular.informacao as celular,
                telefone.informacao as telefone
            from pessoa
            left join (
                select
                    pec.idPessoa,
                    string_agg(pca.informacao,' / ') informacao
                from pessoaContatoAcesso pca
                join tipoAcessoContato tac on tac.idTipoAcessoContato = pca.idTipoAcessoContato
                    and tac.tipo = 'E'
                join pessoaEnderecoContato pec on pec.idPessoaEnderecoContato = pca.idPessoaEnderecoContato
                group by pec.idPessoa
            ) as email on email.idPessoa = pessoa.idPessoa
            left join (
                select
                    pec.idPessoa,
                    string_agg(pca.informacao,' / ') informacao
                from pessoaContatoAcesso pca
                join tipoAcessoContato tac on tac.idTipoAcessoContato = pca.idTipoAcessoContato
                    and tac.tipo = 'C'
                join pessoaEnderecoContato pec on pec.idPessoaEnderecoContato = pca.idPessoaEnderecoContato
                group by pec.idPessoa
            ) as celular on celular.idPessoa = pessoa.idPessoa
            left join (
                select
                    pec.idPessoa,
                    string_agg(pca.informacao,' / ') informacao
                from pessoaContatoAcesso pca
                join tipoAcessoContato tac on tac.idTipoAcessoContato = pca.idTipoAcessoContato
                    and tac.tipo = 'F'
                join pessoaEnderecoContato pec on pec.idPessoaEnderecoContato = pca.idPessoaEnderecoContato
                group by pec.idPessoa
            ) as telefone on telefone.idPessoa = pessoa.idPessoa
        ) as contatos on contatos.idPessoa = erv.idPessoaRelacionamento
    ) empresaRelacionamento on acesso.idEmpresaRelacionamento = empresaRelacionamento.idEmpresaRelacionamento

    inner join pessoa on pessoa.idPessoa = empresaRelacionamento.idPessoaRelacionamento
    inner join terminal terminalEntrada on terminalEntrada.idTerminal = acesso.idTerminalEntrada
    left join terminal terminalSaida on terminalSaida.idTerminal = acesso.idTerminalSaida
    left join tituloSocio ts on ts.idEmpresaRelacionamentoTitular = empresaRelacionamento.idEmpresaRelacionamento
    left join categoriaTitulo ct on ct.idCategoriaTitulo = ts.idCategoriaTitulo
    left join ingresso on ingresso.idIngresso = acesso.idIngresso
    left join tipoIngresso tp on tp.idTipoIngresso = ingresso.idTipoIngresso
    left join empresaRelacionamento erIngresso on erIngresso.idEmpresaRelacionamento = ingresso.idEmpresaRelacionamentoCliente
    left join pessoa pessoaIngresso on pessoaIngresso.idPessoa = erIngresso.idPessoaRelacionamento

    where
        cast(acesso.dataEntrada as date) between ? and ?
        and (
            terminalEntrada.idTerminal in ({terminals_placeholders})
            or terminalSaida.idTerminal in ({terminals_placeholders})
        )
        {incremental_filter}
    order by acesso.idAcesso asc
    """

    with _mssql_conn() as conn:
        cur = conn.cursor()
        cur.execute(sql, params)

        col_names = [d[0] for d in cur.description]
        for row in cur:
            data = dict(zip(col_names, row))
            id_acesso = str(data.get("idAcesso", "")).strip()
            if not id_acesso:
                continue
            yield QualityRow(id_acesso=id_acesso, payload=data)
