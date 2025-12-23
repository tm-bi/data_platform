declare @idTerminal table (
    idTerminal int
)

insert into @idTerminal
--COLOCAR ENTRE OS PARENTESES SEPARADOS POR VIRGULA O ID DOS TERMINAIS 
-- EXEMPLO = (1,2,3)
select idTerminal from terminal where idTerminal  in (1,2,3,4,5,6,7,8,9,10,11,12)


DECLARE
	@dtInicial DATE = DATEADD(year, -3,cast(sysdatetime() as date)),
	@dtFinal DATE = CAST(sysdatetime() as date)


SELECT  
	acesso.idAcesso,
    empresaRelacionamento.idEmpresaRelacionamento,
	FORMAT(acesso.dataEntrada, 'dd/MM/yyyy HH:mm:ss') 'Data/Hora Entrada', 
	FORMAT(acesso.dataSaida, 'dd/MM/yyyy HH:mm:ss') 'Data/Hora Saída', 
	CASE
		WHEN ingresso.idIngresso IS NOT NULL AND ingresso.idIngresso > 0
		THEN ltrim(rtrim(pessoaIngresso.nomeRazaoSocial))
		ELSE ltrim(rtrim(pessoa.nomeRazaoSocial))
	END 'Sócio / Ingresso', 
    empresaRelacionamento.email,
    empresarelacionamento.telefone,
    empresarelacionamento.celular,
	CASE
		WHEN ingresso.idIngresso IS NOT NULL AND ingresso.idIngresso > 0
		THEN CONCAT('Ingresso - ', tp.descricao)
		ELSE IsNull(CONCAT(ct.numero, ' - ', ct.descricao), 'S/N') 
	END 'Categoria / Tipo de Ingresso',
    ingresso.numero 'Numero do ingresso',
	terminalEntrada.descricao 'Terminal Entrada'	,
	terminalSaida.descricao 'Terminal Saída'	,
	CASE
		WHEN ingresso.idIngresso IS NOT NULL AND ingresso.idIngresso > 0
		then 'Ingresso'
		ELSE 'Sócio'
	END 'Tipo de Acesso'
from acesso 
inner join 
(
    select
    erv.ativo,
    erv.nomeRazaoSocial,
    erv.cpfCnpj,
    erv.pfpj,
    erv.idEmpresaRelacionamento,
    erv.idPessoaRelacionamento,
    ts.idTituloSocio,
    titulo.idTitulo,
    titulo.numero Titulo,
    ct.idCategoriaTitulo,
    ct.descricao categoria,
    tre.idTipoRelacionamentoEmpresa,
    tre.descricao tipoRelacionamentoEmpresa,
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
            pessoa.idpessoa,
            email.informacao as email,
            celular.informacao as celular,
            telefone.informacao as telefone
        from pessoa
        left join
        (
            --email
            select 
            pec.idPessoa,
            string_agg(pca.informacao,' / ') informacao
            from pessoaContatoAcesso pca
            join  tipoAcessoContato tac on tac.idTipoAcessoContato = pca.idTipoAcessoContato
                and tac.tipo = 'E'
            join pessoaEnderecoContato pec on pec.idPessoaEnderecoContato = pca.idPessoaEnderecoContato
            group by pec.idPessoa
        ) as email on email.idPessoa = pessoa.idPessoa
        left join
        (
            --celular
            select 
            pec.idPessoa,
            string_agg(pca.informacao,' / ') informacao
            from pessoaContatoAcesso pca
            join  tipoAcessoContato tac on tac.idTipoAcessoContato = pca.idTipoAcessoContato
                and tac.tipo = 'C'
            join pessoaEnderecoContato pec on pec.idPessoaEnderecoContato = pca.idPessoaEnderecoContato
            group by pec.idPessoa
        ) as celular on celular .idPessoa = pessoa.idPessoa
        left join 
        (
            --telefone
            select 
            pec.idPessoa,
            string_agg(pca.informacao,' / ') informacao
            from pessoaContatoAcesso pca
            join  tipoAcessoContato tac on tac.idTipoAcessoContato = pca.idTipoAcessoContato
                and tac.tipo = 'F'
            join pessoaEnderecoContato pec on pec.idPessoaEnderecoContato = pca.idPessoaEnderecoContato
            group by pec.idPessoa
        ) as telefone on telefone.idPessoa = pessoa.idPessoa
    ) as contatos on contatos.idPessoa = erv.idPessoaRelacionamento
) empresaRelacionamento on acesso.idEmpresaRelacionamento = empresaRelacionamento.idEmpresaRelacionamento
inner join pessoa on pessoa.idPessoa = empresaRelacionamento.idPessoaRelacionamento
inner join terminal terminalEntrada on terminalEntrada.idTerminal = acesso.idTerminalEntrada
left join terminal terminalSaida on terminalSaida.idTerminal = acesso.idTerminalSaida
LEFT JOIN tituloSocio ts ON ts.idEmpresaRelacionamentoTitular = empresaRelacionamento.idEmpresaRelacionamento
LEFT JOIN categoriaTitulo ct ON ct.idCategoriaTitulo = ts.idCategoriaTitulo
LEFT JOIN ingresso ON ingresso.idIngresso = acesso.idIngresso
LEFT JOIN tipoIngresso tp ON tp.idTipoIngresso = ingresso.idTipoIngresso
LEFT JOIN empresaRelacionamento erIngresso ON erIngresso.idEmpresaRelacionamento = ingresso.idEmpresaRelacionamentoCliente
LEFT JOIN pessoa pessoaIngresso ON pessoaIngresso.idPessoa = erIngresso.idPessoaRelacionamento


where 
	CAST(acesso.dataEntrada AS DATE) between @dtInicial and @dtFinal and 
	(
		terminalEntrada.idTerminal IN(select * from @idTerminal) OR
		terminalSaida.idTerminal IN(select * from @idTerminal)
	)
order by idacesso desc --, acesso.dataEntrada, pessoa.nomeRazaoSocial


