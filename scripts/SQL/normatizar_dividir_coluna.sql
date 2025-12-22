--TESTAR DIVISÃO
SELECT
    cidade,
    split_part(cidade, '/', 1) AS cidade,
    split_part(cidade, '/', 2) AS uf
FROM s_novaxs_182;

--CRIAR NOVA COLUNA
alter table s_novaxs_182 
add column uf text;

--DIVISÃO POR DEMARCADOR - COM COMMIT CASO PRECISE RETROAGIR
begin;

update s_novaxs_182 
set 
	cidade = trim(split_part(cidade, '/',1)),
	uf = trim(split_part(cidade, '/',2))
where cidade like '%/%';

commit;

--DIVISÃO POR POSIÇÃO
UPDATE s_novaxs_182
SET
    endereco = RTRIM(LEFT(endereco, LENGTH(endereco) - 9)),
    cep = RIGHT(endereco, 9)
WHERE endereco IS NOT NULL
  AND LENGTH(endereco) >= 9;

--TIRAR VIRGUMA FINAL

begin;
update s_novaxs_270 
set 
	endereco= rtrim(endereco, ',')
WHERE endereco LIKE '%,';
commit;