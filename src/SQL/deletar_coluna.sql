-- Deletar colunas de uma tabela

alter table _gold.dim_produto 
drop column ingested_at,
drop column fonte_tabela_bronze;