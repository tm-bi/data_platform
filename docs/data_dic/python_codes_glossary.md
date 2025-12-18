# Glossário dos Arquivos .py
Esse documento explica para que serve cada arquivo .py da pasta data_plarform na VPS do Thermas da Mata

## Códigos que rodam na camada Bronze (raw)
O único tratamento que colocamos nestas tabelas é a data de ingestão

*main_bronze_182.py* - pega os arquivos .csv na pasta 'root/db_medallion/bronze/nova_xs/novaxs_182/' da VPS e os sobe para o servidor PostgreSQL que está na VPS no SCHEMA '_bronze'
*main_bronze_270.py* - pega os arquivos .csv na pasta 'root/db_medallion/bronze/nova_xs/novaxs_270/' da VPS e os sobe para o servidor PostgreSQL que está na VPS no SCHEMA '_bronze'
*main_bronze_418.py* - pega os arquivos .csv na pasta 'root/db_medallion/bronze/nova_xs/novaxs_418/' da VPS e os sobe para o servidor PostgreSQL que está na VPS no SCHEMA '_bronze'
*main_bronze_664.py* - pega os arquivos .csv na pasta 'root/db_medallion/bronze/nova_xs/novaxs_664/' da VPS e os sobe para o servidor PostgreSQL que está na VPS no SCHEMA '_bronze'

_Esses arquivos .csv foram baixados do motor de vendas da Nova XS_

##Códigos que rodam na camada Silver-transacional
Aqui fazemos os seguintes tratamentos: 
  - Tiramos os metadados das tabelas, 
  - Normatizamos os cabeçalhos (ver documento de data_glossary.md), 
  - Definimos os tipos de dados de cada coluna
  - Unificamos as tabelas para ter uma única tabela

Arquivos:
    *main_silver-trans_182.py* 
    *main_silver-trans_270.py*  
    *main_silver-trans_418.py*  
    *main_silver-trans_664.py* 

Todos os outros tratamentos são feitos em SQL via DBeaver ou outro sistema diretamente no banco de dados.