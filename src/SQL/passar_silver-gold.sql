--Código para trazer a tabela tratada da silver para a gold
--Cria uma cópia da tabela com as colunas desejadas

create table dim_formaPgto as 
select
	forma_pagamento,
	tipo_pagamento
from "_silver-transacional".s_dim_formapgto