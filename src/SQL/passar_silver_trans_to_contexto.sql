--EXEMPLO, PASSANDO s_novaxs_182 PARA fato_vendas_produto_ecom_xs

INSERT INTO fato_vendas_produto_ecom_xs (
    id_venda,
    produto,
    dt_venda,
    qtd,
    vlr_total,
    local_venda,
    source_file
)
SELECT
    s.id_venda::bigint,
    trim(s.produto)::text                    AS produto,
    s.dt_venda::date                         AS dt_venda,
    s.qtd::bigint                         	 AS qtd,
    s.vlr_total::numeric(18,2)               AS vlr_total,
    trim(s.agencia)::text                    AS local_venda,
    's_novaxs_270'::text                     AS source_file
FROM "_silver-transacional".s_novaxs_182 s
WHERE upper(trim(s.status)) = 'CONFIRMADA'
  AND (
        s.agencia IS NULL
        OR upper(trim(s.agencia)) NOT IN (
            'THERMAS DA MATA - BILHETERIA',
            'THERMAS DA MATA - PORTAL DO AGENTE',
            'RENATO RODRIGUES DE OLIVEIRA',
            'TREINAMENTO THERMAS DA MATA 01',
            'TESTE TREINAMENTO THERMAS DA MATA',
            'RESTAURANTE MINA DO ESPETO',
            'LUCIANO OLIVEIRA',
            'TESTE THERMAS DA MATA',
            'DANIELA SAC'
        )
      );


select *
from "_silver-transacional".s_novaxs_270
where agencia = 'DANIELA SAC'

select forma_pagamento, count(*) as qtd
from "_silver-transacional".s_novaxs_270 sn 
group by forma_pagamento 
order by  qtd desc
