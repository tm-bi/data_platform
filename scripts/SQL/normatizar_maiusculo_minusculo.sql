--Código para padronizar Primeira letra maiúscula, demais minúsculas e de, da, dos, do, entre nomes, minúsculo

UPDATE dim_cliente
SET cliente =
    REGEXP_REPLACE(
        REGEXP_REPLACE(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        INITCAP(LOWER(TRIM(cliente))),
                        '\mDe\M', 'de', 'g'
                    ),
                    '\mDa\M', 'da', 'g'
                ),
                '\mDo\M', 'do', 'g'
            ),
            '\mDas\M', 'das', 'g'
        ),
        '\mDos\M', 'dos', 'g'
    );