ALTER TABLE s_novaxs_182
ALTER COLUMN dt_nascimento
TYPE DATE
USING
    (
        CASE
            WHEN TO_DATE(dt_nascimento, 'DD/MM/YY') > CURRENT_DATE
                THEN TO_DATE(dt_nascimento, 'DD/MM/YY') - INTERVAL '100 years'
            ELSE TO_DATE(dt_nascimento, 'DD/MM/YY')
        END
    )::DATE;

--caso não altere o tipo:
ALTER TABLE s_novaxs_182
ALTER COLUMN dt_nascimento
TYPE DATE
USING dt_nascimento::DATE;
