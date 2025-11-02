DROP VIEW IF EXISTS vw_clientes;
CREATE VIEW vw_clientes AS
WITH
    -- Passo 1: Usar funções de janela em vez de subconsultas correlacionadas
    net_captacao_window AS (
        SELECT
            codigo_cliente,
            DATE(data_posicao, 'start of month') AS data_referencia,
            captacao_liquida_em_m,
            -- Encontrar o último ponto de reset usando função de janela
            MAX(
                CASE
                    WHEN captacao_liquida_em_m >= 300000 THEN DATE(data_posicao, 'start of month')
                END
            ) OVER (
                PARTITION BY
                    codigo_cliente
                ORDER BY
                    DATE(data_posicao, 'start of month') ROWS UNBOUNDED PRECEDING
            ) AS last_reset_date,
            -- Calcular soma acumulada com lógica de reset apropriada
            CASE
                WHEN captacao_liquida_em_m >= 300000 THEN captacao_liquida_em_m
                ELSE SUM(captacao_liquida_em_m) OVER (
                    PARTITION BY
                        codigo_cliente
                    ORDER BY
                        DATE(data_posicao, 'start of month') ROWS UNBOUNDED PRECEDING
                )
            END AS captacao_liq_acumulada_temp
        FROM
            tb_positivador
        WHERE
            captacao_liquida_em_m IS NOT NULL
            AND codigo_cliente IS NOT NULL
    ),
    -- Passo 2: Aplicar lógica de reset apropriada
    net_captacao_cumulative AS (
        SELECT
            codigo_cliente,
            data_referencia,
            captacao_liquida_em_m,
            CASE
                WHEN captacao_liquida_em_m >= 300000 THEN captacao_liquida_em_m
                ELSE SUM(
                    CASE
                        WHEN last_reset_date IS NULL
                        OR data_referencia > last_reset_date THEN captacao_liquida_em_m
                        ELSE 0
                    END
                ) OVER (
                    PARTITION BY
                        codigo_cliente
                    ORDER BY
                        data_referencia ROWS UNBOUNDED PRECEDING
                )
            END AS captacao_liq_acumulada
        FROM
            net_captacao_window
    ),
    -- Passo 3: Encontrar a primeira data em que cada conta atingiu 300K (spike único ou acumulado até reset)
    ativacao_300k_dates AS (
        SELECT
            codigo_cliente,
            MIN(data_referencia) AS data_ativacao_300k
        FROM
            net_captacao_cumulative
        WHERE
            captacao_liq_acumulada >= 300000
        GROUP BY
            codigo_cliente
    ),
    -- Passo 5: Obter dados do cliente com assessor atual
    client_data AS (
        SELECT DISTINCT
            positivador.codigo_cliente,
            saldo.nome_cliente,
            positivador.profissao,
            positivador.sexo,
            positivador.segmento,
            DATE(positivador.data_nascimento) AS data_nascimento,
            rv.suitability,
            DATE(positivador.data_cadastro, 'start of month') AS data_cadastro,
            CASE
                WHEN positivador.ativou_em_m = 'Sim' THEN DATE(positivador.data_posicao, 'start of month')
                ELSE NULL
            END AS data_ativacao,
            CASE
                WHEN positivador.evadiu_em_m = 'Sim' THEN DATE(positivador.data_posicao, 'start of month')
                ELSE NULL
            END AS data_evasao,
            positivador.aplicacao_financeira_declarada_ajustada AS patrimonio_declarado,
            CASE
                WHEN tipo_pessoa = 'PESSOA JURÍDICA' THEN 'PJ'
                ELSE 'PF'
            END AS tipo_pessoa,
            positivador.codigo_assessor
        FROM
            tb_positivador positivador
            LEFT JOIN tb_saldo saldo ON positivador.codigo_cliente = saldo.codigo_cliente
            LEFT JOIN tb_ordens_rv rv ON positivador.codigo_cliente = rv.codigo_cliente
    )
    -- SELECT final
SELECT
    cd.codigo_cliente,
    cd.nome_cliente,
    cd.profissao,
    cd.sexo,
    cd.segmento,
    cd.data_nascimento,
    cd.suitability,
    cd.data_cadastro,
    cd.data_ativacao,
    a300.data_ativacao_300k,
    cd.data_evasao,
    cd.patrimonio_declarado,
    cd.tipo_pessoa,
    cd.codigo_assessor
FROM
    client_data cd
    LEFT JOIN ativacao_300k_dates a300 ON cd.codigo_cliente = a300.codigo_cliente
WHERE
    cd.codigo_cliente IS NOT NULL
    AND cd.nome_cliente IS NOT NULL;