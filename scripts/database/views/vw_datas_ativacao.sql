/*CREATE OR ALTER VIEW view_datas_ativacao AS*/
WITH
    -- Passo 1: Usar funções de janela em vez de subconsultas correlacionadas
    net_captacao_window AS (
        SELECT
            codigo_cliente,
            data_referencia,
            captacao_liquida_em_m,
            -- Encontrar o último ponto de reset usando função de janela
            MAX(
                CASE
                    WHEN captacao_liquida_em_m >= 300000 THEN data_referencia
                END
            ) OVER (
                PARTITION BY
                    codigo_cliente
                ORDER BY
                    data_referencia ROWS UNBOUNDED PRECEDING
            ) AS last_reset_date,
            -- Calcular soma acumulada com lógica de reset apropriada
            CASE
                WHEN captacao_liquida_em_m >= 300000 THEN captacao_liquida_em_m
                ELSE SUM(captacao_liquida_em_m) OVER (
                    PARTITION BY
                        codigo_cliente
                    ORDER BY
                        data_referencia ROWS UNBOUNDED PRECEDING
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
    -- Passo 4: Filtrar ativações baseadas em data_cadastro dentro de janela de 12 meses
    ativacao_300k_filtered AS (
        SELECT
            a300.codigo_cliente,
            a300.data_ativacao_300k
        FROM
            ativacao_300k_dates a300
            INNER JOIN tb_cliente c ON a300.codigo_cliente = c.conta_xp
        WHERE
            c.data_contanova_ativacao >= '2025-01-01'
    ),
    -- Passo 5: Obter dados do cliente com assessor atual
    client_data AS (
        SELECT
            c.id_cliente,
            c.nome_completo AS nome_cliente,
            c.conta_xp,
            c.data_cadastro,
            -- ASSESSOR ATUAL
            tc.codigo_assessor,
            tc.nome_completo AS nome_assessor,
            tc_eq.nome_equipe AS nome_time_assessor,
            ROW_NUMBER() OVER (
                PARTITION BY
                    c.id_cliente
                ORDER BY
                    c.data_cadastro
            ) AS rn
        FROM
            tb_cliente c
            LEFT JOIN tb_funcionario tc ON c.id_colaborador = tc.id_colaborador
            LEFT JOIN tb_equipe tc_eq ON tc.id_time = tc_eq.id_equipe
        WHERE
            c.conta_xp IS NOT NULL
            AND c.nome_completo IS NOT NULL
    )
    -- SELECT final com dados de ativação - APENAS CLIENTES ATIVADOS
SELECT
    cd.conta_xp AS codigo_cliente,
    cd.nome_cliente,
    cd.codigo_assessor,
    cd.nome_assessor,
    cd.nome_time_assessor,
    cd.data_cadastro,
    a300.data_ativacao_300k
FROM
    client_data cd
    INNER JOIN ativacao_300k_filtered a300 ON cd.conta_xp = a300.codigo_cliente
WHERE
    cd.rn = 1
    AND cd.conta_xp IS NOT NULL
    AND cd.nome_cliente IS NOT NULL
    AND a300.data_ativacao_300k IS NOT NULL;