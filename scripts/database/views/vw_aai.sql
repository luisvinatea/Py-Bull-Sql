--DROP VIEW IF EXISTS vw_aai;
CREATE VIEW vw_aai AS
WITH
    positivador_agg AS (
        SELECT
            DATE(data_posicao, 'start of month') AS data_referencia,
            codigo_assessor,
            -- Custodia
            SUM(COALESCE(net_em_m, 0)) AS net_total,
            -- Custodia por categorias
            SUM(COALESCE(net_renda_fixa, 0)) AS net_renda_fixa,
            SUM(COALESCE(net_fundos_imobiliarios, 0)) AS net_fundos_imobiliarios,
            SUM(COALESCE(net_renda_variavel, 0)) AS net_renda_variavel,
            SUM(COALESCE(net_fundos, 0)) AS net_fundos,
            SUM(COALESCE(net_financeiro, 0)) AS net_financeiro,
            SUM(COALESCE(net_previdencia, 0)) AS net_previdencia,
            SUM(COALESCE(net_outros, 0)) AS net_outros,
            -- Captação
            SUM(COALESCE(captacao_bruta_em_m, 0)) AS captacao_bruta_total,
            SUM(COALESCE(resgate_em_m, 0)) AS resgate_total,
            SUM(COALESCE(captacao_liquida_em_m, 0)) AS captacao_liquida_total,
            -- Captação por canais
            SUM(COALESCE(captacao_ted, 0)) AS captacao_ted_total,
            SUM(COALESCE(captacao_st, 0)) AS captacao_st_total,
            SUM(COALESCE(captacao_ota, 0)) AS captacao_ota_total,
            SUM(COALESCE(captacao_rf, 0)) AS captacao_rf_total,
            SUM(COALESCE(captacao_td, 0)) AS captacao_td_total,
            SUM(COALESCE(captacao_prev, 0)) AS captacao_prev_total,
            -- Receita
            SUM(COALESCE(receita_no_mes, 0)) AS receita_bruta_total,
            -- Receita por categorias
            SUM(COALESCE(receita_bovespa, 0)) AS receita_bovespa_total,
            SUM(COALESCE(receita_futuros, 0)) AS receita_futuros_total,
            SUM(COALESCE(receita_rf_bancarios, 0)) AS receita_rf_bancarios_total,
            SUM(COALESCE(receita_rf_privados, 0)) AS receita_rf_privados_total,
            SUM(COALESCE(receita_rf_publicos, 0)) AS receita_rf_publicos_total,
            SUM(COALESCE(receita_aluguel, 0)) AS receita_aluguel_total,
            SUM(
                COALESCE(receita_complemento_pacote_corretagem, 0)
            ) AS receita_complemento_total,
            -- Clientes
            SUM(
                CASE
                    WHEN ativou_em_m = 'Sim' THEN 1
                    ELSE 0
                END
            ) AS clientes_novos,
            SUM(
                CASE
                    WHEN evadiu_em_m = 'Sim' THEN 1
                    ELSE 0
                END
            ) AS clientes_perdidos,
            SUM(
                CASE
                    WHEN sexo IS NULL THEN 1
                    ELSE 0
                END
            ) AS clientes_pj,
            SUM(
                CASE
                    WHEN sexo IS NOT NULL THEN 1
                    ELSE 0
                END
            ) AS clientes_pf
        FROM
            tb_positivador
        GROUP BY
            DATE(data_posicao, 'start of month'),
            codigo_assessor
    ),
    ordens_rf_agg AS (
        SELECT
            DATE(data_ordem, 'start of month') AS data_referencia,
            codigo_assessor,
            SUM(COALESCE(volume, 0)) AS volume_operado_rf
        FROM
            tb_ordens_rf
        GROUP BY
            DATE(data_ordem, 'start of month'),
            codigo_assessor
    ),
    ordens_rv_agg AS (
        SELECT
            DATE(data_ordem, 'start of month') AS data_referencia,
            codigo_assessor,
            SUM(COALESCE(volume, 0)) AS volume_operado_rv
        FROM
            tb_ordens_rv
        GROUP BY
            DATE(data_ordem, 'start of month'),
            codigo_assessor
    ),
    saldo_agg AS (
        SELECT
            DATE(data_saldo, 'start of month') AS data_referencia,
            codigo_assessor,
            SUM(COALESCE(saldo_total, 0)) AS saldo_clientes_total,
            AVG(COALESCE(saldo_total, 0)) AS saldo_cliente_medio
        FROM
            tb_saldo
        GROUP BY
            DATE(data_saldo, 'start of month'),
            codigo_assessor
    )
SELECT
    p.data_referencia,
    p.codigo_assessor,
    -- Custodia
    p.net_total,
    p.net_renda_fixa,
    p.net_fundos_imobiliarios,
    p.net_renda_variavel,
    p.net_fundos,
    p.net_financeiro,
    p.net_previdencia,
    p.net_outros,
    -- Captação
    p.captacao_bruta_total,
    p.resgate_total,
    p.captacao_liquida_total,
    p.captacao_ted_total,
    p.captacao_st_total,
    p.captacao_ota_total,
    p.captacao_rf_total,
    p.captacao_td_total,
    p.captacao_prev_total,
    -- Receita
    p.receita_bruta_total,
    p.receita_bovespa_total,
    p.receita_futuros_total,
    p.receita_rf_bancarios_total,
    p.receita_rf_privados_total,
    p.receita_rf_publicos_total,
    p.receita_aluguel_total,
    p.receita_complemento_total,
    -- Clientes
    p.clientes_novos,
    p.clientes_perdidos,
    p.clientes_pj,
    p.clientes_pf,
    -- Ordens
    COALESCE(rf.volume_operado_rf, 0) AS volume_operado_rf,
    COALESCE(rv.volume_operado_rv, 0) AS volume_operado_rv,
    -- Saldo Clientes
    COALESCE(s.saldo_clientes_total, 0) AS saldo_clientes_total,
    COALESCE(s.saldo_cliente_medio, 0) AS saldo_cliente_medio
FROM
    positivador_agg p
    LEFT JOIN ordens_rf_agg rf ON p.data_referencia = rf.data_referencia
    AND p.codigo_assessor = rf.codigo_assessor
    LEFT JOIN ordens_rv_agg rv ON p.data_referencia = rv.data_referencia
    AND p.codigo_assessor = rv.codigo_assessor
    LEFT JOIN saldo_agg s ON p.data_referencia = s.data_referencia
    AND p.codigo_assessor = s.codigo_assessor;