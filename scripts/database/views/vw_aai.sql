CREATE VIEW vw_aai AS
SELECT
    DATE(t1.data_posicao, 'start of month') AS data_referencia,
    t1.codigo_assessor,
    -- Custodia
    COALESCE(t1.net_em_m, 0) AS net_total,
    -- Custodia por categorias
    COALESCE(t1.net_renda_fixa, 0) as net_renda_fixa,
    COALESCE(t1.net_fundos_imobiliarios, 0) as net_fundos_imobiliarios,
    COALESCE(t1.net_renda_variavel, 0) as net_renda_variavel,
    COALESCE(t1.net_fundos, 0) as net_fundos,
    COALESCE(t1.net_financeiro, 0) as net_financeiro,
    COALESCE(t1.net_previdencia, 0) as net_previdencia,
    COALESCE(t1.net_outros, 0) as net_outros,
    -- Captação
    COALESCE(t1.captacao_bruta_em_m, 0) AS captacao_bruta_total,
    COALESCE(t1.resgate_em_m, 0) AS resgate_total,
    COALESCE(t1.captacao_liquida_em_m, 0) AS captacao_liquida_total,
    -- Captação por canais
    COALESCE(t1.captacao_ted, 0) AS captacao_ted_total,
    COALESCE(t1.captacao_st, 0) AS captacao_st_total,
    COALESCE(t1.captacao_ota, 0) AS captacao_ota_total,
    COALESCE(t1.captacao_rf, 0) AS captacao_rf_total,
    COALESCE(t1.captacao_td, 0) AS captacao_td_total,
    COALESCE(t1.captacao_prev, 0) AS captacao_prev_total,
    -- Receita
    COALESCE(t1.receita_no_mes, 0) AS receita_bruta_total,
    -- Receita por categorias
    COALESCE(t1.receita_bovespa, 0) AS receita_bovespa_total,
    COALESCE(t1.receita_futuros, 0) AS receita_futuros_total,
    COALESCE(t1.receita_rf_bancarios, 0) AS receita_rf_bancarios_total,
    COALESCE(t1.receita_rf_privados, 0) AS receita_rf_privados_total,
    COALESCE(t1.receita_rf_publicos, 0) AS receita_rf_publicos_total,
    COALESCE(t1.receita_aluguel, 0) AS receita_aluguel_total,
    COALESCE(t1.receita_complemento_pacote_corretagem, 0) AS receita_complemento_total,
    -- Clientes
    COALESCE(
        COUNT(
            CASE
                WHEN t1.ativou_em_m = 'Sim' THEN 1
                ELSE 0
            END
        ),
        0
    ) AS clientes_novos,
    COALESCE(
        COUNT(
            CASE
                WHEN t1.evadiu_em_m = 'Sim' THEN 1
                ELSE 0
            END
        ),
        0
    ) AS clientes_perdidos,
    COALESCE(
        COUNT(
            CASE
                WHEN t1.sexo IS NULL THEN 1
                ELSE 0
            END
        ),
        0
    ) AS clientes_pj,
    COALESCE(
        COUNT(
            CASE
                WHEN t1.sexo IS NOT NULL THEN 1
                ELSE 0
            END
        ),
        0
    ) AS clientes_pf,
    -- Ordens
    COALESCE(SUM(t2.volume), 0) AS volume_operado_rf,
    COALESCE(SUM(t3.volume), 0) AS volume_operado_rv,
    -- Saldo Clientes
    COALESCE(SUM(t4.saldo_total), 0) AS saldo_clientes_total,
    COALESCE(AVG(t4.saldo_total), 0) AS saldo_cliente_medio
FROM
    tb_positivador t1
    LEFT JOIN tb_ordens_rf t2 ON t1.codigo_assessor = t2.codigo_assessor
    AND DATE(t1.data_posicao, 'start of month') = DATE(t2.data_ordem, 'start of month')
    LEFT JOIN tb_ordens_rv t3 ON t1.codigo_assessor = t3.codigo_assessor
    AND DATE(t1.data_posicao, 'start of month') = DATE(t3.data_ordem, 'start of month')
    LEFT JOIN tb_saldo t4 ON t1.codigo_assessor = t4.codigo_assessor
    AND DATE(t1.data_posicao, 'start of month') = DATE(t4.data_saldo, 'start of month')
GROUP BY
    DATE(t1.data_posicao, 'start of month'),
    t1.codigo_assessor;