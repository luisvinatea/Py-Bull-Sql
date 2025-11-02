--DROP VIEW IF EXISTS vw_clientes;
CREATE VIEW vw_clientes AS
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
    LEFT JOIN tb_ordens_rv rv ON positivador.codigo_cliente = rv.codigo_cliente;