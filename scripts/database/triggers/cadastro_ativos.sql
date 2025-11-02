DROP TRIGGER IF EXISTS trg_cadastro_ativos_rf;
-- Trigger para tb_ordens_rf
CREATE TRIGGER IF NOT EXISTS trg_cadastro_ativos_rf
AFTER INSERT ON tb_ordens_rf
FOR EACH ROW
WHEN NEW.tipo_ativo IS NOT NULL 
    AND NEW.tipo_ativo NOT IN (SELECT tipo_ativo FROM tb_ativos)
BEGIN
    INSERT INTO tb_ativos (tipo_ativo, categoria)
    VALUES (NEW.tipo_ativo, 'Renda Fixa');
END;

DROP TRIGGER IF EXISTS trg_cadastro_ativos_rv;
-- Trigger para tb_ordens_rv
CREATE TRIGGER IF NOT EXISTS trg_cadastro_ativos_rv
AFTER INSERT ON tb_ordens_rv
FOR EACH ROW
WHEN NEW.tipo_produto IS NOT NULL
    AND NEW.tipo_produto NOT IN (SELECT tipo_ativo FROM tb_ativos)
BEGIN
    INSERT INTO tb_ativos (tipo_ativo, categoria)
    VALUES (NEW.tipo_produto, 'Renda Variável');
END;