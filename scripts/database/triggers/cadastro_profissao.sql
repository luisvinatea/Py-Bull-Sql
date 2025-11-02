DROP TRIGGER IF EXISTS trg_cadastro_profissao;

-- Trigger para tb_positivador
CREATE TRIGGER IF NOT EXISTS trg_cadastro_profissao AFTER INSERT ON tb_positivador FOR EACH ROW WHEN NEW.profissao IS NOT NULL
AND NEW.profissao NOT IN (
    SELECT
        profissao
    FROM
        tb_profissao
) BEGIN
INSERT INTO
    tb_profissao (profissao)
VALUES
    (NEW.profissao);

END;