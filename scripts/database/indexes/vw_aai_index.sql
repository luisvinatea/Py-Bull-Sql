-- Índices para otimização da view vw_aai
-- tb_positivador
CREATE INDEX IF NOT EXISTS idx_tb_positivador_assessor_data ON tb_positivador (codigo_assessor, data_posicao);

CREATE INDEX IF NOT EXISTS idx_tb_positivador_data_posicao ON tb_positivador (data_posicao);

-- tb_ordens_rf
CREATE INDEX IF NOT EXISTS idx_tb_ordens_rf_assessor_data ON tb_ordens_rf (codigo_assessor, data_ordem);

CREATE INDEX IF NOT EXISTS idx_tb_ordens_rf_data_ordem ON tb_ordens_rf (data_ordem);

-- tb_ordens_rv
CREATE INDEX IF NOT EXISTS idx_tb_ordens_rv_assessor_data ON tb_ordens_rv (codigo_assessor, data_ordem);

CREATE INDEX IF NOT EXISTS idx_tb_ordens_rv_data_ordem ON tb_ordens_rv (data_ordem);

-- tb_saldo
CREATE INDEX IF NOT EXISTS idx_tb_saldo_assessor_data ON tb_saldo (codigo_assessor, data_saldo);

CREATE INDEX IF NOT EXISTS idx_tb_saldo_data_saldo ON tb_saldo (data_saldo);