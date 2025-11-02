CREATE TABLE IF NOT EXISTS tb_ordens_rf (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    data_ordem TEXT,
    codigo_assessor TEXT,
    codigo_cliente INTEGER,
    tipo_ativo TEXT,
    ticker TEXT,
    nome_papel TEXT,
    indexador TEXT,
    data_vencimento TEXT,
    tipo_operacao TEXT,
    quantidade INTEGER,
    volume REAL,
    receita_a_dividir REAL,
    pu_cliente REAL,
    pu_tmr REAL,
    taxa_cliente REAL,
    taxa_tmr REAL
)