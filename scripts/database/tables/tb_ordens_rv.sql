CREATE TABLE IF NOT EXISTS tb_ordens_rv (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    codigo_cliente INTEGER,
    suitability INTEGER,
    codigo_assessor INTEGER,
    matriz TEXT,
    ticker TEXT,
    quantidade INTEGER,
    receita_corretagem REAL,
    volume REAL,
    tipo_produto TEXT,
    canal TEXT,
    tipo_corretagem TEXT,
    mercado TEXT,
    lado TEXT,
    data_ordem TEXT
)