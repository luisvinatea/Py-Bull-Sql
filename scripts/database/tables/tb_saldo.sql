CREATE TABLE IF NOT EXISTS tb_saldo (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    codigo_cliente INTEGER,
    nome_cliente TEXT,
    codigo_assessor TEXT,
    d0 REAL,
    d1 REAL,
    d2 REAL,
    d3 REAL,
    saldo_total REAL,
    data_saldo TEXT
)