CREATE TABLE IF NOT EXISTS tb_rastreamento_arquivos (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    nome_arquivo TEXT NOT NULL,
    nome_tabela TEXT NOT NULL,
    ultima_modificacao DATETIME NOT NULL,
    ultimo_processamento DATETIME
)