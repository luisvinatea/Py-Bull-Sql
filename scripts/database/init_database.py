"""
Script para inicializar o banco de dados SQLite com todas as tabelas necessárias.
"""

import os
import sqlite3
import logging
from pathlib import Path
from dotenv import load_dotenv

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Carregar variáveis de ambiente
load_dotenv()


def get_database_path():
    """Obtém o caminho do banco de dados a partir das variáveis de ambiente."""
    db_path = os.getenv("DB_PATH", "data/db/database.db")

    # Converte para caminho absoluto se for relativo
    if not os.path.isabs(db_path):
        script_dir = Path(__file__).resolve().parent
        project_root = script_dir.parents[1]
        db_path = project_root / db_path

    return db_path


def create_database():
    """Cria o banco de dados SQLite e todas as tabelas necessárias."""

    db_path = get_database_path()

    # Garante que o diretório existe
    db_dir = os.path.dirname(db_path)
    if db_dir and not os.path.exists(db_dir):
        os.makedirs(db_dir)
        logger.info(f"Diretório criado: {db_dir}")

    # Conecta ao banco de dados (será criado se não existir)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    logger.info(f"Conectado ao banco de dados: {db_path}")

    try:
        # Tabela de rastreamento de arquivos
        logger.info("Criando tabela tb_rastreamento_arquivos...")
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS tb_rastreamento_arquivos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nome_arquivo TEXT NOT NULL,
            nome_tabela TEXT NOT NULL,
            ultima_modificacao DATETIME NOT NULL,
            ultimo_processamento DATETIME
        )""")

        # Commit das alterações
        conn.commit()
        logger.info("Todas as tabelas foram criadas com sucesso!")

        # Exibe informações sobre as tabelas criadas
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        )
        tables = cursor.fetchall()
        logger.info(f"\nTabelas no banco de dados ({len(tables)}):")
        for table in tables:
            logger.info(f"  - {table[0]}")

        logger.info(f"\nBanco de dados criado com sucesso em: {db_path}")

    except Exception as e:
        logger.error(f"Erro ao criar banco de dados: {e}")
        conn.rollback()
        raise

    finally:
        conn.close()
        logger.info("Conexão com banco de dados fechada.")


def main():
    """Função principal."""
    try:
        logger.info("=" * 60)
        logger.info("Inicializando banco de dados SQLite")
        logger.info("=" * 60)
        create_database()
        logger.info("=" * 60)
        logger.info("Processo concluído!")
        logger.info("=" * 60)
    except Exception as e:
        logger.error(f"Erro durante a inicialização: {e}")
        raise


if __name__ == "__main__":
    main()
