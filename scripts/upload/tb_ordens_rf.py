import os
import pandas as pd
from pathlib import Path
import sqlite3
import logging
from dotenv import load_dotenv
import datetime
from contextlib import contextmanager
import glob

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Carregar variáveis de ambiente
load_dotenv()
logger.info("Variáveis de ambiente carregadas com sucesso.")


@contextmanager
def get_database_connection():
    """Gerenciador de contexto para conexões de banco de dados com limpeza adequada de recursos."""

    # Conexão com SQLite
    db_path = os.getenv("DB_PATH", "data/db/database.db")

    # Garantir que o diretório exista
    db_dir = os.path.dirname(db_path)
    if db_dir and not os.path.exists(db_dir):
        os.makedirs(db_dir)

    conn = None
    try:
        conn = sqlite3.connect(db_path)
        logger.info(
            f"Conexão com o banco de dados SQLite estabelecida: {db_path}"
        )
        yield conn
    except Exception as e:
        logger.error(f"Erro ao conectar com o banco de dados: {e}")
        raise
    finally:
        if conn:
            conn.close()
            logger.info("Conexão com banco de dados fechada.")


class OrdensRFConfig:
    """Classe de configuração para processamento do relatório de ordens de renda fixa."""

    @staticmethod
    def interpret_file_name(file_name):
        """O arquivo de ordens de renda fixa segue o padrão: ordens_rf_YYYYMMDD_DD-MM-YYYY-HH-MM-SS.xlsx, sendo YYYYMMDD a data dos dados e DD-MM-YYYY-HH-MM-SS a data de geração do arquivo. Para nós, a parte relevante é a data dos dados (YYYYMMDD)."""
        try:
            data_dados = file_name.split("_")[2]
            return datetime.datetime.strptime(data_dados, "%Y%m%d").date()
        except Exception as e:
            logger.error(
                f"Erro ao interpretar nome do arquivo {file_name}: {e}"
            )
            return None

    FILE_TO_DB_MAPPING = {"ordens_rf.xlsx": "tb_ordens_rf"}

    @staticmethod
    def get_input_folder():
        """Obtém o caminho da pasta de entrada com lógica de fallback."""
        script_dir = Path(__file__).resolve().parent
        project_root = script_dir.parents[1]

        input_folder = project_root / "data" / "raw"

        if input_folder.exists():
            logger.info(f"Usando pasta de entrada: {input_folder}")
            return input_folder
        else:
            error_msg = (
                f"Arquivo não encontrado na pasta especificada: {input_folder}"
            )
            logger.error(error_msg)
            raise FileNotFoundError(error_msg)


def get_file_last_modified(file_path):
    """Obtém a data de última modificação de um arquivo."""
    try:
        timestamp = file_path.stat().st_mtime
        return datetime.datetime.fromtimestamp(timestamp)
    except Exception as e:
        logger.error(f"Erro ao obter timestamp do arquivo {file_path}: {e}")
        return None


def should_process_file(
    cursor, file_name, table_name, current_modified_time, data_dados
):
    """Verifica se um arquivo deve ser processado com base em sua data de modificação."""
    try:
        cursor.execute(
            "SELECT ultima_modificacao FROM tb_rastreamento_arquivos WHERE nome_arquivo = ?",
            (file_name,),
        )
        result = cursor.fetchone()

        if result is None:
            logger.info(f"Arquivo {file_name} nunca foi processado antes.")
            return True

        last_processed_time = result[0]
        time_diff = abs(
            (current_modified_time - last_processed_time).total_seconds()
        )

        if time_diff > 1:
            logger.info(
                f"Arquivo {file_name} foi modificado desde a última execução."
            )
            return True
        else:
            logger.info(
                f"Arquivo {file_name} não foi modificado. Pulando processamento."
            )
            return False

    except Exception as e:
        logger.error(f"Erro ao verificar status do arquivo {file_name}: {e}")
        return True


def update_file_tracking(cursor, conn, file_name, table_name, modified_time):
    """Atualiza ou insere registro de rastreamento de arquivo."""
    try:
        cursor.execute(
            """UPDATE tb_rastreamento_arquivos 
               SET ultima_modificacao = ?, ultimo_processamento = datetime('now') 
               WHERE nome_arquivo = ?""",
            (modified_time, file_name),
        )

        if cursor.rowcount == 0:
            cursor.execute(
                """INSERT INTO tb_rastreamento_arquivos (nome_arquivo, nome_tabela, ultima_modificacao) 
                   VALUES (?, ?, ?)""",
                (file_name, table_name, modified_time),
            )

        conn.commit()
        logger.info(f"Rastreamento atualizado para {file_name}")

    except Exception as e:
        logger.error(
            f"Erro ao atualizar rastreamento do arquivo {file_name}: {e}"
        )


def get_identity_column(cursor, table_name):
    """Obtém o nome da coluna IDENTITY de uma tabela (SQLite usa AUTOINCREMENT)."""
    try:
        # SQLite usa PRAGMA table_info para obter informações da coluna
        query = f"PRAGMA table_info({table_name})"
        cursor.execute(query)
        columns = cursor.fetchall()

        # Procura pela coluna PRIMARY KEY com AUTOINCREMENT (pk=1)
        for col in columns:
            if col[5] == 1:
                identity_col = col[1]
                logger.info(
                    f"Coluna IDENTITY da tabela {table_name}: {identity_col}"
                )
                return identity_col

        logger.info(
            f"Nenhuma coluna IDENTITY encontrada na tabela {table_name}"
        )
        return None
    except Exception as e:
        logger.error(
            f"Erro ao obter coluna IDENTITY da tabela {table_name}: {e}"
        )
        return None


def delete_non_finished_data(cursor, conn):
    """Se data dos dados for diferente do fechamento do mês atual, substituímos os dados do mês atual. Como os relatórios são extraídos em D+2, pode acontecer de no começo do mês termos dados do mês anterior, especificamente se estivermos nos primeiros dois dias úteis do mês. Nesse caso, continuamos atualizando o mês anterior. Uma vez que os dados do mês anterior são finalizados, começamos anexando os dados do mês atual e assim sucessivamente."""
    try:
        current_date = datetime.datetime.now()

        # Calcular próximo mês
        if current_date.month == 12:
            next_month = current_date.replace(
                year=current_date.year + 1,
                month=1,
                day=1,
                hour=0,
                minute=0,
                second=0,
                microsecond=0,
            )
        else:
            next_month = current_date.replace(
                month=current_date.month + 1,
                day=1,
                hour=0,
                minute=0,
                second=0,
                microsecond=0,
            )

        # Calcular início do mês anterior
        if current_date.month == 1:
            previous_month_start = current_date.replace(
                year=current_date.year - 1,
                month=12,
                day=1,
                hour=0,
                minute=0,
                second=0,
                microsecond=0,
            )
        else:
            previous_month_start = current_date.replace(
                month=current_date.month - 1,
                day=1,
                hour=0,
                minute=0,
                second=0,
                microsecond=0,
            )

        delete_query = """
            DELETE FROM tb_ordens_rf
            WHERE data_ordem >= ? AND data_ordem < ?
        """

        cursor.execute(delete_query, (previous_month_start, next_month))
        deleted_count = cursor.rowcount
        conn.commit()

        logger.info(
            f"Removidos {deleted_count:,} registros não concluídos da tabela no banco de dados."
        )
        return True

    except Exception as e:
        logger.error(f"Erro ao remover dados não concluídos: {e}")
        return False


def process_ordens_rf(cursor, conn, df, file_modified_time):
    """Processa dados do arquivo ordends_rf.xlsx."""
    try:
        column_mapping = {
            "Data": "data_ordem",
            "Cód. assessor": "codigo_assessor",
            "Cód. conta": "codigo_cliente",
            "Tipo ativo": "tipo_ativo",
            "Ticker": "ticker",
            "Nome papel": "nome_papel",
            "Indexador": "indexador",
            "Vencimento": "data_vencimento",
            "Tipo operação": "tipo_operacao",
            "Quantidade": "quantidade",
            "Volume": "volume",
            "Receita a dividir": "receita_a_dividir",
            "PU Cliente": "pu_cliente",
            "PU TMR": "pu_tmr",
            "Taxa Cliente": "taxa_cliente",
            "Taxa TMR": "taxa_tmr",
        }

        # Aplica transformações de dados
        for file_col, db_col in column_mapping.items():
            if file_col in df.columns:
                # Remove R$, pontos como separadores de milhar e substitui vírgulas por pontos em colunas numéricas
                if db_col in ["volume", "receita_a_dividir"]:
                    df[file_col] = (
                        df[file_col]
                        .astype(str)
                        .str.replace("R$", "", regex=False)
                        .str.replace(".", "", regex=False)
                        .str.replace(",", ".", regex=False)
                    )
                    df[file_col] = pd.to_numeric(df[file_col], errors="coerce")
                elif db_col in ["codigo_cliente", "quantidade"]:
                    df[file_col] = pd.to_numeric(
                        df[file_col], errors="coerce"
                    ).astype("Int64")
                elif db_col in ["data_ordem", "data_vencimento"]:
                    df[file_col] = pd.to_datetime(
                        df[file_col], errors="coerce"
                    ).dt.date

        # Renomeia colunas para corresponder ao esquema do banco de dados
        df = df.rename(columns=column_mapping)

        # Cria tabela se não existir
        create_table_query = """
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
        """
        cursor.execute(create_table_query)
        conn.commit()
        logger.info("Tabela tb_ordens_rf criada/verificada com sucesso.")

        # Limpa apenas os dados do mês atual antes de inserir novos dados
        if not delete_non_finished_data(cursor, conn):
            return False

        # Prepara dados para inserção
        records_inserted = 0
        for _, row in df.iterrows():
            values = []
            columns = []
            placeholders = []

            # Adiciona todas as colunas mapeadas (usando os NOVOS nomes de coluna após renomear)
            for db_col in column_mapping.values():
                if db_col in df.columns:
                    columns.append(db_col)
                    placeholders.append("?")
                    value = row[db_col]
                    values.append(None if pd.isna(value) else value)

            if columns:
                insert_query = f"""
                INSERT INTO tb_ordens_rf ({", ".join(columns)})
                VALUES ({", ".join(placeholders)})
                """
                cursor.execute(insert_query, values)
                records_inserted += 1

        conn.commit()
        logger.info(
            f"Processamento do relatório de ordens renda fixa concluído: {records_inserted} registros inseridos."
        )
        return True

    except Exception as e:
        logger.error(f"Erro ao processar relatório de ordens renda fixa: {e}")
        conn.rollback()
        return False


def load_excel_file(file_path):
    """Carrega arquivo Excel e retorna DataFrame."""
    try:
        df = pd.read_excel(file_path)
        logger.info(
            f"Arquivo Excel carregado com sucesso: {file_path} ({len(df)} linhas)"
        )
        return df
    except Exception as e:
        logger.error(f"Erro ao carregar arquivo Excel {file_path}: {e}")
        return None


def process_file(
    cursor,
    conn,
    file_name,
    table_name,
    file_path,
    file_modified_time,
):
    """Processa um único arquivo com base em seu tipo."""
    try:
        # Carrega o arquivo Excel
        df = load_excel_file(file_path)
        if df is None:
            return False

        # Processa com base no tipo de arquivo
        if file_name == "ordens_rf.xlsx":
            return process_ordens_rf(cursor, conn, df, file_modified_time)
        else:
            logger.warning(f"Tipo de arquivo não reconhecido: {file_name}")
            return False

    except Exception as e:
        logger.error(f"Erro ao processar arquivo {file_name}: {e}")
        return False


def main():
    """Função principal de execução."""
    try:
        # Obtém pasta de entrada
        input_folder = OrdensRFConfig.get_input_folder()

        # Processa arquivos com conexão de banco de dados
        with get_database_connection() as conn:
            cursor = conn.cursor()

            processed_count = 0
            for (
                file_name,
                table_name,
            ) in OrdensRFConfig.FILE_TO_DB_MAPPING.items():
                # Remove a extensão do nome do arquivo base
                base_name = file_name.replace(".xlsx", "")

                # Procura por arquivos que correspondem ao padrão base_name_*_*.xlsx
                # Exemplo: ordens_rf_20241102_02-11-2024-10-30-45.xlsx
                pattern = str(input_folder / f"{base_name}_*_*.xlsx")
                matching_files = glob.glob(pattern)

                if not matching_files:
                    logger.warning(
                        f"Nenhum arquivo encontrado para o padrão: {pattern}"
                    )
                    continue

                # Ordena por data de modificação (mais recente primeiro)
                matching_files.sort(
                    key=lambda x: os.path.getmtime(x), reverse=True
                )
                file_path = Path(matching_files[0])

                logger.info(f"Arquivo encontrado: {file_path.name}")

                # Verifica data de modificação
                current_modified_time = get_file_last_modified(file_path)
                if current_modified_time is None:
                    logger.error(
                        f"Não foi possível obter timestamp do arquivo {file_path}"
                    )
                    continue

                # Interpreta o nome do arquivo para obter data_dados
                data_dados = OrdensRFConfig.interpret_file_name(file_path.name)

                # Verifica se o arquivo precisa ser processado
                if not should_process_file(
                    cursor,
                    file_path.name,
                    table_name,
                    current_modified_time,
                    data_dados,
                ):
                    continue

                logger.info(f"Processando arquivo modificado: {file_path}")

                # Processa o arquivo
                if process_file(
                    cursor,
                    conn,
                    file_name,
                    table_name,
                    file_path,
                    current_modified_time,
                ):
                    # Atualiza rastreamento
                    update_file_tracking(
                        cursor,
                        conn,
                        file_path.name,
                        table_name,
                        current_modified_time,
                    )
                    processed_count += 1
                    logger.info(
                        f"Arquivo {file_path.name} processado com sucesso."
                    )
                else:
                    logger.error(
                        f"Falha ao processar arquivo {file_path.name}"
                    )

            logger.info(
                f"Processamento concluído. {processed_count} arquivos processados."
            )

    except Exception as e:
        logger.error(f"Erro na execução principal: {e}")
        raise


if __name__ == "__main__":
    main()
