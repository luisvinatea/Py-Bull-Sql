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


class SaldoConfig:
    """Classe de configuração para processamento do relatório de saldo."""

    @staticmethod
    def interpret_file_name(file_name):
        """O arquivo de saldo segue o padrão: saldo_YYYYMMDD_DD-MM-YYYY-HH-MM-SS.xlsx, sendo YYYYMMDD a data dos dados e DD-MM-YYYY-HH-MM-SS a data de geração do arquivo. Para nós, a parte relevante é a data dos dados (YYYYMMDD)."""
        try:
            data_dados = file_name.split("_")[1]
            return datetime.datetime.strptime(data_dados, "%Y%m%d").date()
        except Exception as e:
            logger.error(
                f"Erro ao interpretar nome do arquivo {file_name}: {e}"
            )
            return None

    FILE_TO_DB_MAPPING = {"saldo.xlsx": "tb_saldo"}

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
            cursor.execute(
                "SELECT data_saldo FROM tb_saldo WHERE data_saldo = ?",
                (data_dados,),
            )
            if data_dados is not None and cursor.fetchone() is not None:
                logger.info(
                    f"Dados para a data {data_dados} já existem na tabela. Pulando processamento."
                )
                return False
            return True

        last_processed_time = result[0]

        # Converte string para datetime se necessário
        if isinstance(last_processed_time, str):
            # Tentar parsing com microsegundos primeiro, depois sem
            try:
                last_processed_time = datetime.datetime.strptime(
                    last_processed_time, "%Y-%m-%d %H:%M:%S.%f"
                )
            except ValueError:
                last_processed_time = datetime.datetime.strptime(
                    last_processed_time, "%Y-%m-%d %H:%M:%S"
                )

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
                """INSERT INTO tb_rastreamento_arquivos (nome_arquivo, nome_tabela, ultima_modificacao, ultimo_processamento) 
                   VALUES (?, ?, ?, datetime('now'))""",
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


def delete_non_finished_data(cursor, conn, data_dados):
    """Se data dos dados for diferente do fechamento do mês atual, substituímos os dados do mês atual. Como os relatórios são extraídos em D+2, pode acontecer de no começo do mês termos dados do mês anterior, especificamente se estivermos nos primeiros dois dias úteis do mês. Nesse caso, continuamos atualizando o mês anterior. Uma vez que os dados do mês anterior são finalizados, começamos anexando os dados do mês atual e assim sucessivamente."""
    try:
        # Use data_dados to determine which month to delete
        if data_dados is None:
            logger.warning("data_dados is None, usando data atual")
            data_date = datetime.datetime.now()
        else:
            # Convert date to datetime
            data_date = datetime.datetime.combine(
                data_dados, datetime.time.min
            )

        # Calcular início do mês dos dados
        month_start = data_date.replace(
            day=1,
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        )

        # Calcular início do próximo mês
        if data_date.month == 12:
            next_month = data_date.replace(
                year=data_date.year + 1,
                month=1,
                day=1,
                hour=0,
                minute=0,
                second=0,
                microsecond=0,
            )
        else:
            next_month = data_date.replace(
                month=data_date.month + 1,
                day=1,
                hour=0,
                minute=0,
                second=0,
                microsecond=0,
            )

        delete_query = """
            DELETE FROM tb_saldo 
            WHERE data_saldo >= ? AND data_saldo < ?
        """

        # Converte datetime para string para comparação no SQLite
        month_start_str = month_start.strftime("%Y-%m-%d %H:%M:%S")
        next_month_str = next_month.strftime("%Y-%m-%d %H:%M:%S")

        cursor.execute(delete_query, (month_start_str, next_month_str))
        deleted_count = cursor.rowcount
        conn.commit()

        logger.info(
            f"Removidos {deleted_count:,} registros do mês {data_date.strftime('%Y-%m')} da tabela no banco de dados."
        )
        return True

    except Exception as e:
        logger.error(f"Erro ao remover dados não concluídos: {e}")
        return False


def process_saldo(cursor, conn, df, file_modified_time, data_dados=None):
    """Processa dados do arquivo saldo.xlsx."""
    try:
        column_mapping = {
            "Conta": "codigo_cliente",
            "Cliente": "nome_cliente",
            "Assessor": "codigo_assessor",
            "D0": "d0",
            "D+1": "d1",
            "D+2": "d2",
            "D+3": "d3",
            "Total": "saldo_total",
        }

        # Aplica transformações de dados
        for file_col, db_col in column_mapping.items():
            if file_col in df.columns:
                if db_col in [
                    "d0",
                    "d1",
                    "d2",
                    "d3",
                    "saldo_total",
                ]:
                    df[file_col] = pd.to_numeric(df[file_col], errors="coerce")
                elif db_col == "codigo_cliente":
                    df[file_col] = pd.to_numeric(
                        df[file_col], errors="coerce"
                    ).astype("Int64")
                # Converte Código Assessor para string e prefixa com 'A'
                elif db_col == "codigo_assessor":
                    df[file_col] = (
                        df[file_col]
                        .astype(str)
                        .apply(lambda x: f"A{x}" if pd.notnull(x) else x)
                    )

        # Renomeia colunas para corresponder ao esquema do banco de dados
        df = df.rename(columns=column_mapping)

        # Adiciona coluna data_saldo usando data_dados passado como parâmetro
        # Converte data para string para SQLite
        if data_dados:
            data_saldo_str = data_dados.strftime("%Y-%m-%d")
            logger.info(f"data_saldo será definido como: {data_saldo_str}")
        else:
            data_saldo_str = None
            logger.warning("data_dados é None, data_saldo será NULL")
        df["data_saldo"] = data_saldo_str

        # Cria tabela se não existir
        create_table_query = """
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
        """
        cursor.execute(create_table_query)
        conn.commit()
        logger.info("Tabela tb_saldo criada/verificada com sucesso.")

        # Limpa apenas os dados do mês atual antes de inserir novos dados
        if not delete_non_finished_data(cursor, conn, data_dados):
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

                    # Converte pandas Timestamp para string devido a limitações do SQLite
                    if pd.isna(value):
                        values.append(None)
                    elif isinstance(value, (pd.Timestamp, datetime.datetime)):
                        values.append(value.strftime("%Y-%m-%d %H:%M:%S"))
                    else:
                        values.append(value)

            # Adiciona coluna data_saldo que não está no column_mapping
            if "data_saldo" in df.columns:
                columns.append("data_saldo")
                placeholders.append("?")
                values.append(row["data_saldo"])

            if columns:
                insert_query = f"""
                INSERT INTO tb_saldo ({", ".join(columns)})
                VALUES ({", ".join(placeholders)})
                """
                cursor.execute(insert_query, values)
                records_inserted += 1

        conn.commit()
        logger.info(
            f"Processamento do relatório de saldo concluído: {records_inserted} registros inseridos."
        )
        return True

    except Exception as e:
        logger.error(f"Erro ao processar relatório de saldo: {e}")
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
    data_dados=None,
):
    """Processa um único arquivo com base em seu tipo."""
    try:
        # Carrega o arquivo Excel
        df = load_excel_file(file_path)
        if df is None:
            return False

        # Processa com base no tipo de arquivo
        if file_name == "saldo.xlsx":
            return process_saldo(
                cursor, conn, df, file_modified_time, data_dados
            )
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
        input_folder = SaldoConfig.get_input_folder()

        # Processa arquivos com conexão de banco de dados
        with get_database_connection() as conn:
            cursor = conn.cursor()

            processed_count = 0
            for (
                file_name,
                table_name,
            ) in SaldoConfig.FILE_TO_DB_MAPPING.items():
                # Remove a extensão do nome do arquivo base
                base_name = file_name.replace(".xlsx", "")

                # Procura por arquivos que correspondem ao padrão base_name_*_*.xlsx
                # Exemplo: saldo_20241102_02-11-2024-10-30-45.xlsx
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
                data_dados = SaldoConfig.interpret_file_name(file_path.name)

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
                    data_dados,
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
