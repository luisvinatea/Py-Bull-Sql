"""
Itera sobre todas as tabelas do banco de dados e cria backups no formato Excel, o caminho de saída é "data/backups/nome_da_tabela (extraída do information.schema)/ano (extraída da coluna de data)/mês (extraída da coluna de data)/nome_da_tabela_yyyy_mm_01.xlsx".
"""

import sys
import pandas as pd
import sqlite3
import logging
from pathlib import Path
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def check_requirements():
    """Verifica se as dependências necessárias estão disponíveis"""
    required_packages = ["pandas", "openpyxl"]
    missing_packages = []

    for package in required_packages:
        try:
            __import__(package)
        except ImportError:
            missing_packages.append(package)

    if missing_packages:
        logger.error(
            f"Pacotes necessários não encontrados: {', '.join(missing_packages)}"
        )
        logger.error("Execute: pip install pandas openpyxl")
        sys.exit(1)


def get_database_path() -> Path:
    """Obtém o caminho do banco de dados SQLite"""
    script_dir = Path(__file__).resolve()
    project_root = script_dir.parents[2]
    db_path = project_root / "data" / "db" / "database.db"

    if not db_path.exists():
        error_msg = f"Banco de dados não encontrado: {db_path}"
        logger.error(error_msg)
        raise FileNotFoundError(error_msg)

    logger.info(f"Banco de dados encontrado: {db_path}")
    return db_path


def get_table_date_column(table_name: str) -> str:
    """Identifica a coluna de data para cada tabela"""
    date_column_mapping = {
        "tb_positivador": "data_posicao",
        "tb_ordens_rf": "data_ordem",
        "tb_ordens_rv": "data_ordem",
        "tb_saldo": "data_saldo",
    }
    return date_column_mapping.get(table_name)


def extract_table_from_database(
    db_path: Path, table_name: str
) -> pd.DataFrame:
    """Extrai uma tabela do banco de dados SQLite"""
    try:
        conn = sqlite3.connect(db_path)
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql_query(query, conn)
        conn.close()
        logger.info(f"Tabela '{table_name}' extraída com {len(df)} linhas")
        return df
    except Exception as e:
        logger.error(f"Erro ao extrair tabela '{table_name}': {e}")
        return None


def get_all_tables(db_path: Path) -> list:
    """Obtém lista de todas as tabelas do banco de dados"""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        )
        tables = [row[0] for row in cursor.fetchall()]
        conn.close()
        logger.info(f"Encontradas {len(tables)} tabelas: {', '.join(tables)}")
        return tables
    except Exception as e:
        logger.error(f"Erro ao listar tabelas: {e}")
        return []


def save_table_to_excel(df: pd.DataFrame, output_file: Path, table_name: str):
    """Salva um DataFrame em Excel com formatação apropriada"""
    try:
        with pd.ExcelWriter(output_file, engine="xlsxwriter") as writer:
            df.to_excel(writer, index=False, sheet_name="Sheet1")

            workbook = writer.book
            worksheet = writer.sheets["Sheet1"]
            text_format = workbook.add_format({"num_format": "@"})

            # formatação de colunas com valores numéricos muito grandes
            for idx, col in enumerate(df.columns):
                if df[col].dtype == "object":
                    sample_values = df[col].dropna().head(100)
                    if len(sample_values) > 0:
                        try:
                            numeric_check = pd.to_numeric(
                                sample_values, errors="coerce"
                            )
                            max_val = numeric_check.abs().max()
                            if pd.notna(max_val) and max_val >= 1e9:
                                worksheet.set_column(
                                    idx, idx, None, text_format
                                )
                        except Exception:
                            pass

        logger.info(f"Arquivo salvo: {output_file} ({len(df)} linhas)")
        return True
    except Exception as e:
        logger.error(f"Erro ao salvar arquivo {output_file}: {e}")
        return False


def extract_and_backup_table(
    db_path: Path, table_name: str, backup_dir: Path
) -> dict:
    """Extrai tabela do banco e cria backup completo e por data"""
    stats = {
        "table_name": table_name,
        "total_rows": 0,
        "full_backup_created": False,
        "files_created": 0,
        "errors": 0,
    }

    # Extrai tabela
    df = extract_table_from_database(db_path, table_name)
    if df is None or len(df) == 0:
        logger.warning(f"Tabela '{table_name}' vazia ou não pôde ser extraída")
        return stats

    stats["total_rows"] = len(df)

    # cria diretório da tabela
    table_dir = backup_dir / table_name
    table_dir.mkdir(parents=True, exist_ok=True)

    # Salva backup completo
    full_backup_file = table_dir / f"{table_name}_backup_completo.xlsx"
    if save_table_to_excel(df, full_backup_file, table_name):
        stats["full_backup_created"] = True
    else:
        stats["errors"] += 1

    # Verifica coluna de data
    date_column = get_table_date_column(table_name)
    if not date_column or date_column not in df.columns:
        logger.info(
            f"Tabela '{table_name}' não possui coluna de data configurada. Apenas backup completo criado."
        )
        return stats

    # Divide por data
    logger.info(
        f"Dividindo tabela '{table_name}' por datas na coluna '{date_column}'"
    )

    # Converte para datetime
    df[date_column] = pd.to_datetime(df[date_column], errors="coerce")
    invalid_dates = df[date_column].isnull().sum()

    if invalid_dates > 0:
        logger.warning(
            f"{invalid_dates} linhas com datas inválidas em '{table_name}'"
        )

    df_clean = df.dropna(subset=[date_column])

    if len(df_clean) == 0:
        logger.warning(f"Nenhuma data válida encontrada em '{table_name}'")
        return stats

    unique_dates = df_clean[date_column].dt.date.unique()
    logger.info(
        f"Encontradas {len(unique_dates)} datas únicas em '{table_name}'"
    )

    for date in sorted(unique_dates):
        date_str = date.strftime("%Y-%m-%d")
        year_str = date.strftime("%Y")
        month_str = date.strftime("%m")

        # Cria estrutura de diretórios ano/mês
        output_dir = table_dir / year_str / month_str
        output_dir.mkdir(parents=True, exist_ok=True)

        output_file = output_dir / f"{table_name}_{date_str}.xlsx"

        df_date = df_clean[df_clean[date_column].dt.date == date].copy()

        # Converte colunas datetime para apenas data
        for col in df_date.columns:
            if pd.api.types.is_datetime64_any_dtype(df_date[col]):
                df_date[col] = df_date[col].dt.date

        if save_table_to_excel(df_date, output_file, table_name):
            stats["files_created"] += 1
        else:
            stats["errors"] += 1

    return stats


def print_summary(all_stats: list):
    """Exibe um resumo das estatísticas do processamento"""
    logger.info("=" * 60)
    logger.info("RESUMO DO PROCESSAMENTO")
    logger.info("=" * 60)

    total_rows = sum(s["total_rows"] for s in all_stats)
    total_files = sum(s["files_created"] for s in all_stats)
    total_errors = sum(s["errors"] for s in all_stats)

    logger.info(f"Total de tabelas processadas: {len(all_stats)}")
    logger.info(f"Total de linhas exportadas: {total_rows:,}")
    logger.info(f"Total de arquivos criados: {total_files:,}")

    for stats in all_stats:
        logger.info(f"\nTabela: {stats['table_name']}")
        logger.info(f"  - Linhas: {stats['total_rows']:,}")
        logger.info(
            f"  - Backup completo: {'Sim' if stats['full_backup_created'] else 'Não'}"
        )
        logger.info(f"  - Arquivos por data: {stats['files_created']:,}")

    if total_errors > 0:
        logger.warning(f"\nErros durante o processamento: {total_errors}")
    else:
        logger.info("\nProcessamento concluído sem erros!")

    logger.info("=" * 60)


def main():
    """Função principal do script"""
    try:
        # Verificar dependências
        check_requirements()

        start_time = datetime.now()
        logger.info(
            f"Iniciando execução em {start_time.strftime('%Y-%m-%d %H:%M:%S')}"
        )

        # Obtém caminho do banco de dados
        db_path = get_database_path()

        # Obtém todas as tabelas
        tables = get_all_tables(db_path)

        if not tables:
            logger.error("Nenhuma tabela encontrada no banco de dados")
            sys.exit(1)

        # Define diretório de backup
        script_dir = Path(__file__).resolve()
        project_root = script_dir.parents[2]
        backup_dir = project_root / "data" / "backups"
        backup_dir.mkdir(parents=True, exist_ok=True)

        # Processa cada tabela
        all_stats = []
        for table in tables:
            logger.info(f"\n{'=' * 60}")
            logger.info(f"Processando tabela: {table}")
            logger.info(f"{'=' * 60}")
            stats = extract_and_backup_table(db_path, table, backup_dir)
            all_stats.append(stats)

        end_time = datetime.now()
        execution_time = end_time - start_time

        print_summary(all_stats)
        logger.info(f"Tempo total de execução: {execution_time}")

        # Retornar código de saída baseado no resultado
        total_errors = sum(s["errors"] for s in all_stats)
        if total_errors > 0:
            logger.warning("Execução finalizada com erros")
            sys.exit(1)
        else:
            logger.info("Execução finalizada com sucesso")
            sys.exit(0)

    except FileNotFoundError as e:
        logger.error(f"Arquivo não encontrado: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        logger.warning("Execução interrompida pelo usuário")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Erro inesperado: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
