import subprocess
import logging
import pandas as pd
from typing import Optional

logger = logging.getLogger(__name__)

def bulk_insert_bcp(
    df: pd.DataFrame,
    target_table: str,
    db_server_port: str,
    temp_file: str,
    error_log_file: str = 'bcp_error.log',
    use_trusted_connection: bool = False,
    username: Optional[str] = None,
    password: Optional[str] = None,
    batch_num: Optional[int] = None,
    bcp_batch_size: int = 500000,
    separator: str = ';',
    encoding_codepage: str = "65001"
):
    """
    Saves a DataFrame to a temporary CSV and uses the BCP utility 
    to bulk insert it into a SQL Server database.

    Args:
        df: The pandas DataFrame to insert.
        target_table: The full target table name (e.g., "MyDatabase.dbo.MyTable").
        db_server_port: The server and port (e.g., "MyServer,1433").
        temp_file: The path to use for the temporary CSV file.
        error_log_file: The path for the BCP error log.
        use_trusted_connection: If True, uses '-T' (Windows Authentication).
                                If False, username and password are required.
        username: The SQL Server username (required if not using trusted connection).
        password: The SQL Server password (required if not using trusted connection).
        batch_num: An optional batch number, used for logging context.
        bcp_batch_size: The batch size for BCP ('-b' parameter).
        separator: The field separator for the CSV and BCP.
        encoding_codepage: The code page for BCP ('-C' parameter).
    """   
    log_prefix = f"[Batch {batch_num}] " if batch_num is not None else ""

    if df.empty:
        logger.warning(f"{log_prefix}DataFrame is empty. Skipping.")
        return

    try:
        logger.info(f"{log_prefix}Saving {len(df):,} records to {temp_file}...")
        df.to_csv(
            temp_file,
            sep=separator,
            index=False,
            header=False,
            encoding='utf-8'
        )
    except Exception as e:
        logger.error(f"{log_prefix}Error saving temporary CSV file: {e}")
        raise e

    try:
        logger.info(f"{log_prefix}Executing BCP for {target_table}...")
        
        bcp_command = [
            'bcp', target_table, 'in',
            temp_file,
            '-S', db_server_port,
            '-c', 
            '-t', separator,
            '-F', '1',
            '-C', encoding_codepage,
            '-b', str(bcp_batch_size),
            '-e', error_log_file,
        ]

        if use_trusted_connection:
            bcp_command.append('-T')
            logger.info(f"{log_prefix}Using Trusted Connection.")
        elif username and password:
            bcp_command.extend(['-U', username])
            bcp_command.extend(['-P', password])
            logger.info(f"{log_prefix}Using Username/Password for user '{username}'.")
        else:
            raise ValueError(
                "Authentication error: Must provide either "
                "`use_trusted_connection=True` or both `username` and `password`."
            )

        logger.debug(f"{log_prefix}Running BCP command (password redacted)...")
        
        result = subprocess.run(
            bcp_command,
            check=True,
            capture_output=True,
            text=True,
            encoding='utf-8',
            errors='ignore'
        )
        
        logger.info(f"{log_prefix}✅ BCP completed successfully.")
        logger.debug(f"{log_prefix}BCP Output: {result.stdout}")

    except subprocess.CalledProcessError as e:
        logger.error(f"--- BCP ERROR ({log_prefix}Lote {batch_num}) ---")
        safe_command = [part if i-1 != bcp_command.index('-P') else '***' for i, part in enumerate(bcp_command)]
        logger.error(f"BCP command failed: {' '.join(safe_command)}")
        logger.error(f"BCP Stderr: {e.stderr}")
        logger.error(f"BCP Stdout: {e.stdout}")
        logger.error(f"Check the error file: {error_log_file}")
        raise e
    except FileNotFoundError:
        logger.error("BCP command not found. Is 'bcp' in your system's PATH?")
        raise
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        raise