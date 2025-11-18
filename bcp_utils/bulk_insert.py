import subprocess
import logging
import os
import pandas as pd
import numpy as np
from typing import Optional, Dict, Any

from .converters import BCP_CONVERTER_MAP
from .xml_builder import generate_bcp_xml

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
        if '-P' in bcp_command:
            safe_command = bcp_command.copy()
            p_idx = safe_command.index('-P')
            if p_idx + 1 < len(safe_command):
                safe_command[p_idx + 1] = '***'
        else:
            safe_command = bcp_command

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
    
    
def bulk_insert_bcp_native(
    df: pd.DataFrame,
    table_schema: Dict[str, Dict[str, Any]],
    target_table: str,
    db_server_port: str,
    temp_file_base: str,
    error_log_file: str = 'bcp_error.log',
    use_trusted_connection: bool = False,
    username: Optional[str] = None,
    password: Optional[str] = None,
    batch_num: Optional[int] = None,
    bcp_batch_size: int = 500000,
    cleanup_temp_files: bool = False,
    trust_server_certificate: bool = True
):
    """
        Saves a DataFrame to native BCP format (.dat) and uses an
        XML format file (.xml) to bulk insert it into a SQL Server database.

        Args:
            df: The pandas DataFrame to insert.
            table_schema: A dictionary defining the table columns, types, and lengths.
                        (e.g., {'col1': {'type': 'INT'}, 'col2': {'type': 'VARCHAR', 'max_length': 100}})
            target_table: The full target table name (e.g., "MyDatabase.dbo.MyTable").
            db_server_port: The server and port (e.g., "MyServer,1433").
            temp_file_base: The base path for temp files (e.g., "C:\\temp\\my_batch").
                            This will create "my_batch.dat" and "my_batch.xml".
            error_log_file: The path for the BCP error log.
            use_trusted_connection: If True, uses '-T' (Windows Authentication).
                                    If False, username and password are required.
            username: The SQL Server username (required if not using trusted connection).
            password: The SQL Server password (required if not using trusted connection).
            batch_num: An optional batch number, used for logging context.
            bcp_batch_size: The batch size for BCP ('-b' parameter).
            cleanup_temp_files: If True, deletes the temporary .dat and .xml files
                                after the command finishes. Defaults to False,
                                which is safer for debugging.
    """
    log_prefix = f"[Batch {batch_num}] " if batch_num is not None else ""

    if df.empty:
        logger.warning(f"{log_prefix}DataFrame is empty. Skipping.")
        return

    dat_file = temp_file_base + ".dat"
    xml_format_file = temp_file_base + ".xml"

    try:
        logger.info(f"{log_prefix}Generating {xml_format_file}...")
        xml_content = generate_bcp_xml(table_schema)
        with open(xml_format_file, 'w', encoding='utf-8') as f:
            f.write(xml_content)
    except Exception as e:
        logger.error(f"{log_prefix}Error generating XML format file: {e}")
        raise e

    try:
        logger.info(f"{log_prefix}Converting {len(df):,} records to native format...")
        
        converted_column_arrays = []
        
        for col_name, info in table_schema.items():
            
            sql_type = info['type'].upper()
            converter_func = BCP_CONVERTER_MAP.get(sql_type)
            
            if not converter_func:
                raise ValueError(f"No BCP converter found for SQL type: {sql_type}")
            
            if col_name not in df.columns:
                 raise ValueError(f"Column '{col_name}' from schema not found in DataFrame.")
            
            result_array = converter_func(df[col_name])
            result_array_as_object = result_array.astype(object)
            
            converted_column_arrays.append(
                result_array_as_object.reshape(-1, 1)
            )

        logger.info(f"{log_prefix}Building converted numpy array...")
        bcp_array = np.hstack(converted_column_arrays)

        logger.info(f"{log_prefix}Concatenating byte streams...")
        final_byte_stream = b''.join(bcp_array.ravel())
        
        logger.info(f"{log_prefix}Saving native data to {dat_file}...")
        with open(dat_file, 'wb') as f:
            f.write(final_byte_stream)

    except Exception as e:
        logger.error(f"{log_prefix}Error creating native .dat file: {e}")
        raise e

    try:
        logger.info(f"{log_prefix}Executing BCP for {target_table}...")

        bcp_command = [
            'bcp', target_table, 'in',
            dat_file,
            '-S', db_server_port,
            '-f', xml_format_file,
            '-F', '1',
            '-b', str(bcp_batch_size),
            '-e', error_log_file,
        ]
        
        if trust_server_certificate:
            bcp_command.append('-u')

        if use_trusted_connection:
            bcp_command.append('-T')
        elif username and password:
            bcp_command.extend(['-U', username, '-P', password])
        else:
            raise ValueError("Authentication error: Must provide trusted connection or user/pass.")

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

        if '-P' in bcp_command:
            safe_command = bcp_command.copy()
            p_idx = safe_command.index('-P')
            if p_idx + 1 < len(safe_command):
                safe_command[p_idx + 1] = '***'
        else:
            safe_command = bcp_command

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
    finally:
        if cleanup_temp_files:
            try:
                if os.path.exists(dat_file):
                    os.remove(dat_file)
                if os.path.exists(xml_format_file):
                    os.remove(xml_format_file)
                logger.debug(f"{log_prefix}Cleaned up temp files.")
            except Exception as e:
                logger.warning(f"{log_prefix}Could not clean up temp files: {e}")