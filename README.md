# Python BCP Utility (py-bcp-utils)

A Python utility to run the SQL Server `bcp` (Bulk Copy Program) command using a Pandas DataFrame as the source.

This package provides two methods for high-speed bulk inserts:

1. **Standard (CSV):** A simple, wrapper that saves the DataFrame to a temporary CSV and bulk inserts it.
    
2. **Native Format:** An ultra-performant method that converts the DataFrame to `bcp`'s native binary format in memory, bypassing the CSV step for massive speed gains.
    

## Key Features

- Bulk insert `pandas.DataFrame` objects directly into SQL Server.
    
- Bypasses `pd.to_sql()` for significant performance improvements.
    
- **Simple Method (`bulk_insert_bcp`):** Easy-to-use function that relies on a temporary CSV.
    
- **Native Method (`bulk_insert_bcp_native`):** Extremely fast function that writes data directly to SQL Server's native binary format. Ideal for inserting millions of rows.
    
- Supports both SQL Server Authentication (username/password) and Trusted Connections (Windows Authentication).
    

## Requirements

1. **Python 3.8+**
    
2. **`pandas`** & **`numpy`** (will be installed automatically)
    
3. **`bcp` Utility:** The SQL Server `bcp` command-line utility **must be installed on your system** and available in your shell's PATH.
    
    - On Windows, this is typically installed with **SQL Server Management Studio (SSMS)** or the **Microsoft Command Line Utilities for SQL Server**.
        

## 💾 Installation

Once the package is published to the real PyPI, you can install it with:

Bash

```
pip install py-bcp-utils
```

## Usage

You have two functions to choose from, depending on your performance needs.

---

### 1. Standard Insert (Simple, CSV-based)

This is the easiest method. It's reliable and great for most use cases. It works by saving the DataFrame to a temporary CSV file.

Python

```
import pandas as pd
import logging
from bcp_utils import bulk_insert_bcp

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

data = {
    'column1': [1, 2, 3],
    'column2': ['apple', 'banana', 'orange'],
    'column3': [10.5, 20.1, 30.2]
}
df = pd.DataFrame(data)

DB_SERVER = "YourServerName,1433"
DB_TABLE = "YourDatabase.dbo.YourTable"
TEMP_CSV = "temp_bcp_data.csv"
ERROR_LOG = "bcp_error.log"

try:
    logging.info("Attempting insert with SQL Server login (CSV method)...")
    bulk_insert_bcp(
        df=df,
        target_table=DB_TABLE,
        db_server_port=DB_SERVER,
        temp_file=TEMP_CSV,
        error_log_file=ERROR_LOG,
        username="your_sql_user",
        password="your_sql_password"
    )
    logging.info("✅ Successfully inserted data!")

except Exception as e:
    logging.error(f"Data insert failed: {e}")
```

---

### 2. Native Insert (Fastest, Advanced)

This method is significantly faster (potentially 100x+) than the CSV method because it skips the text conversion step. It's ideal for very large DataFrames.

It works by converting the DataFrame to SQL Server's internal binary format (`.dat` file) and generating a corresponding XML format file (`.xml`).

**Note:** This function requires a `table_schema` dictionary so it knows how to convert the data.

Python

```
import pandas as pd
import numpy as np
import logging
from bcp_utils import bulk_insert_bcp_native

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Example data, including nulls
data = {
    'col_a_int': [1, 2, np.nan, 4],
    'col_b_varchar': ['hello', 'world', '!!', np.nan],
    'col_c_date': [pd.Timestamp('2025-01-01'), pd.NaT, pd.Timestamp('2025-01-03'), pd.Timestamp('2025-01-04')],
    'col_d_float': [1.23, 4.56, 7.89, np.nan]
}
df = pd.DataFrame(data)

# You MUST define a schema that matches your SQL table
# This is required for the native binary conversion
my_schema = {
    'col_a_int': {'type': 'INT'},
    'col_b_varchar': {'type': 'VARCHAR', 'max_length': '100'},
    'col_c_date': {'type': 'DATE'},
    'col_d_float': {'type': 'FLOAT'}
}

DB_SERVER = "YourServerName,1433"
DB_TABLE = "YourDatabase.dbo.YourTable"
TEMP_BASE = "temp_native_batch"  # Will create .dat and .xml files
ERROR_LOG = "bcp_error.log"

try:
    logging.info("Attempting insert with Trusted Connection (NATIVE method)...")
    bulk_insert_bcp_native(
        df=df,
        table_schema=my_schema,
        target_table=DB_TABLE,
        db_server_port=DB_SERVER,
        temp_file_base=TEMP_BASE,
        error_log_file=ERROR_LOG,
        use_trusted_connection=True,
        cleanup_temp_files=False  # Set to True in production
    )
    logging.info("✅ Successfully inserted data using NATIVE format!")

except Exception as e:
    logging.error(f"Data insert failed: {e}")
```

## ⚡ Native Format Supported Types

The high-performance `bulk_insert_bcp_native` function currently supports the following SQL Server data types. You must ensure the `type` specified in your `table_schema` dictionary matches one of the following strings (case-insensitive):

- **Integer Types:**
    
    - `BIGINT`
        
    - `INT`
        
    - `SMALLINT`
        
    - `TINYINT`
        
    - `BIT` (converts `True`/`False`/`None`)
        
- **Floating-Point Types:**
    
    - `FLOAT` (SQL `FLOAT(53)`)
        
    - `REAL` (SQL `FLOAT(24)`)
        
- **String Types:**
    
    - `VARCHAR`
        
    - `NVARCHAR`
        
- **Date/Time Types:**
    
    - **`DATE`**: Accepts a pandas `datetime` column but **truncates the time component**. Intended for SQL `DATE` columns.
        
    - **`DATETIME2`**: Accepts a pandas `datetime` column and **preserves the full timestamp** (up to 100ns precision). Intended for SQL `DATETIME2(7)` columns.
        
- **Geometry Types:**
    
    - `GEOMETRY`
        
Support for other types (like `DECIMAL`/`NUMERIC`) will be added in future versions.

### Geometry columns (`GEOMETRY`)

For columns of type `GEOMETRY`, this library **does not** convert WKT or other textual
representations to WKB. It expects that the DataFrame already contains the binary
representation (WKB / varbinary) of the geometry.

Supported input values in the DataFrame (per row):

- `bytes` / `bytearray` / `memoryview` containing the geometry WKB, for example:
  the result of `SELECT geom.STAsBinary()` in SQL Server.
- `str` containing the hexadecimal representation of the WKB, for example:
  - `"0xE61000000104..."`  
  - `"E61000000104..."` (with or without the `0x` prefix, with or without whitespace)

**Not supported:**

- WKT strings like `"POLYGON (...)"`, `"POINT (...)"`, etc.
- GeoJSON or any other textual format.

During conversion, the library generates a native BCP payload with a 4-byte length prefix:

- Non-null row:
  - `[4-byte little-endian length N][N bytes of WKB]`
- Null row:
  - `[4-byte little-endian -1]` (`0xFF FF FF FF`)

Your `table_schema` should declare the geometry column as:

```python
table_schema = {
    "col_geometry": {"type": "GEOMETRY"},
    }
```

And the generated BCP XML for this column will use:

- FIELD with xsi:type="NativePrefix" and PREFIX_LENGTH="4"

- COLUMN with xsi:type="SQLUDT"

## License

This project is licensed under the MIT License.