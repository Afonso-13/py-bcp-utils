# Python BCP Utility (py-bcp-utils)

[![PyPI version](https://badge.fury.io/py/py-bcp-utils.svg)](https://badge.fury.io/py/py-bcp-utils)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A simple Python utility to run the SQL Server `bcp` (Bulk Copy Program) command using a Pandas DataFrame as the source.

This package acts as a thin wrapper, saving the DataFrame to a temporary CSV file and then calling the `bcp` command-line tool to perform a high-speed bulk insert.

## Key Features

* Bulk insert `pandas.DataFrame` objects directly into SQL Server.
* Uses the highly performant, native `bcp` command-line tool.
* Supports both SQL Server Authentication (username/password) and Trusted Connections (Windows Authentication).
* Simple, single-function API.

## Requirements

1.  **Python 3.8+**
2.  **`pandas`** (will be installed automatically)
3.  **`bcp` Utility:** The SQL Server `bcp` command-line utility **must be installed on your system** and available in your shell's PATH.
    * On Windows, this is typically installed with **SQL Server Management Studio (SSMS)** or the **Microsoft Command Line Utilities for SQL Server**.

## 💾 Installation

Once the package is published to the real PyPI, you can install it with:

```bash
pip install py-bcp-utils
```

## Usage Example

Here is a complete example of how to import and use the function.

```python
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
    logging.info("Attempting insert with SQL Server login...")
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

## License

This project is licensed under the MIT License.