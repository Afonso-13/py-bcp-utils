import xml.etree.ElementTree as ET

BCP_NATIVE_TYPE_MAP = {
    'BIGINT':    {'field_type': 'NativePrefix', 'prefix_length': 1, 'column_type': 'SQLBIGINT'},
    'INT':       {'field_type': 'NativePrefix', 'prefix_length': 1, 'column_type': 'SQLINT'},
    'SMALLINT':  {'field_type': 'NativePrefix', 'prefix_length': 1, 'column_type': 'SQLSMALLINT'},
    'TINYINT':   {'field_type': 'NativePrefix', 'prefix_length': 1, 'column_type': 'SQLTINYINT'},
    'BIT':       {'field_type': 'NativePrefix', 'prefix_length': 1, 'column_type': 'SQLBIT'},
    'FLOAT':     {'field_type': 'NativePrefix', 'prefix_length': 1, 'column_type': 'SQLFLT8'},
    'REAL':      {'field_type': 'NativePrefix', 'prefix_length': 1, 'column_type': 'SQLFLT4'},
    'DATE':      {'field_type': 'NativePrefix', 'prefix_length': 1, 'column_type': 'SQLDATE'},
    'DATETIME2': {'field_type': 'NativePrefix', 'prefix_length': 1, 'column_type': 'SQLDATETIME2'},

    'VARCHAR':   {'field_type': 'CharPrefix',  'prefix_length': 2, 'column_type': 'SQLVARYCHAR'},
    'NVARCHAR':  {'field_type': 'NCharPrefix', 'prefix_length': 2, 'column_type': 'SQLNVARCHAR'},
}

def generate_bcp_xml(table_schema: dict,
                     collation: str = "SQL_Latin1_General_CP1_CI_AS") -> str:
    root = ET.Element("BCPFORMAT")
    root.set("xmlns", "http://schemas.microsoft.com/sqlserver/2004/bulkload/format")
    root.set("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance")

    record = ET.SubElement(root, "RECORD")
    row = ET.SubElement(root, "ROW")

    field_id = 1
    for col_name, info in table_schema.items():
        sql_type = info['type'].upper()
        if sql_type not in BCP_NATIVE_TYPE_MAP:
            raise ValueError(f"Unsupported SQL type for BCP: {sql_type}")

        mapping = BCP_NATIVE_TYPE_MAP[sql_type]
        field_type = mapping['field_type']
        prefix_len = mapping['prefix_length']
        column_xsi = mapping['column_type']

        field = ET.SubElement(record, "FIELD")
        field.set("ID", str(field_id))

        if field_type == "NativePrefix":
            field.set("xsi:type", "NativePrefix")
            field.set("PREFIX_LENGTH", str(prefix_len))

        elif field_type in ("CharPrefix", "NCharPrefix"):
            field.set("xsi:type", field_type)
            field.set("PREFIX_LENGTH", str(prefix_len))

            max_len = info.get('max_length')
            if max_len is None:
                raise ValueError(
                    f"max_length is required for {sql_type} column '{col_name}'"
                )
            field.set("MAX_LENGTH", str(max_len))
            field.set("COLLATION", collation)

        else:
            raise ValueError(f"Unsupported FIELD type '{field_type}' for column '{col_name}'")

        column = ET.SubElement(row, "COLUMN")
        column.set("SOURCE", str(field_id))
        column.set("NAME", col_name)
        column.set("xsi:type", column_xsi)

        field_id += 1

    ET.indent(root, space="  ")
    return '<?xml version="1.0"?>\n' + ET.tostring(root, encoding='utf-8').decode('utf-8')