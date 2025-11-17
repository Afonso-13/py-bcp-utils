import pandas as pd
import numpy as np
from datetime import date
import struct

def _build_native_prefixed(data_bytes: np.ndarray, non_null_len: int, null_mask: np.ndarray) -> np.ndarray:
    """
    Helper: given data_bytes shape (N, L) and null_mask (N,),
    build an array shape (N,) of dtype V(1+L) with prefix+data.
    """
    N, L = data_bytes.shape
    assert L == non_null_len, "data_bytes width must equal non_null_len"

    prefix = np.full(N, non_null_len, dtype="u1")
    prefix[null_mask] = 0xFF

    raw = np.empty((N, 1 + L), dtype="u1")
    raw[:, 0] = prefix
    raw[:, 1:] = data_bytes

    return raw.view(f"V{1 + L}").ravel()

def convert_int_to_bcp(series: pd.Series) -> np.ndarray:
    """
    Native INT with 1-byte length prefix.
    Non-null: [0x04][4-byte little-endian int32]
    Null:     [0xFF]
    """
    records = []
    non_null_prefix = 4
    null_prefix = 0xFF

    int_series = series.astype("Int32")

    for val in int_series:
        if pd.isna(val):
            records.append(bytes([null_prefix]))
        else:
            iv = int(val)
            payload = struct.pack("<i", iv)
            records.append(bytes([non_null_prefix]) + payload)

    return np.array(records, dtype=object)

def convert_bigint_to_bcp(series: pd.Series) -> np.ndarray:
    """
    Native BIGINT with 1-byte length prefix.
    Non-null: [0x08][8-byte little-endian int64]
    Null:     [0xFF]
    """
    records = []
    non_null_prefix = 8
    null_prefix = 0xFF

    int_series = series.astype("Int64")

    for val in int_series:
        if pd.isna(val):
            records.append(bytes([null_prefix]))
        else:
            iv = int(val)
            payload = struct.pack("<q", iv)
            records.append(bytes([non_null_prefix]) + payload)

    return np.array(records, dtype=object)



def convert_date_to_bcp(series: pd.Series) -> np.ndarray:
    """
    Native DATE with 1-byte length prefix.

    Storage: 3-byte little-endian signed int = days since 0001-01-01
    Non-null: [0x03][3-byte day count]
    Null:     [0xFF]
    """
    records = []
    non_null_prefix = 3
    null_prefix = 0xFF

    base_ordinal = date(1, 1, 1).toordinal()

    for val in series:
        if pd.isna(val):
            records.append(bytes([null_prefix]))
        else:
            ts = pd.Timestamp(val).normalize()
            d = ts.date()
            days = d.toordinal() - base_ordinal

            payload_full = struct.pack("<i", days)
            payload = payload_full[:3]

            records.append(bytes([non_null_prefix]) + payload)

    return np.array(records, dtype=object)


def convert_varchar_to_bcp(series: pd.Series, encoding='latin1') -> np.ndarray:
    null_prefix = b'\xFF\xFF'
    empty_prefix = b'\x00\x00'
    
    def process_row(s):
        if pd.isna(s):
            return null_prefix
        s_str = str(s)
        if len(s_str) == 0:
            return empty_prefix
        data_bytes = s_str.encode(encoding)
        len_prefix = struct.pack('<H', len(data_bytes))
        return len_prefix + data_bytes

    return series.apply(process_row).values

def convert_float_to_bcp(series: pd.Series) -> np.ndarray:
    """
    Native FLOAT(53) (8-byte) with 1-byte length prefix.
    Non-null: [0x08][8-byte IEEE 754 double]
    Null:     [0xFF]
    """
    records = []
    non_null_prefix = 8
    null_prefix = 0xFF

    for val in series:
        if pd.isna(val):
            records.append(bytes([null_prefix]))
        else:
            fv = float(val)
            payload = struct.pack("<d", fv)
            records.append(bytes([non_null_prefix]) + payload)

    return np.array(records, dtype=object)


def convert_real_to_bcp(series: pd.Series) -> np.ndarray:
    """
    Native REAL (4-byte) with 1-byte length prefix.
    Non-null: [0x04][4-byte IEEE 754 float]
    Null:     [0xFF]
    """
    records = []
    non_null_prefix = 4
    null_prefix = 0xFF

    for val in series:
        if pd.isna(val):
            records.append(bytes([null_prefix]))
        else:
            fv = float(val)
            payload = struct.pack("<f", fv)
            records.append(bytes([non_null_prefix]) + payload)

    return np.array(records, dtype=object)

def convert_nvarchar_to_bcp(series: pd.Series, encoding='utf-16-le') -> np.ndarray:
    """
    Converts a pandas Series (string) to a BCP native NVARCHAR format
    (as a numpy array of bytes objects).

    Format per row: [2-byte prefix][N-byte data]
    Prefix: 0xFFFF (NULL) or 0x0000 (empty) or Length (e.g., 0x0400 for 'hi')
    """
    null_prefix = b'\xFF\xFF'
    empty_prefix = b'\x00\x00'
    
    def process_row(s):
        if pd.isna(s):
            return null_prefix
        
        s_str = str(s)
        if len(s_str) == 0:
            return empty_prefix

        data_bytes = s_str.encode(encoding)
        len_prefix = struct.pack('<H', len(data_bytes))
        
        return len_prefix + data_bytes

    return series.apply(process_row).values

def convert_smallint_to_bcp(series: pd.Series) -> np.ndarray:
    """
    Native SMALLINT with 1-byte length prefix.
    Non-null: [0x02][2-byte little-endian int16]
    Null:     [0xFF]
    """
    records = []
    non_null_prefix = 2
    null_prefix = 0xFF

    int_series = series.astype("Int16")

    for val in int_series:
        if pd.isna(val):
            records.append(bytes([null_prefix]))
        else:
            iv = int(val)
            payload = struct.pack("<h", iv)
            records.append(bytes([non_null_prefix]) + payload)

    return np.array(records, dtype=object)


def convert_tinyint_to_bcp(series: pd.Series) -> np.ndarray:
    """
    Native TINYINT with 1-byte length prefix.
    Non-null: [0x01][1-byte unsigned int]
    Null:     [0xFF]
    """
    records = []
    non_null_prefix = 1
    null_prefix = 0xFF

    int_series = series.astype("UInt8")

    for val in int_series:
        if pd.isna(val):
            records.append(bytes([null_prefix]))
        else:
            iv = int(val)
            payload = struct.pack("<B", iv)
            records.append(bytes([non_null_prefix]) + payload)

    return np.array(records, dtype=object)

def convert_bit_to_bcp(series: pd.Series) -> np.ndarray:
    """
    Native BIT with 1-byte length prefix.
    Non-null: [0x01][1-byte 0 or 1]
    Null:     [0xFF]
    """
    records = []
    non_null_prefix = 1
    null_prefix = 0xFF

    for val in series:
        if pd.isna(val):
            records.append(bytes([null_prefix]))
        else:
            bv = 1 if bool(val) else 0
            payload = struct.pack("<B", bv)
            records.append(bytes([non_null_prefix]) + payload)

    return np.array(records, dtype=object)

def convert_datetime2_to_bcp(series: pd.Series, scale: int = 7) -> np.ndarray:
    """
    Native DATETIME2(7) with 1-byte length prefix.

    Para scale 7:
      - 5 bytes de time: número de intervalos de 100 ns desde meia-noite
      - 3 bytes de date: dias desde 0001-01-01
      Total payload: 8 bytes

    Non-null: [0x08][5-byte time][3-byte date]
    Null:     [0xFF]
    """
    if scale != 7:
        raise NotImplementedError("convert_datetime2_to_bcp atualmente assume DATETIME2(7).")

    records = []
    non_null_prefix = 8
    null_prefix = 0xFF

    base_ordinal = date(1, 1, 1).toordinal()

    for val in series:
        if pd.isna(val):
            records.append(bytes([null_prefix]))
            continue

        ts = pd.Timestamp(val)

        date_only = ts.normalize()
        d = date_only.date()
        days = d.toordinal() - base_ordinal
        date_bytes = struct.pack("<i", days)[:3]

        midnight = pd.Timestamp(d)
        nanos_since_midnight = (ts - midnight).value
        ticks_100ns = nanos_since_midnight // 100
        time_bytes = struct.pack("<Q", ticks_100ns)[:5]

        payload = time_bytes + date_bytes  # 8 bytes
        records.append(bytes([non_null_prefix]) + payload)

    return np.array(records, dtype=object)

BCP_CONVERTER_MAP = {
    'INT': convert_int_to_bcp,
    'BIGINT': convert_bigint_to_bcp,
    'SMALLINT': convert_smallint_to_bcp,
    'TINYINT': convert_tinyint_to_bcp,
    'BIT': convert_bit_to_bcp,
    'FLOAT': convert_float_to_bcp,
    'REAL': convert_real_to_bcp,
    'DATE': convert_date_to_bcp,
    'DATETIME2': convert_datetime2_to_bcp,
    'VARCHAR': convert_varchar_to_bcp,
    'NVARCHAR': convert_nvarchar_to_bcp,
}