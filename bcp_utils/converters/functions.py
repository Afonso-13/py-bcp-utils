import pandas as pd
import numpy as np
from datetime import date
import struct


def _build_native_prefixed(data_bytes: np.ndarray, non_null_len: int, null_mask: np.ndarray) -> np.ndarray:
    """
    Helper: given data_bytes of shape (N, L) and null_mask (N,),
    build an array of shape (N,) with a 1-byte prefix plus data (viewed as V(1+L)).
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


def convert_date_to_bcp(series: pd.Series) -> np.ndarray:
    """
    Native DATE with 1-byte length prefix.

    Storage: 3-byte little-endian signed int = days since 0001-01-01.
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


def convert_datetime2_to_bcp(series: pd.Series, scale: int = 7) -> np.ndarray:
    """
    Native DATETIME2(7) with 1-byte length prefix.

    For scale 7:
      - Time: 5 bytes, little-endian, number of 100 ns intervals since midnight.
      - Date: 3 bytes, little-endian, days since 0001-01-01.
      Total payload: 8 bytes.

    Non-null: [0x08][5-byte time][3-byte date]
    Null:     [0xFF]
    """
    if scale != 7:
        raise NotImplementedError("convert_datetime2_to_bcp currently assumes DATETIME2(7).")

    records = []
    non_null_prefix = 8
    null_prefix = 0xFF

    base_ordinal = date(1, 1, 1).toordinal()

    for val in series:
        if pd.isna(val):
            records.append(bytes([null_prefix]))
            continue

        ts = pd.Timestamp(val)

        # Date part -> days since 0001-01-01 (3 bytes)
        date_only = ts.normalize()
        d = date_only.date()
        days = d.toordinal() - base_ordinal
        date_bytes = struct.pack("<i", days)[:3]

        # Time part -> 100 ns ticks since midnight (5 bytes)
        midnight = pd.Timestamp(d)
        nanos_since_midnight = (ts - midnight).value
        ticks_100ns = nanos_since_midnight // 100
        time_bytes = struct.pack("<Q", ticks_100ns)[:5]

        payload = time_bytes + date_bytes
        records.append(bytes([non_null_prefix]) + payload)

    return np.array(records, dtype=object)


def convert_varchar_to_bcp(series: pd.Series, encoding: str = "latin1") -> np.ndarray:
    """
    Native VARCHAR with 2-byte length prefix (CharPrefix).

    Per row:
      - NULL  : [0xFF 0xFF]
      - Empty : [0x00 0x00]
      - Other : [2-byte little-endian length N][N bytes encoded with `encoding`]
    """
    null_prefix = b"\xFF\xFF"
    empty_prefix = b"\x00\x00"

    def process_row(s):
        if pd.isna(s):
            return null_prefix

        s_str = str(s)
        if len(s_str) == 0:
            return empty_prefix

        data_bytes = s_str.encode(encoding)
        len_prefix = struct.pack("<H", len(data_bytes))
        return len_prefix + data_bytes

    return series.apply(process_row).values


def convert_nvarchar_to_bcp(series: pd.Series, encoding: str = "utf-16-le") -> np.ndarray:
    """
    Native NVARCHAR with 2-byte length prefix (NCharPrefix).

    Per row:
      - NULL  : [0xFF 0xFF]
      - Empty : [0x00 0x00]
      - Other : [2-byte little-endian length N][N bytes encoded with `encoding`]
    """
    null_prefix = b"\xFF\xFF"
    empty_prefix = b"\x00\x00"

    def process_row(s):
        if pd.isna(s):
            return null_prefix

        s_str = str(s)
        if len(s_str) == 0:
            return empty_prefix

        data_bytes = s_str.encode(encoding)
        len_prefix = struct.pack("<H", len(data_bytes))
        return len_prefix + data_bytes

    return series.apply(process_row).values


def convert_geometry_to_bcp(series: pd.Series) -> np.ndarray:
    """
    Native GEOMETRY (SQLUDT) with 4-byte length prefix.

    Expected input for each non-null value:
      - bytes / bytearray / memoryview containing the geometry WKB/varbinary
        (for example: result of SELECT geom.STAsBinary() in SQL Server), or
      - hex string representing that WKB, e.g.:
          * "0xE61000000104..."
          * "E61000000104..." (with or without the "0x" prefix, with or without whitespace)

    Not supported:
      - WKT (e.g. "POLYGON (...)", "POINT (...)", etc.)
      - GeoJSON or any other textual representation.

    BCP payload per row:
      - Non-null: [4-byte little-endian length N][N bytes of WKB]
      - Null    : [4-byte little-endian -1]  (0xFF FF FF FF)

    Important:
      - The FIELD in the BCP XML for this column must have PREFIX_LENGTH=4,
        and the COLUMN must use xsi:type="SQLUDT".
    """
    result = []
    null_prefix = np.array(-1, dtype="<i4").tobytes()

    for val in series:
        if pd.isna(val):
            result.append(null_prefix)
            continue

        # Normalize to bytes (WKB)
        if isinstance(val, (bytes, bytearray, memoryview)):
            payload = bytes(val)
        elif isinstance(val, str):
            s = val.strip()
            if s.startswith(("0x", "0X")):
                s = s[2:]
            s = "".join(s.split())
            try:
                payload = bytes.fromhex(s)
            except ValueError:
                raise ValueError(
                    "Invalid geometry value. Expected WKB in hex "
                    "(e.g. '0xE6100000...') or raw bytes. "
                    f"Received: {repr(val)[:80]}"
                )
        else:
            raise TypeError(
                "Invalid type for geometry. Expected bytes/bytearray/memoryview "
                "or hex string (e.g. '0xE6100000...'). "
                f"Received: {type(val)}"
            )

        length = len(payload)
        length_prefix = np.array(length, dtype="<i4").tobytes()
        result.append(length_prefix + payload)

    return np.array(result, dtype=object)


BCP_CONVERTER_MAP = {
    "INT": convert_int_to_bcp,
    "BIGINT": convert_bigint_to_bcp,
    "SMALLINT": convert_smallint_to_bcp,
    "TINYINT": convert_tinyint_to_bcp,
    "BIT": convert_bit_to_bcp,
    "FLOAT": convert_float_to_bcp,
    "REAL": convert_real_to_bcp,
    "DATE": convert_date_to_bcp,
    "DATETIME2": convert_datetime2_to_bcp,
    "VARCHAR": convert_varchar_to_bcp,
    "NVARCHAR": convert_nvarchar_to_bcp,
    "GEOMETRY": convert_geometry_to_bcp,
}