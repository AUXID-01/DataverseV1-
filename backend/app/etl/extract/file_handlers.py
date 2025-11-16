import json
import pandas as pd
import logging
from .smart_readers import smart_read_combined
from .pdf_readers import read_pdf_tables, read_pdf_text_ocr

logger = logging.getLogger(__name__)


# ----------------------------------
# SAFE JSON PARSER (solves your error)
# ----------------------------------

def safe_json_to_df(data):
    """
    Converts ANY JSON into a clean DataFrame.
    Handles:
    - uneven lists
    - dict of lists
    - list of dicts
    - missing fields
    - nested objects (stringified)
    """

    # Case 1: List of dicts
    if isinstance(data, list):
        normalized = []

        for item in data:
            if isinstance(item, dict):
                row = {}
                for k, v in item.items():
                    if isinstance(v, (dict, list)):
                        row[k] = json.dumps(v)
                    else:
                        row[k] = v
                normalized.append(row)
            else:
                normalized.append({"value": item})

        return pd.DataFrame(normalized)

    # Case 2: Dict of arrays / single values
    if isinstance(data, dict):
        max_len = max(
            (len(v) for v in data.values() if isinstance(v, list)),
            default=1
        )

        cleaned = {}
        for key, value in data.items():
            if isinstance(value, list):
                padded = value + [None] * (max_len - len(value))
                cleaned[key] = padded
            else:
                # duplicate single value to match rows
                cleaned[key] = [value] + [None] * (max_len - 1)

        return pd.DataFrame(cleaned)

    # Case 3: Primitive value
    return pd.DataFrame([data])


def read_json(path):
    """Unified safe JSON handler."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        return safe_json_to_df(data)

    except Exception as e:
        logger.error(f"JSON parsing failed: {e}")
        return pd.DataFrame()


# ----------------------------------
# HTML / XML SAFE READERS
# ----------------------------------

def read_html_safely(path):
    try:
        tables = pd.read_html(path, flavor="bs4", parser="lxml")
        if tables:
            return tables[0]
    except Exception:
        pass

    try:
        tables = pd.read_html(path, flavor="bs4", parser="html5lib")
        if tables:
            return tables[0]
    except Exception:
        pass

    try:
        tables = pd.read_html(path)
        if tables:
            return tables[0]
    except Exception as e:
        logger.warning(f"HTML read failed: {e}")

    return pd.DataFrame()


def read_xml_safely(path):
    try:
        return pd.read_xml(path)
    except Exception as e:
        logger.warning(f"XML read failed: {e}")
        return pd.DataFrame()


# ----------------------------------
# READER REGISTRY
# ----------------------------------

READERS = {
    "json": read_json,
    "csv": pd.read_csv,
    "txt": lambda path: smart_read_combined(path),
    "html": lambda path: read_html_safely(path),
    "md": lambda path: smart_read_combined(path),
    "xml": lambda path: read_xml_safely(path),
    "xlsx": lambda path: pd.read_excel(path, engine="openpyxl"),
    "xls": lambda path: pd.read_excel(path, engine="xlrd", dtype=str),
    "tsv": lambda path: pd.read_csv(path, sep="\t"),
    "pdf": lambda path: (read_pdf_tables(path) or read_pdf_text_ocr(path)),
    "parquet": pd.read_parquet,
}
