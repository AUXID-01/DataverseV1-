import os
import tempfile
import logging
from typing import Dict, Any

import pandas as pd

# ✔ Correct import paths — based on your project structure
from app.etl.transform.transform_main import run_transform_pipeline
from app.etl.load.schema_generator import SchemaGenerator
from app.etl.load.schema_evolution import SchemaEvolution
from app.etl.extract.extract import extract_data, detect_file_type
from app.utils.mongo_sanitize import sanitize_for_mongo

logger = logging.getLogger(__name__)

# Initialize schema evolution tracker (uses schemas/ directory)
_schema_evolution = SchemaEvolution(storage_path=os.path.join(os.path.dirname(__file__), "..", "etl", "schemas"))


async def run_dynamic_etl_bytes(file_bytes: bytes, filename: str, source_id: str = None) -> Dict[str, Any]:
    """
    Run extraction, transformation, and schema generation directly on uploaded bytes.
    
    Args:
        file_bytes: Raw file bytes
        filename: Original filename
        source_id: Optional source identifier (defaults to filename without extension)
    
    Returns:
        Dictionary with structured_data, schema, row_count, cleaning_stats, and parsed_fragments
    """
    # Generate source_id from filename if not provided
    if source_id is None:
        source_id = os.path.splitext(filename)[0].replace(" ", "_")
    
    ext = os.path.splitext(filename)[1]

    # Write to temporary file
    with tempfile.NamedTemporaryFile(delete=False, suffix=ext) as tmp:
        tmp.write(file_bytes)
        tmp_path = tmp.name

    try:
        # Extract
        df, fragments = extract_data(tmp_path, return_fragments=True)

        if df is None or df.empty:
            return {
                "structured_data": [],
                "schema": {},
                "row_count": 0,
                "cleaning_stats": {},
                "parsed_fragments": fragments,
            }

        # Transform and get stats
        transformed_df, transform_stats = run_transform_pipeline(df)

        # Generate Schema
        schema_gen = SchemaGenerator()
        schema = schema_gen.generate_schema(transformed_df, source_id, fragments)
        
        # Track schema evolution
        try:
            schema = _schema_evolution.add_schema(source_id, schema)
            logger.info(f"Schema version {schema.get('version', 1)} tracked for {source_id}")
        except Exception as e:
            logger.warning(f"Schema evolution tracking failed: {e}")
            # Continue without evolution tracking

        # Convert DF to records and sanitize for MongoDB
        structured = transformed_df.to_dict(orient="records")
        
        # Sanitize all records for MongoDB compatibility
        sanitized_structured = []
        for record in structured:
            # Convert NaN/NaT to None
            sanitized_record = {}
            for key, value in record.items():
                if pd.isna(value):
                    sanitized_record[key] = None
                else:
                    sanitized_record[key] = sanitize_for_mongo(value)
            sanitized_structured.append(sanitized_record)

        # Sanitize schema for MongoDB
        sanitized_schema = sanitize_for_mongo(schema)

        # Calculate nulls removed
        nulls_before = int(df.isna().sum().sum())
        nulls_after = int(transformed_df.isna().sum().sum())
        nulls_removed = nulls_before - nulls_after

        # Format cleaning_stats for frontend
        cleaning_stats = {
            # Frontend-expected format
            "nullsRemoved": max(0, nulls_removed),
            "typesCast": transform_stats.get("types_cast", 0),
            "formatsFixed": transform_stats.get("formats_fixed", 0),
            "duplicatesDropped": transform_stats.get("duplicates_dropped", 0),
            # Backend internal format (for compatibility)
            "rows_before": len(df),
            "rows_after": len(transformed_df),
            "nulls_before": nulls_before,
            "nulls_after": nulls_after,
        }

        return {
            "structured_data": sanitized_structured,
            "schema": sanitized_schema,
            "row_count": len(sanitized_structured),
            "cleaning_stats": cleaning_stats,
            "parsed_fragments": fragments,
            "source_id": source_id,
            "schema_version": schema.get("version", 1)
        }

    finally:
        try:
            os.remove(tmp_path)
        except:
            pass
