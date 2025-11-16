# app/services/schema_diff_service.py
from typing import Dict, List, Any, Optional

def _extract_fields_from_schema(schema_obj: dict) -> Dict[str, Dict[str, Any]]:
    """
    Extract fields from schema object.
    Handles both ETL format (fields as list) and legacy format (fields as dict).
    
    Returns:
        Dict mapping field name to field object
    """
    fields = {}
    
    if not schema_obj:
        return fields
    
    # Handle canonical schema format (from _canonicalize_schema_from_etl)
    # This has fields as a list of field objects
    if isinstance(schema_obj.get("fields"), list):
        for field in schema_obj["fields"]:
            if isinstance(field, dict):
                field_name = field.get("name") or field.get("path") or "unknown"
                fields[field_name] = field
    
    # Handle legacy format (fields as dict)
    elif isinstance(schema_obj.get("fields"), dict):
        for field_name, field_meta in schema_obj["fields"].items():
            if isinstance(field_meta, dict):
                fields[field_name] = {**field_meta, "name": field_name}
            else:
                fields[field_name] = {"name": field_name, "type": str(field_meta)}
    
    # Handle raw ETL schema format (direct fields list)
    elif isinstance(schema_obj.get("fields"), list):
        for field in schema_obj["fields"]:
            if isinstance(field, dict):
                field_name = field.get("name") or "unknown"
                fields[field_name] = field
    
    # Fallback: try to extract from raw_schema if present
    if not fields and isinstance(schema_obj.get("raw_schema"), dict):
        raw = schema_obj["raw_schema"]
        if isinstance(raw.get("fields"), list):
            for field in raw["fields"]:
                if isinstance(field, dict):
                    field_name = field.get("name") or "unknown"
                    fields[field_name] = field
    
    return fields


def compare_schemas(old_schema: Optional[dict], new_schema: dict) -> dict:
    """
    Compare two schema objects and return added / removed / changed fields.
    
    Works with:
    - ETL schema format (fields as list of objects)
    - Canonical schema format (from schema_service)
    - Legacy format (fields as dict)
    
    Returns format compatible with frontend:
    {
        "added": [field_objects],
        "removed": [field_objects],
        "changed": [{name, old, new}]
    }
    """
    old_fields = _extract_fields_from_schema(old_schema) if old_schema else {}
    new_fields = _extract_fields_from_schema(new_schema) if new_schema else {}
    
    old_names = set(old_fields.keys())
    new_names = set(new_fields.keys())
    
    # Added fields
    added = [new_fields[name] for name in new_names - old_names]
    
    # Removed fields
    removed = [old_fields[name] for name in old_names - new_names]
    
    # Changed fields (type or nullable changed)
    changed = []
    for name in old_names & new_names:
        old_field = old_fields[name]
        new_field = new_fields[name]
        
        old_type = old_field.get("type", "string")
        new_type = new_field.get("type", "string")
        old_nullable = old_field.get("nullable", True)
        new_nullable = new_field.get("nullable", True)
        
        if old_type != new_type or old_nullable != new_nullable:
            changed.append({
                "name": name,
                "field": name,  # Frontend compatibility
                "old": {
                    "type": old_type,
                    "nullable": old_nullable
                },
                "new": {
                    "type": new_type,
                    "nullable": new_nullable
                },
                "old_type": old_type,  # Frontend compatibility
                "new_type": new_type   # Frontend compatibility
            })
    
    return {
        "added": added,
        "removed": removed,
        "changed": changed,
        # Keep backward compatibility
        "added_fields": [f.get("name") or f.get("path", "unknown") for f in added],
        "removed_fields": [f.get("name") or f.get("path", "unknown") for f in removed],
        "old_count": len(old_fields),
        "new_count": len(new_fields)
    }
