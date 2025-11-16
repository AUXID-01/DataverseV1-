import numpy as np

def sanitize_for_mongo(obj):
    """
    Recursively convert numpy types into pure Python types
    so MongoDB can store them.
    """

    # Base conversions
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.floating,)):
        return float(obj)
    if isinstance(obj, (np.bool_,)):
        return bool(obj)
    if isinstance(obj, np.ndarray):
        return [sanitize_for_mongo(v) for v in obj.tolist()]
    if obj is np.nan:
        return None

    # Dict
    if isinstance(obj, dict):
        return {k: sanitize_for_mongo(v) for k, v in obj.items()}

    # List / Tuple
    if isinstance(obj, (list, tuple)):
        return [sanitize_for_mongo(v) for v in obj]

    return obj
