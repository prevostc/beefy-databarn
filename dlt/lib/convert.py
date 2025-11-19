from typing import Any, Optional

def get_int_like(obj: Any, key: str) -> Optional[int]:
    val = obj.get(key)
    if val == "NaN":
        return None

    return val