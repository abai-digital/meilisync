# Abai Digital patches for meilisync:
# 1. datetime.date / datetime.datetime -> ISO format string.
# 2. bytes -> UTF-8 string with hex fallback (handles MySQL binary/raw binlog fields).
# 3. decimal.Decimal -> float/int.
# 4. set -> list.
import json as _json
import datetime as _dt
import decimal as _decimal

_original_encoder_default = _json.JSONEncoder.default

def _custom_json_default(self, o):
    if isinstance(o, (_dt.date, _dt.datetime)):
        return o.isoformat()
    if isinstance(o, bytes):
        try:
            return o.decode("utf-8")
        except UnicodeDecodeError:
            return o.hex()
    if isinstance(o, _decimal.Decimal):
        return float(o)
    if isinstance(o, set):
        return list(o)
    return _original_encoder_default(self, o)

_json.JSONEncoder.default = _custom_json_default
