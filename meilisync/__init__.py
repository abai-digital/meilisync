
# Abai Digital patch: handle datetime.date in JSON serialization.
# MySQL DATE columns return Python datetime.date objects which are not
# JSON-serializable by default. Meilisearch expects ISO string format.
import json as _json
import datetime as _dt

_original_encoder_default = _json.JSONEncoder.default

def _date_aware_default(self, o):
    if isinstance(o, (_dt.date, _dt.datetime)):
        return o.isoformat()
    return _original_encoder_default(self, o)

_json.JSONEncoder.default = _date_aware_default
