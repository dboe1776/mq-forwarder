import datetime as dt
import json
from dataclasses import dataclass, field, InitVar, asdict
from typing import Any

def escape_lp_identifier(s: str) -> str:
    """Escape measurement, tag key/value, field key for Line Protocol."""
    if not isinstance(s, str):
        s = str(s)
    return (
        s.replace("\\", "\\\\")
         .replace(",", "\\,")
         .replace("=", "\\=")
         .replace(" ", "\\ ")
    )

def escape_lp_field_value(v: Any) -> str:
    """Format and escape a field value for Line Protocol."""
    if isinstance(v, str):
        escaped = v.replace("\\", "\\\\").replace('"', '\\"')
        return f'"{escaped}"'
    elif isinstance(v, bool):
        return "t" if v else "f"
    elif isinstance(v, int):
        return f"{v}i"          # integer type tag
    elif isinstance(v, float):
        return f"{v:g}"         # avoid scientific notation noise
    else:
        raise ValueError(f"Unsupported field value type: {type(v).__name__}")

@dataclass(kw_only=True)
class DataPoint:
    time_ns: int              # nanoseconds since epoch
    measurement: str
    tags: dict[str, str] = field(default_factory=dict)
    fields: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if not self.fields:
            raise ValueError("DataPoint must have at least one field")
    
    def to_json(self):
        return json.dumps(asdict(self))

@dataclass(kw_only=True)
class Message:
    time: float | int = field(default_factory=lambda: dt.datetime.now(dt.timezone.utc).timestamp())
    t_receive: dt.datetime = field(default_factory=lambda: dt.datetime.now(dt.timezone.utc))
    id: str
    measurement: str | None = None           # renamed for clarity
    tags: dict[str, str] | None = field(default_factory=dict)
    fields: dict[str, Any] | None = field(default_factory=dict)
    topic: InitVar[str]

    def __post_init__(self, topic: str):
        # Basic sanity on time (seconds since epoch)
        if not (1_500_000_000 < self.time < 4_000_000_000):  # roughly 2017–2096
            raise ValueError(f"Invalid timestamp: {self.time} (expected seconds since epoch)")

        # Enrich
        self.tags = self.tags or {}
        self.tags["id"] = self.id

        self.fields = self.fields or {}
        self.fields["t_receive"] = round(self.t_receive.timestamp(), 3)

        # Fallback measurement name
        if self.measurement is None:
            self.measurement = topic.replace("/", "_").replace(" ", "_").strip("_")

    def to_data_point(self) -> DataPoint:
        return DataPoint(
            time_ns=int(float(self.time) * 1_000_000_000),  # → ns
            measurement=self.measurement,
            tags=self.tags,
            fields=self.fields,
        )

    @classmethod
    def from_json(cls, payload: str, topic: str) -> "Message":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON payload: {e}") from e

        # Optional: strict field checking here later
        return cls(**data, topic=topic)


def to_line_protocol(point: DataPoint,ignore_errors=False) -> str:
    """Convert DataPoint to InfluxDB v2 Line Protocol (ns precision)."""
    try:
        if not point.fields:
            raise ValueError("Cannot write DataPoint with no fields")

        meas = escape_lp_identifier(point.measurement)

        tag_parts = [
            f"{escape_lp_identifier(k)}={escape_lp_identifier(v)}"
            for k, v in sorted(point.tags.items())   # sort → deterministic output
        ]
        tags_str = "," + ",".join(tag_parts) if tag_parts else ""

        field_parts = [
            f"{escape_lp_identifier(k)}={escape_lp_field_value(v)}"
            for k, v in point.fields.items()
            if v is not None
        ]
        fields_str = ",".join(field_parts)

        return f"{meas}{tags_str} {fields_str} {point.time_ns}"
    except Exception as e:
        if ignore_errors: return None
        else:
            # logger.warning("lp_conversion_failed", point_time=point.time_ns, error=str(e))
            raise e

if __name__ == '__main__':
    sample = f"""{{
        "time": {dt.datetime.now(tz=dt.timezone.utc).timestamp()},
        "id": "test-client",
        "measurement": "periodic-data",
        "tags": {{
            "room": "living-room",
            "name": "sample"
        }},
        "fields": {{
            "temp": 22.51,
            "humidity": 54.1,
            "int_val": 1032,
            "bool_val": true
        }}
        }}"""    
    p = Message.from_json(sample,topic='telemetry/test').to_data_point()
    print(to_line_protocol((p)))    