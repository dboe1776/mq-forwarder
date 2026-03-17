import datetime as dt
from dataclasses import dataclass, field, InitVar 
import json



@dataclass(kw_only=True)
class DataPoint:   
    time: float|int
    meas: str
    tags: dict[str, str] = field(default_factory=dict)
    fields: dict[str, str | int | float] = field(default_factory=dict) 

@dataclass(kw_only=True)
class Message:   
    time: float|int = field(default_factory=lambda: dt.datetime.now(tz=dt.timezone.utc).timestamp())
    t_receive: dt.datetime = field(default_factory=lambda: dt.datetime.now(tz=dt.timezone.utc))
    id: str
    meas: str | None = None
    tags: dict[str, str]| None = field(default_factory=dict)
    fields: dict[str, str | int | float] | None = field(default_factory=dict)
    topic : InitVar[str]

    def __post_init__(self,topic: str):

        # SOME Sort of chek on whether time.  It must be in seconds. 

        self.tags.update({"id": self.id})
        self.fields.update({"t_receive": round(self.t_receive.timestamp(),3)})
        if self.meas is None:
            self.meas = topic.replace("/","_").replace(" ","_")

    def dump_point(self) -> DataPoint:
        return DataPoint(
            time=self.time,
            meas=self.meas,
            tags=self.tags,
            fields=self.fields
        )
    
    @classmethod
    def load_json(cls, json_str: str, topic: str) -> "Message":
        try:
            json_dict = json.loads(json_str)
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {e}")
            raise
        return cls(**json_dict, topic=topic)

def format_line_protocol(key:str, value: str | int | float | bool) -> str:
    if isinstance(value, str):
        value = value.replace('"', '\\"')  # Escape double quotes
        return f'{key}="{value}"'
    elif isinstance(value, bool):
        return f'{key}={str(value).lower()}'  # InfluxDB expects boolean as true/false
    else:
        return f'{key}={value}'    

def dump_line_protocol(point:DataPoint) -> str:
    time = int(point.time*1e3) # InfluxDB expects time in milliseconds
    tags = ','+','.join([f'{k}={v}' for k,v in point.tags.items()]) if point.tags else ''
    fields = ','.join([format_line_protocol(k,v) for k, v in point.fields.items() if v is not None])
    return f"{point.meas}{tags} {fields} {time}"




if __name__ == "__main__":

    sample = """{
    "time": 1773698280.413,
    "id": "test-client",
    "meas": "periodic-data",
    "tags": {
        "room": "living-room",
        "name": "sample"
    },
    "fields": {
        "temp": 22.51,
        "humidity": 54.1
    }
    }"""
    p = Message.load_json(json.loads(sample),topic='telemetry/test').dump_point()
    print(dump_line_protocol(p))