from dataclasses import dataclass, field
from typing import List, Dict, Optional
import tomllib
from pathlib import Path

@dataclass
class MqttBroker:
    host: str
    port: int = 1883
    tls: bool = False
    username: Optional[str] = None
    password: Optional[str] = None
    client_id_prefix: str = "mq-forwarder-"

@dataclass
class SinkConfig:
    type: str                       # "influxdb", "sqlite", ...
    # common fields
    batch_size: int = 500
    flush_interval_sec: int = 15
    # sink-type specific fields go into extra dict
    extra: Dict[str, any] = field(default_factory=dict)

@dataclass
class Pipeline:
    name: str
    broker: str                     # reference to broker name
    topics: List[str]
    sinks: List[str] = field(default_factory=list)  # can be empty → stdout
    measurement_map: Dict[str, str] = field(default_factory=dict)
    field_mapping: Dict[str, str] = field(default_factory=dict)

@dataclass
class AppConfig:
    log_level: str = "INFO"
    data_dir: str = "./data"
    brokers: Dict[str, MqttBroker] = field(default_factory=dict)
    sinks: Dict[str, SinkConfig] = field(default_factory=dict)
    pipelines: List[Pipeline] = field(default_factory=list)

def load_config(path: Path | str) -> AppConfig:
    path = Path(path)
    data = tomllib.loads(path.read_text(encoding="utf-8"))

    cfg = AppConfig()

    # [app]
    app_table: dict = data.get("app", {})
    cfg.log_level = app_table.get("log_level", "INFO")
    cfg.data_dir  = app_table.get("data_dir", "./data")

    # [brokers.*]
    brokers_table = data.get("brokers", {})
    for name, b in brokers_table.items():
        cfg.brokers[name] = MqttBroker(**b)

    # [sinks.*]
    sinks_table = data.get("sinks", {})
    for name, s in sinks_table.items():
        sink = SinkConfig(type=s.pop("type"), extra=s)
        cfg.sinks[name] = sink

    # [[pipelines]]
    for p in data.get("pipelines", []):
        # measurement_map and field_mapping can be nested tables
        meas_map = p.pop("measurement_map", {})
        field_map = p.pop("field_mapping", {})
        pipe = Pipeline(**p, measurement_map=meas_map, field_mapping=field_map)
        cfg.pipelines.append(pipe)

    return cfg