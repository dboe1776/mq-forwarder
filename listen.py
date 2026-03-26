# src/mq_forwarder/mqtt_listener.py

import asyncio
import fnmatch
import models
import json
import aiomqtt
import structlog
import utils  
from collections import defaultdict
from typing import List, Dict, Optional, Tuple
from persistence import PersistentSinkDispatcher
from models import Message
from config_loader import AppConfig, Pipeline

logger = structlog.get_logger(__name__)

def group_pipelines_by_broker(config: AppConfig) -> Dict[str, List[Pipeline]]:
    """Group pipelines by their referenced broker name."""
    groups: Dict[str, List[Pipeline]] = defaultdict(list)
    for pipe in config.pipelines:
        groups[pipe.broker].append(pipe)
    return dict(groups)

def collect_topic_patterns(pipelines: List[Pipeline]) -> List[str]:
    """Gather unique topic patterns for a set of pipelines."""
    patterns = set()
    for pipe in pipelines:
        for t in pipe.topics:
            patterns.add(t)
    return list(patterns)

def find_matching_pipelines(topic: str, pipelines: List[Pipeline]) -> List[Pipeline]:
    """Return pipelines (within the given list) that match the topic."""
    matches = []
    for pipe in pipelines:
        if any(fnmatch.fnmatch(topic, pattern) for pattern in pipe.topics):
            matches.append(pipe)
    return matches

def get_effective_measurement(
    original_meas: str,
    topic: str,
    pipeline: Pipeline
) -> str:
    """Apply pipeline-specific measurement override if present.
        By default, if a message does not include a measurement tag,
        we use the topic with "/" mapped to "_". Otherwise, the measurement
        is pulled from the messge content. However if the the pipeline
        config defines an override at the topic level, we prefer that over all.
        even if the message itself defines a measurement.  
    """
    meas_map = pipeline.measurement_map
    if resolved := utils.resolve_measurement(topic,meas_map):
        return resolved
    else:
        return original_meas

class MessageBundler:
    """Groups partial messages by (device_id, bundle_id) and merges them."""

    def __init__(self):
        # Key: (device_id, bundle_id, topic) → list of Messages + timeout task
        self.buffers: Dict[Tuple[str, str, str], List[Message]] = defaultdict(list)
        self.timeout_tasks: Dict[Tuple[str, str, str], asyncio.Task] = {}

    async def add_message(
        self,
        msg: Message,
        pipe: Pipeline,
        dispatcher: PersistentSinkDispatcher
    ) -> None:
        if msg.bundle_id is None:
            # Normal path - no bundling
            point = msg.to_data_point()
            await dispatcher.dispatch_point(point, pipe.sinks)
            return

        if msg.bundle_size is None or msg.bundle_size < 1:
            logger.warning("bundle_missing_size", bundle_id=msg.bundle_id, id=msg.id)
            # Fallback: dispatch as-is
            point = msg.to_data_point()
            await dispatcher.dispatch_point(point, pipe.sinks)
            return

        key = (msg.id, msg.bundle_id, msg.topic)  # scope by device MAC + bundle

        self.buffers[key].append(msg)

        # Cancel existing timeout if any
        if key in self.timeout_tasks:
            self.timeout_tasks[key].cancel()

        # Schedule new timeout
        self.timeout_tasks[key] = asyncio.create_task(
            self._timeout_handler(key, pipe, dispatcher)
        )

        # Check if we have a complete bundle
        if len(self.buffers[key]) >= msg.bundle_size:
            await self._flush_bundle(key, pipe, dispatcher)

    async def _timeout_handler(self, key: Tuple[str, str, str], pipe: Pipeline, dispatcher: PersistentSinkDispatcher):
        try:
            await asyncio.sleep(pipe.bundle_timeout_ms / 1000.0)
            if key in self.buffers:
                logger.warning("bundle_timeout", bundle_key=key, received=len(self.buffers[key]))
                await self._flush_bundle(key, pipe, dispatcher)
        except asyncio.CancelledError:
            pass  # Normal when we get more parts quickly
        except Exception:
            logger.exception("bundle_timeout_error", bundle_key=key)

    async def _flush_bundle(
        self,
        key: Tuple[str, str, str],
        pipe: Pipeline,
        dispatcher: PersistentSinkDispatcher
    ):
        parts: List[Message] = self.buffers.pop(key, [])
        if key in self.timeout_tasks:
            task = self.timeout_tasks.pop(key)
            task.cancel()

        if not parts: return

        merged_msg = models.merge_messages(parts)

        # Enrich measurement again (in case it depends on pipeline)
        merged_msg.measurement = get_effective_measurement(
            merged_msg.measurement, merged_msg.topic or "", pipe
        )

        point = merged_msg.to_data_point()

        logger.info(
            "bundle_merged",
            bundle_key=key,
            parts=len(parts),
            expected=parts[0].bundle_size,
            time_ns=point.time_ns
        )

        await dispatcher.dispatch_point(point, pipe.sinks)

async def run_single_broker_listener(
    config: AppConfig,
    broker_name: str,
    pipelines: List[Pipeline],
    dispatcher: PersistentSinkDispatcher,
    bundler: MessageBundler
):
    """
    Run a single MQTT client / listener for one broker and its associated pipelines.
    """

    broker = config.brokers.get(broker_name)
    identifier = f"{broker.client_id_prefix}listener-{broker_name}"
    topic_patterns = collect_topic_patterns(pipelines)
    logger.info("starting_broker_listener",endpoint=f"{broker.host}:{broker.port}")

    tls_params = aiomqtt.TLSParameters() if broker.tls else None

    attempts = 0
    while True:
        try:
            async with aiomqtt.Client(
                hostname=broker.host,
                port=broker.port,
                username=broker.username,
                password=broker.password,
                identifier=identifier,
                tls_params=tls_params,
                keepalive=60,
            ) as client:

                logger.info("broker_connected", broker_name=broker_name)

                await client.subscribe([(pattern, 1) for pattern in topic_patterns])
                logger.info("broker_subscribed", broker_name=broker_name, patterns=topic_patterns)

                async for mqtt_msg in client.messages:
                    topic = mqtt_msg.topic.value

                    try:
                        payload_str = mqtt_msg.payload.decode("utf-8")
                        msg = Message.from_json(payload_str, topic=topic)

                        matching = find_matching_pipelines(topic, pipelines)

                        if not matching:
                            logger.debug("message_no_pipeline_match", topic=topic, broker=broker_name)
                            continue

                        logger.debug(
                            "message_received",
                            broker=broker_name,
                            topic=topic,
                            id=msg.id,
                            original_measurement=msg.measurement,
                            matched_pipelines=[p.name for p in matching]
                        )

                        for pipe in matching:
                            msg.measurement = get_effective_measurement(msg.measurement, topic, pipe)

                            if not pipe.sinks:
                                lp = models.to_line_protocol(msg.to_data_point())
                                logger.info("stdout_fallback",pipeline=pipe.name,topic=topic,lp=lp)
                            else:
                                logger.debug("appended_to_persistence", pipeline=pipe.name, sinks=pipe.sinks)
                                await bundler.add_message(msg, pipe, dispatcher)                            
                            

                    except UnicodeDecodeError:
                        logger.warning("non_utf8_payload", broker=broker_name, topic=topic)
                    except json.JSONDecodeError as e:
                        logger.error("invalid_json", broker=broker_name, topic=topic, error=str(e))
                    except Exception:
                        logger.exception("message_processing_error", broker=broker_name, topic=topic)

        except asyncio.CancelledError:
            logger.info("listener_cancelled", broker_name=broker_name)
            break
        except aiomqtt.MqttError as e:
            logger.error("mqtt_connection_failed", broker_name=broker_name, error=str(e))
        except Exception:
            logger.exception("broker_listener_crashed", broker_name=broker_name)
        
        # Backoff before retry
        await asyncio.sleep(min(60, 2 ** attempts))  # 1, 2, 4, 8, ..., cap at 60s
        attempts += 1


async def run_all_listeners(config: AppConfig, dispatcher: PersistentSinkDispatcher):
    """
    Start one listener task per unique broker using TaskGroup.
    """
    broker_groups = group_pipelines_by_broker(config)
    bundler = MessageBundler()

    async with asyncio.TaskGroup() as tg:
        for broker_name, pipelines in broker_groups.items():
            tg.create_task(
                run_single_broker_listener(config, broker_name, pipelines, dispatcher, bundler),
                name=f"mqtt-listener-{broker_name}"
            )

    logger.info("all_broker_listeners_completed")


# ────────────────────────────────────────────────
# Simple test / entry point
# ────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    from config_loader import load_config

    # Windows asyncio fix — must be very early
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


    config = load_config("config.toml")  # adjust path

    dispatcher = PersistentSinkDispatcher(config)

    async def main():

        await dispatcher.start()
        
        print("Starting multi-broker MQTT listeners...\n")
        try:
            await run_all_listeners(config,dispatcher)
        except KeyboardInterrupt:
            print("\nShutdown requested.")
        except Exception as e:
            print(f"Fatal error: {e}")
        finally:
            if dispatcher is not None:
                await dispatcher.stop()

    asyncio.run(main())