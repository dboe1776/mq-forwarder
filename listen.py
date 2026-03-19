# src/mq_forwarder/mqtt_listener.py

import asyncio
import fnmatch
import models
import json
import aiomqtt
import structlog
import utils  
from collections import defaultdict
from typing import List, Dict
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

async def run_single_broker_listener(
    config: AppConfig,
    broker_name: str,
    pipelines: List[Pipeline],
    dispatcher: PersistentSinkDispatcher
):
    """
    Run a single MQTT client / listener for one broker and its associated pipelines.
    """

    broker = config.brokers.get(broker_name)
    identifier = f"{broker.client_id_prefix}listener-{broker_name}"
    topic_patterns = collect_topic_patterns(pipelines)
    logger.info("starting_broker_listener",endpoint=f"{broker.host}:{broker.port}")

    tls_params = aiomqtt.TLSParameters() if broker.tls else None

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
                            
                            # Create enriched DataPoint
                            point = msg.to_data_point()

                            if not pipe.sinks:
                                lp = models.to_line_protocol(point)
                                logger.info("stdout_fallback",pipeline=pipe.name,topic=topic,lp=lp)

                            else:
                                # Dispatch to queues
                                await dispatcher.dispatch_point(point, pipe.sinks)
                                logger.debug("appended_to_persistence", pipeline=pipe.name, sinks=pipe.sinks)

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

    async with asyncio.TaskGroup() as tg:
        for broker_name, pipelines in broker_groups.items():
            tg.create_task(
                run_single_broker_listener(config, broker_name, pipelines, dispatcher),
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