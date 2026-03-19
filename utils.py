def mqtt_topic_matches(pattern: str, topic: str) -> bool:
    """
    True if the concrete topic matches the MQTT subscription pattern.
    Supports + (single level) and # (multi-level) wildcards.
    """
    if pattern == topic:
        return True

    # Split into levels
    p_levels = pattern.split('/')
    t_levels = topic.split('/')

    if len(p_levels) != len(t_levels) and '#' not in pattern:
        return False

    for p, t in zip(p_levels, t_levels):
        if p == '#':
            return True  # # matches everything from here on
        if p == '+' or p == t:
            continue
        return False

    # If pattern has trailing # and topic is longer
    if p_levels and p_levels[-1] == '#' and len(t_levels) >= len(p_levels):
        return True

    return len(p_levels) == len(t_levels)

def resolve_measurement(topic: str, measurement_map: dict[str, str]) -> str | None:
    """
    Return the measurement name if any pattern matches, None otherwise.
    Exact matches have priority over patterns.
    """
    # 1. Exact match first (highest priority)
    if topic in measurement_map:
        return measurement_map[topic]

    # 2. Try each pattern
    for pattern, meas_name in measurement_map.items():
        if mqtt_topic_matches(pattern, topic):
            return meas_name

    return None