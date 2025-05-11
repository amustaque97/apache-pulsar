import re
import os
import sys
import time
import pulsar
import logging
import requests
from typing import List, Dict, Union
from balancer import client, get_topics


logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)


def subscribe_ns(namespace: str) -> Union[pulsar.Consumer, None]:
    try:
        consumer = client.subscribe(
            topic=re.compile(f"persistent://public/{namespace}/.*"),
            subscription_name="py-inspector",
            pattern_auto_discovery_period=1,
            consumer_type=pulsar.ConsumerType.Shared,
        )
        return consumer
    except Exception as err:
        logging.error(f"[x] got error cannot subscribe whole namespace {namespace}")
    return None


def get_unack_count(topic: str, ns: str) -> int:
    unack_count = 0

    stats_url = f"http://localhost:8080/admin/v2/persistent/public/{ns}/{topic}/stats"
    res = requests.get(stats_url)
    if not res.ok:
        logging.error(f"[x] Unable to get stats for given topic {topic!r} in ns {ns!r}")

    response = res.json()
    subscriptions = response.get("subscriptions", {})
    for _, sub_data in subscriptions.items():
        unack_count = sub_data.get("unackedMessages", 0)
    return unack_count


def get_topics_stats(topics: List[str], ns: str) -> Dict[str, int]:
    stats = {}
    for topic in topics:
        topic = topic.split("/")[-1]
        stats[topic] = get_unack_count(topic, ns)
    return stats


def main() -> int:
    ns = "was-scanner"

    while True:
        consumer = subscribe_ns(ns)
        logging.info(f"[*] subscribed whole namespace {ns!r}")
        if not consumer:
            continue

        topics = get_topics()
        if not topics:
            logging.error(f"[x] unable to get all topics for given ns {ns!r}")
            continue

        stats = get_topics_stats(topics, ns)
        if not stats:
            logging.error(
                f"[x] unable to get stats for given ns {ns!r} topics {topics!r}"
            )
            os._exit(1)

        logging.info(f"[*] got unack messages stats {stats!r}")

        consumer.close()
        time.sleep(5)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
