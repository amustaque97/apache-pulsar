from __future__ import annotations
import os
import sys
import time
import string
import pulsar
import random
import requests
import logging
from typing import List, Dict, Union


logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)


URL = "pulsar://localhost:6650"
client = pulsar.Client(URL)


def get_topics() -> List[str]:
    topics_url = "http://localhost:8080/admin/v2/persistent/public/was-scanner"
    res = requests.get(topics_url)
    if res.ok:
        logging.info("[*] getting all topics for namespace 'was-scanner'")
        return res.json()
    return []


def get_randomized_string(k=7) -> str:
    characters = string.ascii_lowercase + string.ascii_uppercase + string.digits
    return "".join(random.choices(characters, k=k))


def create_producer(topic_name: str) -> Union[pulsar.Producer, None]:
    new_topic_path = f"persistent://public/was-scanner/{topic_name}"
    try:
        producer = client.create_producer(new_topic_path, producer_name=topic_name)
        return producer
    except Exception as err:
        logging.error(f"[x] got erorr while creating producer {err}")
    return None


def send_randomized_payload(producer: pulsar.Producer, topic: str) -> None:
    no_of_payloads = random.randint(1_000, 1_00_000)
    logging.info(f"[*] sending payload to {topic} {no_of_payloads!r} times!")

    for _ in range(no_of_payloads):
        producer.send(get_randomized_string().encode())
    logging.info(f"[*] complete payload push to {topic} {no_of_payloads!r} times!")


def main() -> int:
    topics = get_topics()
    logging.info(f"[*] topics list for 'was-scanner' {topics}")

    for _ in range(1_00_00_000):
        topic = get_randomized_string()
        logging.info(f"[*] randomized topic name is {topic}")
        producer = create_producer(topic)
        if not producer:
            logging.warning(f"[!] unable to create producer with name {topic}")
            os._exit(1)
        logging.info(f"[*] producer created successfully with topic name: {topic}")
        send_randomized_payload(producer, topic)
        time.sleep(10)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
