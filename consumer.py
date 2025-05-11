import pulsar
from balancer import client

consumer = client.subscribe(
    "persistent://public/was-scanner/plzpROT",
    consumer_type=pulsar.ConsumerType.Shared,
    subscription_name="py-inspector",
)

total = 0
while True:
    print(f'current count is {total}')
    msg = consumer.receive()
    if not msg:
        break
    total += 1
    consumer.acknowledge(msg)
print(f'total no. of msgs read from topic {total}')
