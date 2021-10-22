from confluent_kafka import Producer
from core.settings import KAFKA_URL, MIN_FLUSH_COUNT, TOTAL_MESSAGES, PRODUCE_MSG_INTERVAL
from core.functions.paymentsproducer import FakePaymentProducer
import time
import random as rng


def init_fake_producer():
    print("Started fake producer")
    client_id = f"client-{rng.randint(1,100)}"
    conf = {"bootstrap.servers": KAFKA_URL, "client.id": client_id}
    producer = Producer(conf)
    fake_generator = FakePaymentProducer()
    for messages_sent, msg_count in enumerate(range(TOTAL_MESSAGES)):
        data, topic = fake_generator.produce_msg()
        producer.produce(topic, data)

        fake_generator.calculate_speed(messages_sent)
        if msg_count % MIN_FLUSH_COUNT == 0:
            producer.flush()
        time.sleep(PRODUCE_MSG_INTERVAL)

    producer.flush()
    print("Finished fake producer")


if __name__ == "__main__":
    init_fake_producer()
