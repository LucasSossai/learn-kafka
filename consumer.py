from confluent_kafka import Consumer, KafkaException, KafkaError
from core.functions.reportgenerator import ReportGenerator
from core.settings import KAFKA_URL, MIN_COMMIT_COUNT, TOPICS
import sys


def init_consumer():
    print("Initing consumer")
    conf = {"bootstrap.servers": KAFKA_URL, "group.id": "group_id_1", "auto.offset.reset": "smallest"}
    consumer = Consumer(conf)
    consume_loop(consumer, TOPICS)


def consume_loop(consumer, topics):
    try:
        consumer.subscribe(topics)
        msg_count = 0
        report_generator = ReportGenerator()
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write(
                        "%% %s [%d] reached end at offset %d\n" % (msg.topic(), msg.partition(), msg.offset())
                    )
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                msg_count += 1
                if msg_count % 10000 == 0:
                    report_generator.process_msg(msg, msg_count)
                if msg_count % MIN_COMMIT_COUNT == 0:
                    consumer.commit(asynchronous=False)
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()


if __name__ == "__main__":
    init_consumer()
