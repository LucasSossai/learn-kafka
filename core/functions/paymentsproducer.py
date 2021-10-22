import json
import random as rng
import time
from datetime import datetime

from core.settings import TOPICS


class FakePaymentProducer:
    def __init__(self):
        self.speed = 0
        self.start_speed_calc_ts = time.time()

    def calculate_speed(self, total_messages):
        if total_messages > 0:
            self.speed = round(total_messages / (time.time() - self.start_speed_calc_ts), 2)

    def generate_topic(self):
        return rng.choice(TOPICS)

    def generate_data(self):
        data = {"producer_speed": self.speed}
        return json.dumps(data).encode("ascii")

    def produce_msg(self):
        data = self.generate_data()
        topic = self.generate_topic()
        return data, topic
