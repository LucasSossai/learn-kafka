import json
import time


class ReportGenerator:
    def __init__(self):
        self.consumer_speed = 0
        self.producer_speed = 0
        self.start_speed_calc_ts = time.time()

    def process_msg(self, msg, msg_count):
        self.calculate_speed(msg_count)
        data = json.loads(msg.value().decode("ascii"))
        self.producer_speed = data["producer_speed"]
        self.print_report()

    def calculate_speed(self, total_messages):
        if total_messages > 0:
            self.consumer_speed = round(total_messages / (time.time() - self.start_speed_calc_ts), 2)

    def print_report(self):
        print(f"Producer speed : {self.producer_speed} | Consumer speed : {self.consumer_speed}")
