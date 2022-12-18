from datetime import datetime
from time import time_ns
import threading


class Scheduler:
    def __init__(self, *args) -> None:
        self.jobs = args
        self.schedule_thread = threading.Thread(target=self.run)

    def run(self) -> None:
        old = time_ns()
        while True:
            new = time_ns()
            if new - old >= 10**8:
                old = new
                for job in self.jobs:
                    job.job()

    def start(self) -> None:
        self.schedule_thread.start()