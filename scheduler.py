from datetime import datetime
import threading


class Scheduler:
    def __init__(self, *args) -> None:
        self.jobs = args
        self.schedule_thread = threading.Thread(target=self.run)

    def run(self) -> None:
        old = datetime.utcnow().timestamp()
        while True:
            new = datetime.utcnow().timestamp()
            if new - old >= 1:
                old = new
                for job in self.jobs:
                    job.job()

    def start(self) -> None:
        self.schedule_thread.start()