from time import time_ns, time
import threading


class Scheduler:
    def __init__(self, *args, duration=None, event=None, filename=None) -> None:
        self.duration = duration
        self.jobs = args
        self.event = event
        self.filename = filename
        self.schedule_thread = threading.Thread(target=self.run)

    def run(self) -> None:
        start = time()
        old = time_ns()
        while True:
            if self.duration is not None and time_ns() - start >= self.duration:
                break
            new = time()
            if new - old >= 10**8:
                old = new
                [job.job(new, self.filename) for job in self.jobs]
        self.kill()

    def start(self) -> None:
        self.schedule_thread.start()

    def kill(self):
        if self.event is None:
            return
        self.event.set()
        [i.kill() for i in self.jobs]