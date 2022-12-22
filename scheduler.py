from time import time_ns, time
from datetime import datetime as dt
import threading
from consts import GLOBAL_OUTPUT_FILE_NAME, GLOBAL_OUTPUT_FOLDER
from functions import printProgressBar


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
        path = self.filename if self.filename is not None else GLOBAL_OUTPUT_FILE_NAME
        path = GLOBAL_OUTPUT_FOLDER + path
        with open(path, 'a') as file:
            while True:
                if self.duration is not None:
                    tm = time()
                    printProgressBar(tm - start, self.duration)
                    if tm - start >= self.duration:
                        break
                new = time_ns()
                if new - old >= 10**8:
                    old = new
                    [job.job(str(new), file) for job in self.jobs]
        self.kill()

    def start(self) -> None:
        self.schedule_thread.start()

    def kill(self):
        if self.event is None:
            return
        self.event.set()
        [i.kill() for i in self.jobs]