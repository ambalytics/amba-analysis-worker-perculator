import signal
import time
from multiprocessing import Process
import twitter_worker

import logging


class Supervisor:
    def __init__(self):
        self.running = True
        signal.signal(signal.SIGINT, self.stop)
        signal.signal(signal.SIGTERM, self.stop)  # need

    def stop(self, signum):
        logging.warning("Supervisor    :" + signum)
        self.running = False

    def stopWorkers(self):
        logging.warning("Supervisor    : close threads.")
        for worker in self.workers:
            worker.kill()

    def main(self):
        format = "%(asctime)s: %(message)s"
        logging.basicConfig(format=format, level=logging.INFO,
                            datefmt="%H:%M:%S")
        self.workers = list()



if __name__ == "__main__":
    time.sleep(10)
    supervisor = Supervisor()
    supervisor.main()
    number_worker = 5  # same as partitions
    total_workers = 0

    while supervisor.running:
        while len([w for w in supervisor.workers if w.is_alive()]) <= number_worker:
            total_workers += 1
            logging.warning("Main    : create and start thread %d.", total_workers)
            worker = Process(target=twitter_worker.start_worker, args=(total_workers,), daemon=True)
            worker.start()
            supervisor.workers.append(worker)

