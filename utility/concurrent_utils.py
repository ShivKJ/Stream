from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor

from utility.class_utils import constructor_setter


class Task(ABC):
    @abstractmethod
    def __call__(self):
        pass

    @classmethod
    def task_generator(cls, *args, **kwargs):
        raise NotImplementedError


class Worker:
    @constructor_setter
    def __init__(self, *, no_threads=1, tasks):
        pass

    def work(self):
        tp = ThreadPoolExecutor(max_workers=self.no_threads)
        results = []

        for task in self.tasks:
            results.append(tp.submit(task))
        for r in results:
            r.result()

        tp.shutdown()
