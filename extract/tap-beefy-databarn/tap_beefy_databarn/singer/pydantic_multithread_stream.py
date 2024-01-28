from __future__ import annotations

import queue
import threading
import typing as t
from abc import ABCMeta, abstractmethod

from pydantic import BaseModel
from tap_beefy_databarn.singer.pydantic_dataclass_stream import PydanticDataclassStream

T = t.TypeVar("T")
R = t.TypeVar("R", bound=BaseModel)

class PydanticMultithreadStream(t.Generic[T,R], PydanticDataclassStream[R], metaclass=ABCMeta):
    """
    A stream that uses a pydantic dataclass to generate it's schema.
    Also provide multithreading primitives to start multiple threads that will
    produce records.
    """

    @abstractmethod
    def get_thread_params(self, context: dict[t.Any, t.Any] | None) -> t.Iterable[T]:
        """
        Return a list of tuples of parameters to pass to the thread function.
        """
        msg = "get_thread_params must be implemented"
        raise NotImplementedError(msg)

    @abstractmethod
    def thread_record_producer(self, context: dict[t.Any, t.Any] | None, params: T) -> t.Iterable[R]:
        """
        Produce records for the stream.
        """
        msg = "thread_record_producer must be implemented"
        raise NotImplementedError(msg)

    def _run_thread_and_write_to_queue(self, context: dict[t.Any, t.Any] | None, params: T, q: queue.Queue) -> None:
        """
        Run the thread and write the records it produces to the queue.
        """
        try:
            for record in self.thread_record_producer(context, params):
                q.put(record)
        finally:
            # put a None record to signal the end of the stream
            q.put(None)

    def get_models(self, context: dict[t.Any, t.Any] | None) -> t.Iterable[R]:
        """
        Instanciate the worker threads and return the records they produce.
        """
        self.logger.debug("Starting threads")
        q: queue.Queue = queue.Queue()
        threads = []

        for params in self.get_thread_params(context):
            self.logger.debug("Starting thread with params: %s", params)
            t = threading.Thread(target=self._run_thread_and_write_to_queue, args=(context, params, q))
            t.start()
            threads.append(t)

        ended_threads = 0
        while ended_threads < len(threads):
            record = q.get()
            if record is None:
                ended_threads += 1
            else:
                yield record

        for t in threads:
            t.join()

        self.logger.debug("All threads ended")
