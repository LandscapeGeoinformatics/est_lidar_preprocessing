import threading
import logging


class ReturnValueThread(threading.Thread):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.result = None
        self.downloadtime = None

    def run(self):
        if self._target is None:
            return None
        try:
            self.result, self.downloadtime = self._target(*self._args, **self._kwargs)
        except Exception as exc:
            logging.error(f'{type(exc).__name__}: {exc}')

    def join(self, *args, **kwargs):
        super().join(*args, **kwargs)
        return (self.result, self.downloadtime)
