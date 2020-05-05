import redis
from uuid import uuid4
from time import time, sleep
from contextlib import contextmanager
from queue import Full, Empty

_Redis = redis.Redis(host='localhost', port=6379, db=0)


class KStore(object):
    def __init__(self, prefix=None):
        self.prefix = 'kstore:'
        if prefix:
            self.prefix += prefix + ':'
        self.connection = _Redis

    def __delitem__(self, key):
        key = self.prefix + key
        self.connection.delete(key)

    def __getitem__(self, item):
        item = self.prefix + item
        return self.connection.get(item)

    def __setitem__(self, key, value):
        key = self.prefix + key
        self.connection.set(key, value)


class Queue(object):
    def __init__(self, name, maxsize=0):
        self.name = name
        self.connection = _Redis
        self.maxsize = 0
        self._key = self.connection.hget('queues', self.name)
        if self._key is None:
            self._key = 'queue:' + str(uuid4())
            self.connection.hset('queues', self.name, self._key)

    @property
    def length(self):
        return self.connection.llen(self._key)

    def __repr__(self):
        return "Queue<%s>" % (self.name)

    def put(self, *id, block=True, timeout=None):
        if not block:
            if self.length >= self.maxsize:
                raise Full
        elif timeout is None:
            while self.length >= self.maxsize:
                sleep(1)
        elif timeout < 0:
            raise ValueError("'timeout' must be a non-negative number")
        else:
            endtime = time() + timeout
            while self.length >= self.maxsize:
                remaining = endtime - time()
                if remaining <= 0.0:
                    raise Full
                sleep(remaining)

        self.connection.rpush(self._key, *id)

    def get(self, block=True, timeout=None):
        if not block:
            if not self.length:
                raise Empty
        elif timeout is None:
            while not self.length:
                sleep(1)
        elif timeout < 0:
            raise ValueError("'timeout' must be a non-negative number")
        else:
            endtime = time() + timeout
            while not self.length:
                remaining = endtime - time()
                if remaining <= 0.0:
                    raise Empty
                sleep(remaining)

        r = self.connection.lpop(self._key)
        if r is None:
            raise Empty
        return r

    def delete(self):
        print("Deleting Queue")
        self.connection.delete(self._key)

    def __len__(self):
        return self.connection.llen(self._key)

    @contextmanager
    def pipeline(self, res: list):
        conn = self.connection
        self.connection = self.connection.pipeline()
        yield self
        res.extend(self.connection.execute())
        self.connection = conn