import redis
from uuid import uuid4

from contextlib import contextmanager


class Queue(object):
    def __init__(self, name):
        self.name = name
        self.connection = redis.Redis(host='localhost', port=6379, db=0)

        self._key = self.connection.hget('queues', self.name)
        if self._key is None:
            self._key = 'queue:' + str(uuid4())
            self.connection.hset('queues', self.name, self._key)

    def __repr__(self):
        return "Queue<%s>" % (self.name)

    def put(self, *id):
        self.connection.rpush(self._key, *id)

    def get(self):
        return self.connection.lpop(self._key)

    def delete(self):
        print("Deleting Queue")
        self.connection.delete(self._key)

    def __len__(self):
        return self.connection.llen(self._key)

    @contextmanager
    def pipeline(self):
        conn = self.connection
        self.connection = self.connection.pipeline()
        yield self
        self.connection.execute()
        self.connection = conn