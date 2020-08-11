import logging
from contextlib import contextmanager
from queue import Full, Empty
from time import time, sleep
from uuid import uuid4
from enum import Enum
import redis

logger = logging.getLogger(__name__)
_redis = redis.Redis(host='localhost', port=6379, db=0)


def flush():
    return _redis.flushall()


class RTypes(Enum):
    HASH = b'hash'
    STRING = b'string'


class Lock(object):
    def __init__(self, to_lock):
        self.prefix = 'lock:'
        self._redis = _redis
        self._key = self.prefix + str(to_lock)
        self.lua_lock = _redis.lock(self._key)

    def acquire(self, block=True, timeout=None):
        self.lua_lock.acquire(blocking=block, blocking_timeout=timeout)

    def release(self):
        self.lua_lock.release()

    def __enter__(self, block=True, timeout=None):
        self.acquire(block, timeout)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()


class KStore(object):
    def __init__(self, prefix=None):
        self.prefix = 'kstore:'
        if prefix:
            self.prefix += prefix + ':'
        self._redis = _redis

    def __delitem__(self, key):
        key = self.prefix + key
        self._redis.delete(key)

    def __getitem__(self, item):
        item = self.prefix + item
        redis_type = self._redis.type(item)
        if redis_type == RTypes.HASH.value:
            r = self._redis.hgetall(item)
        else:
            r = self._redis.get(item)
        if r is None:
            raise KeyError("'%s' not found." % item)
        return r

    def __setitem__(self, key, value):
        key = self.prefix + key
        if isinstance(value, dict):
            self._redis.hmset(key, value)
        else:
            self._redis.set(key, value)

    def scan(self):
        cursor, keys = self._redis.scan(match=self.prefix + '*')
        res = keys
        while cursor != 0:
            cursor, keys = self._redis.scan(match=self.prefix + '*')
            res.extend(keys)
        return keys

    def delete(self):
        keys = self.scan()
        for key in keys:
            key = self.prefix + key.decode()
            self._redis.delete(key)


class Queue(object):
    def __init__(self, name, maxsize=None):
        self.name = name
        self._redis = _redis
        self.maxsize = maxsize
        self._key = self._redis.hget('queues', self.name)
        if self._key is None:
            self._key = 'queue:' + str(uuid4())
            self._redis.hset('queues', self.name, self._key)
        self.lock = Lock(to_lock=self._key)

    @property
    def length(self):
        return self._redis.llen(self._key)

    def __repr__(self):
        return "Queue<%s>" % (self.name)

    def drain(self):
        uids = []
        try:
            while True:
                uids.append(self.get(block=False))
        except Empty:
            logger.info("Drained queue, %s items." % len(uids))
        finally:
            return uids

    def put(self, uid, block=True, timeout=None):
        if self.maxsize:
            if not block:
                if self.length >= self.maxsize:
                    raise Full
            elif timeout is None:
                while True:
                    with self.lock:
                        if self.length <= self.maxsize:
                            self._redis.rpush(self._key, [uid])
                            return
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

        self._redis.rpush(self._key, [uid])

    def get(self, block=True, timeout=None):
        if not block:
            r = self._redis.lpop(self._key)
            if r is None:
                raise Empty
        elif timeout is None:
            r = None
            while r is None:
                while True:
                    with self.lock:
                        if self.length:
                            return self._redis.lpop(self._key)
                    sleep(0.1)
        elif timeout < 0:
            raise ValueError("'timeout' must be a non-negative number")
        else:
            endtime = time() + timeout
            while not self.length:
                remaining = endtime - time()
                if remaining <= 0.0:
                    raise Empty
                sleep(remaining)
            r = self._redis.lpop(self._key)
        return r

    def delete(self):
        with self.lock:
            logger.debug("Deleting Queue")
            self._redis.delete(self._key)

    def __len__(self):
        return self._redis.llen(self._key)

    @contextmanager
    def pipeline(self, res: list):
        conn = self._redis
        self._redis = self._redis.pipeline()
        yield self
        res.extend(self._redis.execute())
        self._redis = conn
