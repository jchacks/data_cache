import logging

from data_cache.plasma_utils import PlasmaClient
from data_cache.redis_utils import RedisQueue, RedisDict

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())
logger.setLevel(logging.WARNING)

_kstore = RedisDict(prefix='plasma')


class Queue(RedisQueue):
    def __init__(self, client: PlasmaClient, name, maxsize=None):
        super(Queue, self).__init__(name, maxsize)
        self.client = client

    def put(self, data, block=True, timeout=None):
        uid = self.client.put_object(data)
        logger.debug("Put object at '%s'" % uid)
        super(Queue, self).put(uid)

    def get(self, block=True, timeout=None):
        uid = super(Queue, self).get(block, timeout)
        logger.debug("Getting object at '%s'" % uid)
        r = self.client.get_object(uid)
        self.client.delete_objects(uid)
        return r

    def delete(self):
        with self.lock:
            uids = self.drain()
            if uids:
                self.client.delete_objects(*uids)
        super(Queue, self).delete()


class KStore(object):
    def __init__(self, plasma_client, namespace):
        self._namespace = namespace
        self.plasma_client = plasma_client
        self._dict = RedisDict(prefix=namespace)

    def __getitem__(self, item):
        """
        Get an object id from self.kstore and return the corresponding
        object from the plasma store
        :param item: key to retrive from keystore
        :return: python object from plasma store
        """
        return self.plasma_client.get_object(self._dict[item])

    def __setitem__(self, key, value):
        """
        Set item on the plasma store and put the plasma store uid
        at key in the redis store
        :param key: key to place uid in Redis
        :param value: python object to store
        :return: None
        """
        try:
            uid = self._dict[key]
            logger.warning("Found key '%s', deleting from plasma..." % key)
            self.plasma_client.delete_objects(uid)
        except KeyError:
            pass
        finally:
            self._dict[key] = self.plasma_client.put_object(value)

    def __delitem__(self, key):
        uid = self._dict[key]
        self.plasma_client.delete_objects(uid)
        del self._dict[key]


class Client(object):
    """
    Wrapper around plasma client and redis simplifying serialization
    """

    def __init__(self, socket=None):
        if socket is None:
            details = PlasmaClient.get_details()
            socket = details['plasma_store_name'].decode()

        self.socket = socket
        self.queues = {}
        self.stores = {}
        self.plasma_client = PlasmaClient()
        self.plasma_client.connect(socket)

    def make_queue(self, name, maxsize=None):
        return Queue(self.plasma_client, name, maxsize)

    def get_or_create_store(self, item):
        if item in self.stores.keys():
            return self.stores[item]
        else:
            logger.info("Could not find store '%s'; creating..." % item)
            kstore = KStore(self.plasma_client, item)
            self.stores[item] = kstore
            return kstore

    def __repr__(self):
        return "Client<%s, %s>" % (id(self), self.plasma_client)
