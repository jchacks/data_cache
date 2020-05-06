import os
import shutil
import subprocess
import tempfile
import time

import pyarrow as pa
import pyarrow.plasma as plasma

from data_cache.redis_utils import Queue, KStore

_kstore = KStore(prefix='plasma')

context = pa.default_serialization_context()


def register_on_context(cls):
    context.register_type(cls, cls.__name__,
                          custom_serializer=cls.to_dict,
                          custom_deserializer=cls.from_dict)


def bytes_to_oid(bytestr):
    return plasma.ObjectID(bytestr)


class Server(object):
    def __init__(self, plasma_store_memory,
                 plasma_directory=None,
                 use_hugepages=False,
                 external_store=None):
        """Start a plasma store process.
            Args:
                plasma_store_memory (int): Capacity of the plasma store in bytes.
                plasma_directory (str): Directory where plasma memory mapped files will be stored.
                use_hugepages (bool): True if the plasma store should use huge pages.
                external_store (str): External store to use for evicted objects.
            Return:
                A tuple of the name of the plasma store socket and the process ID of
                    the plasma store process.
            """
        self.plasma_store_memory = plasma_store_memory
        self.plasma_directory = plasma_directory
        self.use_hugepages = use_hugepages
        self.external_store = external_store
        self.plasma_store_name = None
        self.proc = None
        self.tmpdir = None

    def start(self):
        self.tmpdir = tempfile.mkdtemp(prefix='plasma-')
        plasma_store_name = os.path.join(self.tmpdir, 'plasma.sock')
        plasma_store_executable = os.path.join(pa.__path__[0], "plasma-store-server")
        command = [plasma_store_executable,
                   "-s", plasma_store_name,
                   "-m", str(self.plasma_store_memory)]
        if self.plasma_directory:
            command += ["-d", self.plasma_directory]
        if self.use_hugepages:
            command += ["-h"]
        if self.external_store is not None:
            command += ["-e", self.external_store]
        stdout_file = None
        stderr_file = None
        proc = subprocess.Popen(command, stdout=stdout_file, stderr=stderr_file)
        time.sleep(0.1)
        rc = proc.poll()
        if rc is not None:
            raise RuntimeError("plasma_store exited unexpectedly with code %d" % (rc,))

        self.plasma_store_name = plasma_store_name
        self.proc = proc
        _kstore['plasma_store_name'] = self.plasma_store_name
        _kstore['plasma_store_memory'] = self.plasma_store_memory
        print(self.plasma_store_name)

    def wait(self):
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            self.stop()

    def stop(self):
        del _kstore['plasma_store_name']
        print("Stopping")
        if self.proc.poll() is None:
            self.proc.kill()
        shutil.rmtree(self.tmpdir)

    def __enter__(self):
        self.start()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()


class Client(object):
    """
    Wrapper around plasma client simplifying serialization
    """

    def __init__(self, socket=None, queue='plasma', queue_maxsize=None):
        if socket is None:
            socket = _kstore['plasma_store_name']
            socket = socket.decode()
        self.socket = socket
        self.queue = Queue(queue, queue_maxsize)
        self.kstore = KStore('generic')
        self.plasma_client = None

    def __enter__(self):
        self.connect()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()

    def put_many(self, *args):
        res = []
        with self.queue.pipeline(res):
            for arg in args:
                self.put(arg)
        return res

    def disconnect(self):
        self.plasma_client.disconnect()

    def connect(self):
        self.plasma_client = plasma.connect(self.socket)

    def get_object(self, id):
        return pa.deserialize_components(self.plasma_client.get(bytes_to_oid(id)), context=context)

    def put_object(self, obj):
        data = pa.serialize(obj, context=context).to_components()
        object_id = self.plasma_client.put(data)
        return object_id.binary()

    def put(self, data, block=True, timeout=None):
        uid = self.put_object(data)
        print("Put object at", uid)
        self.queue.put(uid)

    def get(self, block=True, timeout=None):
        uid = self.queue.get(block, timeout)
        print("Getting object at", uid)
        r = self.get_object(uid)
        self.plasma_client.delete(self, uid)
        return r

    def __getitem__(self, item):
        """
        Get an object id from self.kstore and return the corresponding
        object from the plasma store
        :param item: key to retrive from keystore
        :return: python object from plasma store
        """
        return self.get_object(self.kstore[item])

    def __setitem__(self, key, value):
        """
        Set item on the plasma store and put the plasma store uid
        at key in the redis store
        :param key: key to place uid in Redis
        :param value: python object to store
        :return: None
        """
        self.kstore[key] = self.put_object(value)

    def __delitem__(self, key):
        uid = self.kstore[key]
        self.plasma_client.delete(bytes_to_oid(uid))
        del self.kstore[key]

    def __repr__(self):
        return "Client<q_len=%s, s_len=%s>" % (len(self.queue), len(self.plasma_client.list()))
