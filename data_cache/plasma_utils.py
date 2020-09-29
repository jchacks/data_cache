import logging
import os
import shutil
import subprocess
import tempfile
import time

import pyarrow as pa
import pyarrow.plasma as plasma

from data_cache.redis_utils import _redis

logger = logging.getLogger(__name__)
_context = pa.default_serialization_context()


def register_on_context(cls):
    assert hasattr(cls, 'to_dict') and hasattr(cls, 'from_dict'), "Class needs to have 'to_dict' and 'from_dict' methods."
    _context.register_type(cls, cls.__name__,
                           custom_serializer=cls.to_dict,
                           custom_deserializer=cls.from_dict)


def bytes_to_oid(bytestr: bytes):
    return plasma.ObjectID(bytestr)


class PlasmaServer(object):
    def __init__(self, plasma_store_memory,
                 plasma_directory=None,
                 use_hugepages=False,
                 external_store=None,
                 kstore=None):
        """Start a plasma store process.
            Args:
                plasma_store_memory (int): Capacity of the plasma store in bytes.
                plasma_directory (str): Directory where plasma memory mapped files will be stored.
                use_hugepages (bool): True if the plasma store should use huge pages.
                external_store (str): External store to use for evicted objects.
                _kstore: Redis KeyStore to place the socket info into.
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
        _redis.hmset('plasma', {
            'plasma_store_name': self.plasma_store_name,
            'plasma_store_memory': self.plasma_store_memory,
        })
        print(self.plasma_store_name)

    def wait(self):
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            self.stop()

    def stop(self):
        _redis.delete('plasma')
        logger.info("Stopping")
        if self.proc.poll() is None:
            self.proc.kill()
        shutil.rmtree(self.tmpdir)

    def __enter__(self):
        self.start()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()


class PlasmaClient(object):
    def __init__(self):
        self._client = None

    @staticmethod
    def get_details():
        # Todo move this to redis utils?
        return {k.decode(): v for k, v in _redis.hgetall('plasma').items()}

    def connect(self, socket):
        self._client = plasma.connect(socket)

    def disconnect(self):
        self._client.disconnect()

    def get_object(self, uid):
        data = self._client.get(bytes_to_oid(uid))
        return pa.deserialize_components(data, context=_context)

    def put_object(self, obj):
        data = pa.serialize(obj, context=_context).to_components()
        object_id = self._client.put(data)
        return object_id.binary()

    def delete_objects(self, *uids):
        self._client.delete([bytes_to_oid(uid) for uid in uids])
