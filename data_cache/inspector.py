from data_cache.redis_utils import _redis, KStore
import pyarrow.plasma as plasma


_kstore = KStore(prefix='plasma')
_plasma_socket = _kstore['plasma_store_name'].decode()

plasma_client = plasma.connect(_plasma_socket)

keys = _redis.keys()
print(keys)
for k in keys:
    if k.startswith('kstore:'):
        pass

_redis.hkeys('queues'), _redis.hmget('queues', 'next-sale-training')
