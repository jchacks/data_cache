import numpy as np

from data_cache.data_cache import Client

c = Client()
c.connect()
for i in range(10):
    r = c.put(np.ones((100000,)).astype('float32') * i)

d = np.stack([c.get() for i in range(10)])
print(d)
c.disconnect()
