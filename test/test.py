import numpy as np

from data_cache.data_cache import Client

c = Client()
c.connect()
for i in range(10):
    r = c.put(np.ones((100000,)).astype('float32') * i)

d = []
for i in range(10):
    print("Getting", i)
    d.append(c.get())
d = np.stack(d)
print(d)
c.disconnect()
