import numpy as np

from data_cache.data_cache import Client

c = Client('generic')
c.connect()
q = c.make_queue('test')
for i in range(10):
    r = q.put(np.ones((100000,)).astype('float32') * i)

d = []
for i in range(10):
    print("Getting", i)
    d.append(q.get())
d = np.stack(d)
print(d)
c.disconnect()
