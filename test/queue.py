import logging
logging.basicConfig()

import numpy as np
from data_cache import Client

c = Client()

queue = c.make_queue('test')
for i in range(10):
    queue.put(np.ones((100000,)).astype('float32') * i)


# In a separate python process
c = Client()
queue = c.make_queue('test')

d = []
for i in range(10):
    print("Getting", i)
    d.append(queue.get())
d = np.stack(d)
print(d)
