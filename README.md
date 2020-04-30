# Data Cache

Simple in memory data cache designed for ML applications.
Built using Redis and Apache Arrow's Plasma in-memory store

## Usage

### Server
```python
from data_cache import Server

s = Server()
s.start()
s.wait()

# The location of the plasma store will be returned
# e.g. '/tmp/plasma-qd3yeugu/plasma.sock'
```

### Data Producing Client
```python
from data_cache import Client

c = Client('/tmp/plasma-qd3yeugu/plasma.sock')
c.connect()

# Put some dummy data into the queue
import numpy as np 

for i in range(10):
    r = c.put(np.ones((100000,)).astype('float32') * i)

c.disconnect()
```

### Data Consuming Client
```python
from data_cache import Client

c = Client('/tmp/plasma-qd3yeugu/plasma.sock')
c.connect()

# Fetch data off the queue using c.get()
import numpy as np 
d = np.stack([c.get() for i in range(10)])
print(d) 

# This will print the numpy array of 
# concatenated data in order 1->10
```

Client class can also be used in a with statement to automatically connect/disconnect.