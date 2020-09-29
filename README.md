# Data Cache

Simple in memory data cache designed for local non distributed ML applications.
Built using Redis and Apache Arrow's Plasma in-memory store.

## Installation

Install using pip: 
`pip install git+https://github.com/jchacks/data_cache.git`

### Prerequisites

There are a few python packages that are required.
* Pyarrow
* Redis

Along with a running Redis server for the message queue.


## Usage

### Server
```python
from data_cache import Server

s = Server(100000000) # 100MB
s.start()
s.wait()

# The location of the plasma store will be printed
# e.g. '/tmp/plasma-qd3yeugu/plasma.sock'
# This location is also added to the Redis store 
# so clients can automatically find it.
```

### Data Producing Client
```python
from data_cache import Client

# Ensure the `namespace` is the same everywhere the data is needed to be accessed
c = Client(namespace='generic') 
c.connect()
q = c.make_queue('plasma', None)
# Put some dummy data into the queue
import numpy as np 

for i in range(10):
    r = q.put(np.ones((100000,)).astype('float32') * i)

c.disconnect()
```

### Data Consuming Client
```python
from data_cache import Client

c = Client('generic')
c.connect()
q = c.make_queue('plasma', None) # Use the same name as above

# Fetch data off the queue using c.get()
import numpy as np 
d = np.stack([q.get() for i in range(10)])
print(d) 

# This will print the numpy array of 
# concatenated data in order 1->10
```

Client class can also be used in a with statement to automatically connect/disconnect.

### Setting persistant data on the store
```python
import numpy as np 
from data_cache import Client

c = Client('generic')
c.connect()
c['abc'] = np.ones((100000,)).astype('float32')

# This will access the data and not remove it from plasma
print(c['abc'])

```

## TODO
* Setup a nice API for accessing namespaces and queues.