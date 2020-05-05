import zmq


class Producer(object):
    def __init__(self, port=5559):
        self.port = port
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.PUSH)

    def connect(self):
        self.socket.connect("tcp://localhost:%s" % self.port)

    def push(self, msg):
        self.socket.send_string(msg)


class Consumer(object):
    def __init__(self,port=5560):
        self.port = port
        self.context = zmq.Context()

    def connect(self):
        self.socket = self.context.socket(zmq.PULL)
        self.socket.connect("tcp://localhost:%s" % self.port)

    def pull(self):
        message = self.socket.recv_string()
        print("Received reply ", "[", message, "]")
        return message


p = Producer()
p.connect()
for i in range(100):
    p.push("Message %s" % i)

c = Consumer()
c.connect()
for i in range():
    c.pull()
