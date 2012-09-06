from port import Port
import socket
import czjson as serlib
import snappy

def dumps(data):
    return snappy.compress(serlib.dumps(data))

def loads(data):
    return serlib.loads(snappy.decompress(data))

class Worker(object):
	def __init__(self, C, *args):
		self.instance = C(*args)
		
	def run(self, broker_addr):
		listen_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		listen_sock.connect(broker_addr)
		self.port = Port(listen_sock)
		while True:
			message = self.port.read()
			if message:
				self.port.write(self.handle(message))
			else:
				break

	def handle(self, message):
		func, args = loads(message)
		f = getattr(self.instance, func, lambda _: None)
		return dumps(f(*args))

class Client(object):
	def __init__(self, worker_addr):
		sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		sock.connect(worker_addr)
		self.port = Port(sock)

	def shutdown(self):
		self.port.close()

	def __getattr__(self, func):
		def call(*args):
			self.port.write(dumps((func, args)))
			return loads(self.port.read())
		return call

def call(addr, func, args):
	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	sock.connect(addr)
	port = Port(sock)
	port.write(dumps((func, args)))
	return loads(port.read())
