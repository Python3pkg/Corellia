import gevent
import gevent.socket as socket
from gevent.queue import Queue
from port import Port


class Broker(object):
	def __init__(self, client_port, worker_port):
		self.client_port = client_port
		self.worker_port = worker_port
		self.tdp = Queue()

	def waiter(self, s0):
		while True:
			p0 = Port(s0)
			req = p0.read()
			if not buf: 
				break
			p1 = self.tdq.get()
			while not p1.write(buf):
				p1 = self.tdq.get()
			req = p1.read()
			if req:
				self.tdq.put(p1)
				if not p0.write(req):
					break

	def listen_client(self):
		client_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		client_sock.bind(("", client_port))
		while True:
			sock, _ = client_sock.accept()
			gevent.spawn(waiter, sock)

	def listen_worker(self):
		worker_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		worker_sock.bind(("", worker_port))
		while True:
			sock, _ = worker_sock.accept()
			self.tdq.put(Port(sock)) 

	def run(self):
		gevent.joinall([
			gevent.spawn(self.listen_worker),
			gevent.spawn(self.listen_client)
			])

		