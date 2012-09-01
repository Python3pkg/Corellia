import gevent
import gevent.socket as socket
from gevent.queue import Queue
from port import Port

client_port = "8888"
worker_port = "8889"

tdq = Queue()

def waiter(s0):
	while True:
		p0 = Port(s0)
		req = p0.read()
		if not buf: 
			break
		p1 = tdq.get()
		while not p1.write(buf):
			p1 = tdq.get()
		req = p1.read()
		if req:
			tdq.put(p1)
			if not p0.write(req):
				break

def listen_client():
	client_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	client_sock.bind(("", client_port))
	while True:
		sock, _ = client_sock.accept()
		gevent.spawn(waiter, sock)

def listen_worker():
	worker_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	worker_sock.bind(("", worker_port))
	while True:
		sock, _ = worker_sock.accept()
		tdq.put(Port(sock)) 

gevent.joinall([
	gevent.spawn(listen_worker),
	gevent.spawn(listen_client)
	])

		