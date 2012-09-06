import sys  
sys.path.append('..')
from Corellia import Client

if __name__ == '__main__':
	c = Client(("localhost", 8899))
	print c.add(1, 0)