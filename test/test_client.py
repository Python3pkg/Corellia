import sys  
sys.path.append('..')
from Corellia import Client, call

if __name__ == '__main__':
	c = Client(("localhost", 8899))
	print c.add(6, 8)

	print call(("localhost", 8899), "add", (6, 8))