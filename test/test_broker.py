import sys  
sys.path.append('..')
from Corellia import Broker

if __name__ == '__main__':
	b = Broker(8899, 8898)
	b.run()