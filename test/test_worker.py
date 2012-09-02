import sys  
sys.path.append('..')
from Corellia import Worker

class MathCloud(object):
	def add(self, a, b):
		return a+b

if __name__ == '__main__':
	Worker(MathCloud).run(8888)