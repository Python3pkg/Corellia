Corellia
========

> "The bigger the galaxy, the sweeter the homecoming."  ―Corellian proverb

## About

Corellia was the capital planet of the Corellian system, which included Selonia, Drall, Tralus, and Talus.

For now, Corellia is a distributed task queue for Python, it contains two parts:

1. Convenient tools to quick transform a Python instance into a simple socket server, that is, make every instance method to be callable from remote hosts.
2. A simple task dispatch queue. Corellia can manager large mount of socket server with same functions, for example, transformed from the instance with same class. when request comes, Corellia choose one of the server fairly to handle the request and reply the response.

Corellia is in very, very early develpment, and aims to such goals:

1. Quickly develope and deploy cluster program written in Python without warrying about underlaying network realated problems.         

    For example, you can just write down a normal python class, and push it to the Corellia. The Corellian system handles the others: transforming instances of such class socket servers, deploying on certain scale cluster, enlarging or reducing the scale of the deployment automatically, and so on.
    
2. Fast task queues for queueing requests to the servers and processing them fairly in different workers in the cluster. And also, Corellian system store the response somewhere whit asynchronous calls.

Aditional, we also want to add the tools to talk with PaSS cloud in this system. We want to launch/shutdown VMs automatically when needed, and automatically setup the environment the worker need. The environment can cantains necessary OS components, applications, supporting library, and the resources and other files/modules written by users. This may be done by some automatically generated scripts running by a publish key authored ssh channel later.

## Usage

To setup the broker (task queue):

    from Corellia import Broker
    
    client_port = 8899
    worker_port = 8898
    b = Broker(client_port, worker_port)
	b.run()
	    
To start a server based on certain class:

    from Corellia import Worker
    
    class MathCloud(object):
    	def add(self, a, b):
    		return a+b
    
    borker_addr = ("localhost", 8898)
	Worker(MathCloud).run(borker_addr)
	    
And to call the server remotely:

    from Corellia import Client, call
    
    borker_addr = ("localhost", 8898)
    c = Client(borker_addr)
    print c.add(6, 8)
    print call(borker_addr, "add", (6, 8))
    
## Detailed Design

**TODO**
