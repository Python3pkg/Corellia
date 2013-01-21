import redis
import gevent
from gevent.coros import Semaphore


cdef class Reply(object):
    
    cdef int set
    cdef value
    cdef float interval

    def __cinit__(self, float interval):
        self.set = False
        self.interval = interval

    def set_value(self, value):
        self.value = value
        self.set = True

    def reply(self):
        while not self.set:
            gevent.sleep(self.interval)
        return self.value

cdef class FastRedis(object):

    cdef redis
    cdef pipeline
    cdef lock
    cdef let
    cdef int empty
    cdef count
    cdef replies
    cdef float interval

    def __cinit__(FastRedis self, char* addr, **kargs):
        if ":" in addr:
            host, port = addr.split(":")
            port = int(port)
        else:
            host = addr
            port = 6379
        self.redis = redis.StrictRedis(host=host, port=port)
        self.pipeline = self.redis.pipeline()
        self.interval = kargs.get("interval", 0.01)
        self.lock = Semaphore()
        self.let= gevent.spawn(self.execute)
        self.empty = True
        self.replies = []

    cpdef Reply cmd(FastRedis self, char* cmd, tuple args):
        cdef Reply reply = Reply(self.interval)
        self.lock.acquire()
        getattr(self.pipeline, cmd)(*args)
        self.empty = False
        self.replies.append(reply)
        self.lock.release()
        return reply

    cpdef raw_cmd(FastRedis self, char* cmd, tuple args):
        return getattr(self.redis, cmd)(*args)

    cpdef execute(FastRedis self):
        while 1:
            self.submit()
            gevent.sleep(self.interval)

    cpdef submit(FastRedis self):
        self.lock.acquire()
        if not self.empty:
            replies = self.pipeline.execute()
            for i in xrange(len(replies)):
                self.replies[i].set_value(replies[i])
            self.replies = []
            self.empty = True
        self.lock.release()
