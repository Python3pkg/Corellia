import json
import uuid
import time
import fastredis
import gevent
from gevent.coros import Semaphore

class ResultNotReadyOrExpired(Exception):
    pass

cdef char* RUNNING_SET = "running"

cdef class TaskQueue(object):

    cdef redis
    cdef dumps
    cdef loads
    cdef result_dumps
    cdef result_loads
    cdef int serialize
    cdef dict timeout
    cdef llock
    cdef wlock
    cdef str path
    cdef watch_let
    cdef int worker_num
    cdef last_data
    cdef int massive

    def __cinit__(self, addr, path, **kargs):
        self.redis = fastredis.FastRedis(addr, **kargs)
        pickler = kargs.get("pickler", json)
        self.dumps = kargs.get("dumps", pickler.dumps)
        self.loads = kargs.get("loads", pickler.loads)
        self.result_dumps = kargs.get("result_dumps", self.dumps)
        self.result_loads = kargs.get("result_loads", self.loads)
        self.serialize = kargs.get("serialize", False)
        self.timeout = {
            "result" : kargs.get("result_timeout", 3600),
            "running" : kargs.get("running_timeout", 60),
        }
        self.path = "%s.tq" % path
        self.worker_num = 0
        self.last_data = None
        self.massive = False

    cpdef str PUT_TASK(self, char* method, tuple args, key=None):
        cdef str data
        if not key:
            key = uuid.uuid1().hex
        data = self.dumps([key, method, args])
        if self.serialize:
            self.redis.cmd("rpush", (self.path, data))
        else:
            self.redis.raw_cmd("rpush", (self.path, data))
        return key

    cpdef tuple GET_TASK(self):
        cdef data
        if self.massive and self.serialize:
            data = None
            self.llock.acquire()
            self.wlock.acquire(blocking=0)
            if self.last_data:
                data = self.last_data
                self.last_data = None
            self.llock.release()
            if not data:
                data = self.redis.cmd("lpop", (self.path,)).reply()
            self.wlock.release()
            if not data:
                return None
        else:
            data = self.redis.raw_cmd("blpop", (self.path, 0))[1]
        key, method, args = self.loads(data)
#        expire_time = time.time() + self.timeout["running"]
#        self.redis.cmd("zadd", (RUNNING_SET, expire_time, self.dumps([self.path, key, method, args])))
        return key, method, args

    cpdef PUT_RESULT(self, char* key, object result):
        result = self.result_dumps(result)
        if self.serialize:
            self.redis.cmd("lpush", (key, result))
            self.redis.cmd("expire", (key, self.timeout["result"]))
        else:
            self.redis.raw_cmd("lpush", (key, result))
            self.redis.raw_cmd("expire", (key, self.timeout["result"]))

    cpdef object GET_RESULT(self, char* key, block):
        cdef result
        if block:
            result = self.redis.raw_cmd("blpop", (key, 0))[1]
        elif self.serialize:
            result = self.redis.cmd("lpop", (key,)).reply()
        else:
            result = self.redis.raw_cmd("lpop", (key,))
        if not result:
            raise ResultNotReadyOrExpired
        result = self.result_loads(result)
        return result

    cpdef prepear_massive_GT(self):
        self.massive = True
        self.wlock = Semaphore()
        self.llock = Semaphore()
        self.watch_let = gevent.spawn(self.watch_task)

    cpdef watch_task(self):
        while 1:
            self.llock.acquire()
            self.wlock.acquire()
            self.wlock.release()
            self.last_data = self.redis.raw_cmd("blpop", (self.path, 0))[1]
            self.llock.release()
            gevent.sleep()

    cpdef flush(self):
        self.redis.submit()

    def MAINTAIN(self):
        interval = self.timeout["runnning"]
        while 1:
            current_time = time.time()
            last_time = current_time - interval
            tasks = self.redis.raw_cmd("zrangebyscore", (RUNNING_SET, last_time, current_time))
            for path, key, method, args in tasks:
                self.redis.cmd("lpush", (path, [key, method, args]))
                self.redis.cmd("zrem", (RUNNING_SET, [path, key, method, args]))
                time.sleep(interval)

def mainatiner(addr, **kargs):
    TaskQueue(addr, **kargs).MAINTAIN()


