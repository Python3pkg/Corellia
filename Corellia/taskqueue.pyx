import json
import uuid
import time
import fastredis
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
    cdef Semaphore lock

    def __cinit__(self, addr, **kargs):
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
        self.lock = Semaphore()

    cpdef str PUT_TASK(self, char* path, char* method, tuple args, key=None):
        if not key:
            key = uuid.uuid1().hex
        data = self.dumps([key, method, args])
        if self.serialize:
            self.redis.cmd("rpush", (path, data))
        else:
            self.redis.raw_cmd("rpush", (path, data))
        return key

    cpdef tuple GET_TASK(self, char* path):
        self.lock.acquire()
        data = self.redis.cmd("lpop", (path,)).reply()
        self.lock.release()
        if not data:
            return None
        key, method, args = self.loads(data)
        expire_time = time.time() + self.timeout["running"]
        self.redis.cmd("zadd", (RUNNING_SET, expire_time, self.dumps([path, key, method, args])))
        return key, method, args

    cpdef PUT_RESULT(self, char* key, object result):
        result = self.result_dumps(result)
        if self.serialize:
            self.redis.cmd("set", (key, result))
            self.redis.cmd("expire", (key, self.timeout["result"]))
        else:
            self.redis.raw_cmd("set", (key, result))
            self.redis.raw_cmd("expire", (key, self.timeout["result"]))

    cpdef object GET_RESULT(self, char* key):
        if self.serialize:
            result = self.redis.cmd("get", (key,)).reply()
        else:
            result = self.redis.raw_cmd("get", (key,))
        if not result:
            raise ResultNotReadyOrExpired
        result = self.result_loads(result)
        return result

    cpdef watch_task(self):
        self.lock.acquire()
        task = self.redis.raw_cmd("blpop", (path, 0))[1]
        self.lock.release()
        self.redis.cmd("lpush", (path, task))

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


