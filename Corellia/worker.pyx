from gevent import monkey; monkey.patch_all()
import gevent
from taskqueue import TaskQueue

cdef class WorkerPool(object):

    cdef tq
    cdef int num
    cdef workers

    def __cinit__(self, addr, path, **kargs):
        self.tq = TaskQueue(addr, path, **kargs)
        if kargs.get("mass", False):
            self.tq.prepear_massive_GT()
        self.num = kargs.get("num", 10)

    def run(self, cls, *args, **kargs):
        self.workers = []
        for _ in range(self.num):
            self.workers.append(gevent.spawn(self.run_with, cls(*args)))
        gevent.joinall(self.workers)

    cpdef run_with(self, object ins):
        while 1:
            task =  self.tq.GET_TASK()
            if not task:
                continue
            key, method, args = task
            func = getattr(ins, method, None)
            if func:
                # try:
                result = func(*args)
                # except Exception, e:
                #   result = str(e)
            else:
                result = "No Such Method!"
            self.tq.PUT_RESULT(key, result)