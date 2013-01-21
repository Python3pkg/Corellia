from taskqueue import TaskQueue, ResultNotReadyOrExpired
import time

cdef class Client(object):

    cdef tq
    cdef str path

    def __cinit__(self, char* addr, char* path, **kargs):
        self.tq = TaskQueue(addr, **kargs)
        self.path = "%s.running" % path

    cpdef str put_task(self, char* method, tuple args, key=None):
        return self.tq.PUT_TASK(self.path, method, args, key)

    cpdef object get_result(self, char* key):
        return self.tq.GET_RESULT(key)

    cpdef finish(self):
        self.tq.flush()

    def __getattr__(self, char* method):
        def f(*args):
            key = self.put_task(method, args)
            while True:
                try:
                    result = self.get_result(key)
                    return result
                except ResultNotReadyOrExpired:
                    time.sleep(0.001)
        return f