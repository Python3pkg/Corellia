from taskqueue import TaskQueue, ResultNotReadyOrExpired
import time

cdef class Client(object):

    cdef tq
    cdef str path

    def __cinit__(self, char* addr, char* path, **kargs):
        self.tq = TaskQueue(addr, path, **kargs)

    cpdef str put_task(self, char* method, tuple args, key=None):
        return self.tq.PUT_TASK(method, args, key)

    cpdef object get_result(self, char* key, block=True):
        return self.tq.GET_RESULT(key, block)

    cpdef finish(self):
        self.tq.flush()

    def __getattr__(self, char* method):
        return lambda *args: \
            self.get_result(self.put_task(method, args), \
                            block=True)