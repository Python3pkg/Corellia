from gevent import monkey; monkey.patch_all()
import gevent
from taskqueue import TaskQueue

class WorkerPool(object):
    def __init__(self, addr, path, **kargs):
        self.tq = TaskQueue(addr, **kargs)
        self.path = "%s.running" % path
        self.workers = []

    def run(self, cls, *args, **kargs):
        num = kargs.get("num", 1)
        for _ in range(num):
            self.workers.append(gevent.spawn(self.run_with, cls(*args)))
        gevent.joinall(self.workers)

    def run_with(self, ins):
        while 1:
            task =  self.tq.GET_TASK(self.path)
            if not task:
                continue
            key, method, args =
            print "Processing", key
            func = getattr(ins, method, None)
            if func:
                try:
                    result = func(*args)
                except Exception, e:
                    result = str(e)
            else:
                result = "No Such Method!"
            self.tq.PUT_RESULT(key, result)