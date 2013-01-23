from fastredis import FastRedis

class KVStore(object):
    def __init__(self, addr, name, **kargs):
        self.redis = FastRedis(addr, **kargs)
        self.name = "%s.KVS" % name

    def set(self, key, value, serialize=True):
        if serialize:
            self.redis.cmd("hset", (self.name, key, value))
        else:
            self.redis.raw_cmd("hset", (self.name, key, value))

    def get(self, key, serialize=False):
        if serialize:
            self.redis.cmd("hget", (self.name, key)).reply()
        else:
            self.redis.raw_cmd("hget", (self.name, key))
