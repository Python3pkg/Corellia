from Corellia.RedisQueue import Worker


class TEST(object):
    def add(self, a, b):
        return a+b


Worker("192.168.70.150", 6379, "TEST").run(TEST)