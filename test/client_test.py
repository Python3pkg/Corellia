from Corellia.RedisQueue import Client

client = Client("192.168.70.150", 6379, "TEST")

print client.add(1, "a")
