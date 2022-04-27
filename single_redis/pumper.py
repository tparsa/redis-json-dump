import redis
import json

class RedisPumper:
    def __init__(self, uri):
        self.r = redis.Redis.from_url(uri, decode_responses=True)
    
    def _pump_string(self, key, value):
        self.r.set(key, value)
    
    def _pump_set(self, key, value):
        value = json.loads(f"[{value[1:-1]}]".replace("'", '"'))
        self.r.sadd(key, *value)
    
    def _pump_list(self, key, value):
        value = json.loads(value.replace("'", '"'))
        self.r.delete(key)
        self.r.rpush(key, *value)

    def _pump_hash(self, key, value):
        value = json.loads(value.replace("'", '"'))
        self.r.hmset(key, value)
    
    def _pump_zset(self, key, value):
        value = json.loads(value.replace("'", '"'))
        for idx in range(0, len(value), 2):
            self.r.zadd(key, {value[idx]: value[idx+1]})

    def _pump_with_type(self, key, value, type):
        getattr(self, f"_pump_{type}")(key[1:-1], value)

    def _pump_line(self, line):
        splitted_line = line.split(":")
        [key, value] = [x.strip() for x in [splitted_line[0], ':'.join(splitted_line[1:])]]
        if value[-1] == ",":
            value = value[:-1]
        value = json.loads(value)
        self._pump_with_type(key, value['value'], value['type'])

    def pump_with_pattern(self, pattern):
        with open("dump.json") as dump_file:
            for line in dump_file:
                if line[0] not in "\{\}":
                    self._pump_line(line)

    def pump_all(self):
        pass
