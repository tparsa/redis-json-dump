import redis
import json

class RedisPumper:
    def __init__(self, uri):
        pass
    
    def _pump_string(self, key, value):
        raise NotImplementedError()
    
    def _pump_set(self, key, value):
        raise NotImplementedError()
    
    def _pump_list(self, key, value):
        raise NotImplementedError()

    def _pump_hash(self, key, value):
        raise NotImplementedError()
    
    def _pump_zset(self, key, value):
        raise NotImplementedError()

    def _pump_with_type(self, key, value, type):
        getattr(self, f"_pump_{type}")(key[1:-1], value)

    def _pump_line(self, line):
        raise NotImplementedError()

    def pump_with_pattern(self, pattern):
        raise NotImplementedError()

    def pump_all(self):
        self.pump_with_pattern("*")
