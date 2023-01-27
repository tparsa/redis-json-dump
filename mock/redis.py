import re


class MockRedis:
    def __init__(self, cache=dict()):
        self.cache = cache

    def get(self, key):
        if key in self.cache:
            if isinstance(self.cache[key], str):
                return self.cache[key]
            else:
                raise Exception("WRONGTYPE")
        return None  # return nil

    def set(self, key, value, *args, **kwargs):
        if self.cache:
           self.cache[key] = value
           return "OK"
        return None  # return nil in case of some issue

    def hgetall(self, key):
        if key in self.cache:
            return self.cache[key] if "__type" not in self.cache[key] else self.cache[key]["value"]
        return None  # return nil

    def hset(self, hash, key, value, *args, **kwargs):
        if self.cache:
           self.cache[hash][key] = value
           return 1
        return None  # return nil in case of some issue

    def smembers(self, key):
        if key in self.cache:
            if isinstance(self.cache[key], dict) and self.cache[key]["__type"] == "set":
                return self.cache[key]["value"]
            else:
                raise Exception("WRONGTYPE")
        return None
    
    def sadd(self, key, val):
        if not key in self.cache:
            self.cache[key] = {"__type": "set", "value": []}
        self.cache[key]["value"].append(val)
        return "(integer) 1"
    
    def lrange(self, key, start, end):
        if key in self.cache:
            if isinstance(self.cache[key], list):
                if end == -1:
                    return self.cache[key][start:]
                else:
                    return self.cache[key][start:end]
            else:
                raise Exception("WRONGTYPE")
        return None
    
    def rpush(self, key, val):
        if not key in self.cache:
            self.cache[key] = []
        self.cache[key].append(val)
        return "(integer) 1"

    def exists(self, key):
        if key in self.cache:
            return 1
        return 0
    
    def scan(self, cursor, match=None):
        ret = []
        pattern_matcher = re.compile("^" + match.replace("*", ".*") + "$")
        for key in self.cache:
            if pattern_matcher.match(key):
                ret.append(key)
        return [0, ret]

    def type(self, key):
        if key not in self.cache:
            return "none"
        if isinstance(self.cache[key], str):
            return "string"
        elif isinstance(self.cache[key], list):
            return "list"
        elif isinstance(self.cache[key], dict):
            if "__type" in self.cache[key]:
                return self.cache[key]["__type"]
            return "hash"
        elif isinstance(self.cache[key], set):
            return "set"
        return "UnKnOwN"

    def cache_overwrite(self, cache=dict()):
        self.cache = cache
