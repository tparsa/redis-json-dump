import re
from sortedcontainers import SortedSet

class MockRedis:
    def __init__(self, cache=dict()):
        self.cache = cache

    def get(self, key):
        if key in self.cache:
            return self.cache[key]
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
            return self.cache[key]["value"]
        return None

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
        elif isinstance(self.cache[key], set) or isinstance(self.cache[key], SortedSet):
            return "set"
        return "UnKnOwN"

    def cache_overwrite(self, cache=dict()):
        self.cache = cache
