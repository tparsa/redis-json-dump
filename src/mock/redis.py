import re
from datetime import timedelta

from src.redis_lib.io import is_iterable
from .args import ArgsMock

class MockRedis:
    def __init__(self, cache=dict(), ttls=dict()):
        self.cache = cache
        self.__return_values = []
        self.__exceptions = []
        self.__pipeline_enabled = False
        self.__non_decoded_queries = {}
        self.ttls = ttls
        self.command_stack = [0] * 1001

    def get_nodes(self):
        node = ArgsMock()
        node.add("redis_connection", self)
        return [node]

    def delete(self, key):
        val = 0
        if key in self.cache:
            del self.cache[key]
            val = 1
        # self.__return_values.append(val)
        # self.__non_decoded_queries[len(self.__return_values)-1] = True
        return val

    def get(self, key):
        if key in self.cache:
            if isinstance(self.cache[key], str):
                val = self.cache[key]
                self.__return_values.append(val)
                return val
            else:
                ex = Exception("WRONGTYPE")
                self.__exceptions.append(ex)
                if not self.__pipeline_enabled:
                    raise ex
        self.__return_values.append(None)
        return None  # return nil

    def set(self, key, value, *args, **kwargs):
        if self.cache:
            if key in self.cache and not isinstance(self.cache[key], str):
                ex = Exception("WRONGTYPE")
                self.__exceptions.append(ex)
                if not self.__pipeline_enabled:
                    raise ex
            else:
                self.cache[key] = value
                self.__return_values.append(True)
                self.__non_decoded_queries[len(self.__return_values)-1] = True
                return "OK"
        self.__return_values.append(False)
        return None  # return nil in case of some issue

    def setex(self, key, ttl, value):
        if self.cache:
            self.cache[key] = value
            self.ttls[key] = ttl
            self.__return_values.append(True)
            self.__non_decoded_queries[len(self.__return_values)-1] = True
            return "OK"
        self.__return_values.append(False)
        return None  # return nil in case of some issue

    def hgetall(self, key):
        if key in self.cache:
            if isinstance(self.cache, dict) and "__type" in self.cache[key] and self.cache[key]["__type"] == "hash":
                val = self.cache[key]["value"]
                self.__return_values.append(val)
                return val
            else:
                ex = Exception("WRONGTYPE")
                self.__exceptions.append(ex)
                if not self.__pipeline_enabled:
                    raise ex
        self.__return_values.append(None)
        return None  # return nil

    def hmset(self, key, value, *args, **kwargs):
        if self.cache:
            if key in self.cache:
                if not isinstance(self.cache[key], dict) or "__type" not in self.cache[key] or self.cache[key]["__type"] != "hash":
                    ex = Exception("WRONGTYPE")
                    self.__exceptions.append(ex)
                    if not self.__pipeline_enabled:
                        raise ex
            if key not in self.cache:
                self.cache[key] = dict()
                self.cache[key]["value"] = dict()
            self.cache[key]["__type"] = "hash"
            self.cache[key]["value"].update(value)
            self.__return_values.append(True)
            self.__non_decoded_queries[len(self.__return_values)-1] = True
            return 1
        self.__return_values.append(False)
        return None  # return nil in case of some issue

    def smembers(self, key):
        if key in self.cache:
            if isinstance(self.cache[key], dict) and self.cache[key]["__type"] == "set":
                val = self.cache[key]["value"]
                self.__return_values.append(val)
                return val
            else:
                ex = Exception("WRONGTYPE")
                self.__exceptions.append(ex)
                if not self.__pipeline_enabled:
                    raise ex
        self.__return_values.append(None)
        return None
    
    def sadd(self, key, val, *args):
        if not key in self.cache:
            self.cache[key] = {"__type": "set", "value": []}
        if "__type" not in self.cache[key] or self.cache[key]["__type"] != "set":
            ex = Exception("WRONGTYPE")
            self.__exceptions.append(ex)
            if not self.__pipeline_enabled:
                raise ex
        self.cache[key]["value"].append(val)
        self.__return_values.append(True)
        self.__non_decoded_queries[len(self.__return_values)-1] = True
        if len(args):
            return self.sadd(key, args[0], *args[1:])
        return "(integer) 1"
    
    def lrange(self, key, start, end):
        if key in self.cache:
            if isinstance(self.cache[key], list):
                if end == -1:
                    val = self.cache[key][start:]
                else:
                    val = self.cache[key][start:end]
                self.__return_values.append(val)
                return val
            elif isinstance(self.cache[key], dict) and self.cache[key].get("__type", None) == "list":
                if end == -1:
                    val = self.cache[key]["value"][start:]
                else:
                    val = self.cache[key]["value"][start:end]
                self.__return_values.append(val)
                return val
            else:
                ex = Exception("WRONGTYPE")
                self.__exceptions.append(ex)
                if not self.__pipeline_enabled:
                    raise ex
        self.__return_values.append(None)
        return None
    
    def rpush(self, key, val):
        if not key in self.cache:
            self.cache[key] = []
        if not isinstance(self.cache[key], list):
            ex = Exception("WRONGTYPE")
            self.__exceptions.append(ex)
            if not self.__pipeline_enabled:
                raise ex
        self.cache[key].append(val)
        self.__return_values.append(True)
        self.__non_decoded_queries[len(self.__return_values)-1] = True
        return "(integer) 1"
    
    def zrange(self, key, start, end, withscores=False):
        if key in self.cache:
            if isinstance(self.cache[key], dict) and self.cache[key].get("__type", None) == "zset":
                val = self.cache[key]["value"]
            elif isinstance(self.cache[key], dict):
                ex = Exception("WRONGTYPE")
                self.__exceptions.append(ex)
                if not self.__pipeline_enabled:
                    raise ex
            else:
                val = self.cache[key]
            if not withscores:
                val = [v for i, v in enumerate(val) if i % 2 == 0]
            
            self.__return_values.append(val)
            return val
        self.__return_values.append(None)
        return None

    def exists(self, key):
        val = 0
        if key in self.cache:
            val = 1
        self.__return_values.append(val)
        return val
    
    def scan(self, cursor, match=None):
        return self._scan(cursor, match, False)
    
    def _scan(self, cursor, match=None, internal=False):
        ret = []
        pattern_matcher = re.compile("^" + match.replace("*", ".*") + "$")
        for key in self.cache:
            if pattern_matcher.match(key):
                ret.append(key)
        val = [0, ret]
        if not internal:
            self.__return_values.append(tuple(val))
        return val
    
    def scan_iter(self, match, count):
        keys = self._scan(0, match, True)[1]
        for key in keys:
            yield self._encode(key)

    def type(self, key):
        val = "UnKnOwN"
        if key not in self.cache:
            val = "none"
        if isinstance(self.cache[key], str):
            val = "string"
        elif isinstance(self.cache[key], list):
            val = "list"
        elif isinstance(self.cache[key], dict):
            if "__type" in self.cache[key]:
                val = self.cache[key]["__type"]
            else:
                val = "hash"
        elif isinstance(self.cache[key], set):
            val = "set"
        self.__return_values.append(val)
        return val
    
    def ttl(self, key):
        val = -2
        if key in self.cache:
            val = -1
            if key in self.ttls:
                val = self.ttls[key]
                if isinstance(val, timedelta):
                    val = val.total_seconds()
        self.__return_values.append(val)
        self.__non_decoded_queries[len(self.__return_values)-1] = True
        return val

    def cache_overwrite(self, cache=dict()):
        self.cache = cache

    def pipeline(self, transaction=False):
        self.__pipeline_enabled = True
        self.__return_values = []
        self.__exceptions = []
        self.__non_decoded_queries = {}
        return self

    def _encode(self, val):
        if isinstance(val, dict):
            return {self._encode(k):self._encode(v) for k, v in val.items()}
        if is_iterable(val):
            return [self._encode(i) for i in val]
        return str(val).encode('utf-8')
    
    def execute(self):
        ex = None
        ret = self.__return_values

        if len(self.__exceptions):
            ex = self.__exceptions[0]
        
        self.__exceptions = []
        self.__return_values = []
        # SHOULD MOVE TO GETTERS SO THAT TTL WON'T BE AFFECTED

        ret = [
            (self._encode(r) if idx not in self.__non_decoded_queries else r) 
            for idx, r in enumerate(ret)
        ]
        self.__non_decoded_queries = {}

        if ex:
            raise ex
        return ret
