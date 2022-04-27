import redis
import json


class RedisDumper:
    VALID_TYPES = ('string', 'set', 'list', 'zset', 'hash', 'none')

    def __init__(self, uri):
        self.r = redis.Redis.from_url(uri, decode_responses=True)
        self._first_dump_line = True

    def key_type(self, key):
        return self.r.type(key)

    def _valid_key_type(self, key):
        return self.key_type(key) in self.VALID_TYPES

    def validate_keys(self, keys):
        for key in keys:
            if not self._valid_key_type(key):
                raise Exception("Unknown key type: {} of type {}".format(key, self.key_type(key)))

    def _get_string(self, key):
        return self.r.get(key)
    
    def _get_list(self, key):
        return self.r.lrange(key, 0, -1)
    
    def _get_hash(self, key):
        return self.r.hgetall(key)
    
    def _get_set(self, key):
        if self.r.exists(key):
            return list(self.r.smembers(key))
        return None
    
    def _get_zset(self, key):
        zset = self.r.zrange(key, 0, -1, withscores=True)
        ret = []
        for entry in zset:
            ret += list(entry)
        return ret

    def _get_value(self, key):
        return getattr(self, f"_get_{self.key_type(key)}")(key)

    def dump_keys(self, keys):
        with open('dump.json', 'a') as dump_file:
            dump = ""
            for key in keys:
                if not self._first_dump_line:
                    dump += ",\n"
                else:
                    self._first_dump_line = False
                value = self._get_value(key)
                dump += f'\t"{key}": '.replace("'", '"') + "{" + f'"value": {json.dumps(value)}, "type": "{self.key_type(key)}"' + "}"
            dump_file.write(dump)

    def dump_with_pattern(self, pattern):
        with open('dump.json', 'w') as dump_file:
            dump_file.write('{\n')
        cursor = 0
        while True:
            scan_result = self.r.scan(cursor, match=pattern)
            keys = scan_result[1]
            self.validate_keys(keys)
            self.dump_keys(keys)
            cursor = scan_result[0]
            if cursor == 0:
                break
        with open('dump.json', 'a') as dump_file:
            dump_file.write('\n}')

    def dump_all(self):
        self.dump_with_pattern('*')
