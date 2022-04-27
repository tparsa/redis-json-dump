class RedisDumper:
    VALID_TYPES = ('string', 'set', 'list', 'zset', 'hash', 'none')

    def __init__(self, uri):
        pass

    def key_type(self, key):
        raise NotImplementedError()

    def _valid_key_type(self, key):
        raise NotImplementedError()

    def validate_keys(self, keys):
        raise NotImplementedError()

    def _get_string(self, key):
        raise NotImplementedError()
    
    def _get_list(self, key):
        raise NotImplementedError()
    
    def _get_hash(self, key):
        raise NotImplementedError()
    
    def _get_set(self, key):
        raise NotImplementedError()
    
    def _get_zset(self, key):
        raise NotImplementedError()

    def _get_value(self, key):
        return getattr(self, f"_get_{self.key_type(key)}")(key)

    def dump_keys(self, keys):
        raise NotImplementedError()

    def dump_with_pattern(self, pattern):
        raise NotImplementedError()

    def dump_all(self):
        self.dump_with_pattern("*")
