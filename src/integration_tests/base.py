from abc import abstractmethod

from .utils import *


class TestBase:
    def __init__(self, count_foreach, source_redis_url, dest_redis_url):
        self.setUpClass(count_foreach, source_redis_url, dest_redis_url)
        self.keys = []
    
    @abstractmethod
    def setUpClass(self, count_foreach, source_redis_url, dest_redis_url):
        ...

    def test(self):
        from ..main import CLI
        CLI(self.dump_args).execute()
        CLI(self.restore_args).execute()

        self._check_integrity()
    
    def _check_integrity(self):
        self._check_integrity_of_two_redises(self.source_redis, self.dest_redis)
        self._check_integrity_of_two_redises(self.dest_redis, self.source_redis)

    def _check_integrity_of_two_redises(self, redis1, redis2):
        for key in self.keys:
            self._check_key_existence(key, redis1, redis2)
            self._check_single_key_value_integrity(key, redis1, redis2)
            self._check_single_key_ttl_integrity(key, redis1, redis2)

    def _check_key_existence(self, key, redis1, redis2):
        assert redis1.exists(key) == redis2.exists(key)
    
    def _check_single_key_value_integrity(self, key, redis1, redis2):
        data_type = redis1.type(key)
        if data_type:
            data_type = data_type.decode("utf-8")
            val1 = HANDLERS[data_type].call_for(redis1, key)
            val2 = HANDLERS[data_type].call_for(redis2, key)
            DIFF_CHECKER[data_type](val1, val2)
    
    def _check_single_key_ttl_integrity(self, key, redis1, redis2):
        ttl1 = redis1.ttl(key)
        ttl2 = redis2.ttl(key)

        assert abs(ttl1 - ttl2) < 5
        