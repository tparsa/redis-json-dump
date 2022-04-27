import unittest
import os
from unittest.mock import call, patch, MagicMock, Mock
from single_redis.dumper import RedisDumper
from mock.redis import MockRedis
from sortedcontainers import SortedSet

class MockPathExists(object):
    def __init__(self, return_value):
        self.received_args = None
        self.return_value = return_value

    def __call__(self, *args, **kwargs):
        self.received_args = args
        return self.return_value

class RedisDumperTest(unittest.TestCase):
    def setUp(self):
        self._orig_pathexists = os.path.exists
        os.path.exists = MockPathExists(True)

        redis_cache = {
            "foo": "bar", 
            "barfoo": {"__type": "hash", "value": {"Foo": "Bar"}},
            "foo2": {"__type": "set", "value": ["salam", "aleyk"]}
        }

        self.mock_redis_obj = MockRedis(redis_cache)

        self.mock_redis_method = MagicMock()
        self.mock_redis_method.get = Mock(side_effect=self.mock_redis_obj.get)
        self.mock_redis_method.hgetall = Mock(side_effect=self.mock_redis_obj.hgetall)
        self.mock_redis_method.set = Mock(side_effect=self.mock_redis_obj.set)
        self.mock_redis_method.hset = Mock(side_effect=self.mock_redis_obj.hset)
        self.mock_redis_method.exists = Mock(side_effect=self.mock_redis_obj.exists)
        self.mock_redis_method.scan = Mock(side_effect=self.mock_redis_obj.scan)
        self.mock_redis_method.type = Mock(side_effect=self.mock_redis_obj.type)
        self.mock_redis_method.smembers = Mock(side_effect=self.mock_redis_obj.smembers)
    
    @patch("redis.Redis.from_url")
    def test_file_opennings(self, mock_redis):
        mock_redis.return_value = self.mock_redis_method
        with patch('builtins.open', unittest.mock.mock_open()) as open_mock:
            dumper = RedisDumper("redis://localhost:6379")
            dumper.dump_with_pattern("foo")
        open_calls = [call('dump.json', 'w'), call('dump.json', 'a'), call('dump.json', 'a')]
        open_mock.assert_has_calls(open_calls, any_order=True)

    @patch("redis.Redis.from_url")
    def test_dump_with_starless_pattern(self, mock_redis):
        mock_redis.return_value = self.mock_redis_method
        with patch('builtins.open', unittest.mock.mock_open()) as open_mock:
            dumper = RedisDumper("redis://localhost:6379")
            dumper.dump_with_pattern("foo")
        handle = open_mock()
        write_calls = [call('{\n'), call('\t"foo": {"value": "bar", "type": "string"}'), call('\n}')]
        handle.write.assert_has_calls(write_calls, any_order=False)
    

    @patch("redis.Redis.from_url")
    def test_dump_with_star_pattern(self, mock_redis):
        mock_redis.return_value = self.mock_redis_method
        with patch('builtins.open', unittest.mock.mock_open()) as open_mock:
            dumper = RedisDumper("redis://localhost:6379")
            dumper.dump_with_pattern("foo*")
        handle = open_mock()
        write_calls = [call('{\n'), call('\t"foo": {"value": "bar", "type": "string"},\n\t"foo2": {"value": ["salam", "aleyk"], "type": "set"}'), call('\n}')]
        handle.write.assert_has_calls(write_calls, any_order=False)
    
    @patch("redis.Redis.from_url")
    def test_dump_all(self, mock_redis):
        mock_redis.return_value = self.mock_redis_method
        with patch('builtins.open', unittest.mock.mock_open()) as open_mock:
            dumper = RedisDumper("redis://localhost:6379")
            dumper.dump_all()
        handle = open_mock()
        write_calls = [
            call("{\n"),
            call('\t"foo": {"value": "bar", "type": "string"},\n\t"barfoo": {"value": {"Foo": "Bar"}, "type": "hash"},\n\t"foo2": {"value": ["salam", "aleyk"], "type": "set"}'),
            call('\n}')
        ]
        handle.write.assert_has_calls(write_calls, any_order=False)
