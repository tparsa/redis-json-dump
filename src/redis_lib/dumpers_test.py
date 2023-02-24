import unittest
from io import StringIO

from .dumpers import JSONDumper, CSVDumper
from src.mock.file import FileMock
from src.mock.redis_io import RedisIOMock


class JSONDumperTest(unittest.TestCase):
    def setUp(self) -> None:
        self.maxDiff = None
        self.no_ttls_file_mock = FileMock()
        self.json_dumper_no_ttls = JSONDumper(self.no_ttls_file_mock, 0, False, False)

        self.with_ttls_file_mock = FileMock()
        self.json_dumper_with_ttls = JSONDumper(self.with_ttls_file_mock, 1, False, False)

        redis_io_mock_data = (
            ("hash", "foo", {"foo": "bar"}, 10),
            ("string", "str", "test", 20),
            ("list", "list", ["1", "2", "3"], -1),
            ("zset", "zset", ["1", "1", "2", "10", "3", "1"], 1),
            ("set", "set", ["1", "2", "3"], -1)
        )
        self.redis_io_mock = RedisIOMock(redis_io_mock_data)
    
    def test_dump_no_ttls(self):
        self.json_dumper_no_ttls.dump(self.redis_io_mock)
        expected_value = '''
{"key": "foo", "type": "hash", "value": {"foo": "bar"}, "ttl": -1}
{"key": "str", "type": "string", "value": "test", "ttl": -1}
{"key": "list", "type": "list", "value": ["1", "2", "3"], "ttl": -1}
{"key": "zset", "type": "zset", "value": ["1", "1", "2", "10", "3", "1"], "ttl": -1}
{"key": "set", "type": "set", "value": ["1", "2", "3"], "ttl": -1}
        '''
        expected_value = expected_value.strip() + "\n"

        self.assertEqual(self.no_ttls_file_mock.get_content(), expected_value)
    
    def test_dump_with_ttls(self):
        self.json_dumper_with_ttls.dump(self.redis_io_mock)
        expected_value = '''
{"key": "foo", "type": "hash", "value": {"foo": "bar"}, "ttl": 10}
{"key": "str", "type": "string", "value": "test", "ttl": 20}
{"key": "list", "type": "list", "value": ["1", "2", "3"], "ttl": -1}
{"key": "zset", "type": "zset", "value": ["1", "1", "2", "10", "3", "1"], "ttl": 1}
{"key": "set", "type": "set", "value": ["1", "2", "3"], "ttl": -1}
        '''
        expected_value = expected_value.strip() + "\n"

        self.assertEqual(self.with_ttls_file_mock.get_content(), expected_value)
    
    def test_restore_no_ttls(self):
        redis_io_restore_mock = RedisIOMock(list())
        to_be_restore_data = '''
{"key": "foo", "type": "hash", "value": {"foo": "bar"}, "ttl": -1}
{"key": "str", "type": "string", "value": "test", "ttl": -1}
{"key": "list", "type": "list", "value": ["1", "2", "3"], "ttl": -1}
{"key": "zset", "type": "zset", "value": ["1", "1", "2", "10", "3", "1"], "ttl": -1}
{"key": "set", "type": "set", "value": ["1", "2", "3"], "ttl": -1}
        '''.strip() + "\n"
        self.no_ttls_file_mock.content = to_be_restore_data
        self.json_dumper_no_ttls.restore(redis_io_restore_mock)
        self.assertEqual(redis_io_restore_mock.get_data(), self.redis_io_mock.get_data(ttl=False))

    def test_restore_with_ttls(self):
        redis_io_restore_mock = RedisIOMock(list())
        to_be_restore_data = '''
{"key": "foo", "type": "hash", "value": {"foo": "bar"}, "ttl": 10}
{"key": "str", "type": "string", "value": "test", "ttl": 20}
{"key": "list", "type": "list", "value": ["1", "2", "3"], "ttl": -1}
{"key": "zset", "type": "zset", "value": ["1", "1", "2", "10", "3", "1"], "ttl": 1}
{"key": "set", "type": "set", "value": ["1", "2", "3"], "ttl": -1}
        '''.strip() + "\n"
        self.with_ttls_file_mock.content = to_be_restore_data
        self.json_dumper_with_ttls.restore(redis_io_restore_mock)
        self.assertEqual(redis_io_restore_mock.get_data(), self.redis_io_mock.get_data())


class CSVDumperTest(unittest.TestCase):
    def setUp(self) -> None:
        self.maxDiff = None
        self.no_ttls_file_mock = FileMock()
        self.csv_dumper_no_ttls = CSVDumper(self.no_ttls_file_mock, 0, False, False)

        self.with_ttls_file_mock = FileMock()
        self.csv_dumper_with_ttls = CSVDumper(self.with_ttls_file_mock, 1, False, False)

        redis_io_mock_data = (
            ("hash", "foo", {"foo": "bar"}, 10),
            ("string", "str", "test", 20),
            ("list", "list", ["1", "2", "3"], -1),
            ("zset", "zset", ["1", "1", "2", "10", "3", "1"], 1),
            ("set", "set", ["1", "2", "3"], -1)
        )
        self.redis_io_mock = RedisIOMock(redis_io_mock_data)
    
    def test_dump_no_ttls(self):
        self.csv_dumper_no_ttls.dump(self.redis_io_mock)
        expected_value = '''
foo,hash,{'foo': 'bar'},-1
str,string,test,-1
list,list,"['1', '2', '3']",-1
zset,zset,"['1', '1', '2', '10', '3', '1']",-1
set,set,"['1', '2', '3']",-1
        '''
        expected_value = expected_value.strip() + "\n"
        self.assertEqual(self.no_ttls_file_mock.get_content(), expected_value)
    
    def test_dump_with_ttls(self):
        self.csv_dumper_with_ttls.dump(self.redis_io_mock)
        expected_value =  '''
foo,hash,{'foo': 'bar'},10
str,string,test,20
list,list,"['1', '2', '3']",-1
zset,zset,"['1', '1', '2', '10', '3', '1']",1
set,set,"['1', '2', '3']",-1
        '''
        expected_value = expected_value.strip() + "\n"

        self.assertEqual(self.with_ttls_file_mock.get_content(), expected_value)
    
    def test_restore_no_ttls(self):
        redis_io_restore_mock = RedisIOMock(list())
        to_be_restore_data = '''
foo,hash,{'foo': 'bar'},-1
str,string,test,-1
list,list,"['1', '2', '3']",-1
zset,zset,"['1', '1', '2', '10', '3', '1']",-1
set,set,"['1', '2', '3']",-1
        '''.strip()
        csv_dumper = CSVDumper(StringIO(to_be_restore_data), False, False, False)
        csv_dumper.restore(redis_io_restore_mock)
        self.assertEqual(redis_io_restore_mock.get_data(), self.redis_io_mock.get_data(ttl=False))

    def test_restore_with_ttls(self):
        redis_io_restore_mock = RedisIOMock(list())
        to_be_restore_data = '''
foo,hash,{'foo': 'bar'},10
str,string,test,20
list,list,"['1', '2', '3']",-1
zset,zset,"['1', '1', '2', '10', '3', '1']",1
set,set,"['1', '2', '3']",-1
        '''.strip() + "\n"
        csv_dumper = CSVDumper(StringIO(to_be_restore_data), False, False, False)
        csv_dumper.restore(redis_io_restore_mock)
        self.assertEqual(redis_io_restore_mock.get_data(), self.redis_io_mock.get_data())


# class MockPathExists(object):
#     def __init__(self, return_value):
#         self.received_args = None
#         self.return_value = return_value

#     def __call__(self, *args, **kwargs):
#         self.received_args = args
#         return self.return_value

# class SingleRedisDumperTest(unittest.TestCase):
#     def setUp(self):
#         self._orig_pathexists = os.path.exists
#         os.path.exists = MockPathExists(True)

#         redis_cache = {
#             "foo": "bar", 
#             "barfoo": {"__type": "hash", "value": {"Foo": "Bar"}},
#             "foo2": {"__type": "set", "value": ["salam", "aleyk"]}
#         }

#         self.mock_redis_obj = MockRedis(redis_cache)

#         self.mock_redis_method = MagicMock()
#         self.mock_redis_method.get = Mock(side_effect=self.mock_redis_obj.get)
#         self.mock_redis_method.hgetall = Mock(side_effect=self.mock_redis_obj.hgetall)
#         self.mock_redis_method.set = Mock(side_effect=self.mock_redis_obj.set)
#         self.mock_redis_method.hset = Mock(side_effect=self.mock_redis_obj.hset)
#         self.mock_redis_method.exists = Mock(side_effect=self.mock_redis_obj.exists)
#         self.mock_redis_method.scan = Mock(side_effect=self.mock_redis_obj.scan)
#         self.mock_redis_method.type = Mock(side_effect=self.mock_redis_obj.type)
#         self.mock_redis_method.smembers = Mock(side_effect=self.mock_redis_obj.smembers)
    
#     @patch("redis.Redis.from_url")
#     def test_file_opennings(self, mock_redis):
#         mock_redis.return_value = self.mock_redis_method
#         with patch('builtins.open', unittest.mock.mock_open()) as open_mock:
#             dumper = RedisDumper("redis://localhost:6379")
#             dumper.dump_with_pattern("foo")
#         open_calls = [call('dump.json', 'w'), call('dump.json', 'a'), call('dump.json', 'a')]
#         open_mock.assert_has_calls(open_calls, any_order=True)

#     @patch("redis.Redis.from_url")
#     def test_dump_with_starless_pattern(self, mock_redis):
#         mock_redis.return_value = self.mock_redis_method
#         with patch('builtins.open', unittest.mock.mock_open()) as open_mock:
#             dumper = RedisDumper("redis://localhost:6379")
#             dumper.dump_with_pattern("foo")
#         handle = open_mock()
#         write_calls = [call('{\n'), call('\t"foo": {"value": "bar", "type": "string"}'), call('\n}')]
#         handle.write.assert_has_calls(write_calls, any_order=False)
    

#     @patch("redis.Redis.from_url")
#     def test_dump_with_star_pattern(self, mock_redis):
#         mock_redis.return_value = self.mock_redis_method
#         with patch('builtins.open', unittest.mock.mock_open()) as open_mock:
#             dumper = RedisDumper("redis://localhost:6379")
#             dumper.dump_with_pattern("foo*")
#         handle = open_mock()
#         write_calls = [call('{\n'), call('\t"foo": {"value": "bar", "type": "string"},\n\t"foo2": {"value": ["salam", "aleyk"], "type": "set"}'), call('\n}')]
#         handle.write.assert_has_calls(write_calls, any_order=False)
    
#     @patch("redis.Redis.from_url")
#     def test_dump_all(self, mock_redis):
#         mock_redis.return_value = self.mock_redis_method
#         with patch('builtins.open', unittest.mock.mock_open()) as open_mock:
#             dumper = RedisDumper("redis://localhost:6379")
#             dumper.dump_all()
#         handle = open_mock()
#         write_calls = [
#             call("{\n"),
#             call('\t"foo": {"value": "bar", "type": "string"},\n\t"barfoo": {"value": {"Foo": "Bar"}, "type": "hash"},\n\t"foo2": {"value": ["salam", "aleyk"], "type": "set"}'),
#             call('\n}')
#         ]
#         handle.write.assert_has_calls(write_calls, any_order=False)
