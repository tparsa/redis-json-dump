import unittest
from unittest.mock import patch

from .io import *
from mock.redis import MockRedis


class UtilsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.int = 1
        self.float = 0.1
        self.list = ["1", "2", "3"]
        self.string = "test"
        self.dict = {"a": "1", "b": "2"}

        self.encoded_int = b64encode(self._str_encode(self.int))
        self.encoded_float = b64encode(self._str_encode(self.float))
        self.encoded_list = [b64encode(self._str_encode(i)) for i in self.list]
        self.encoded_string = b64encode(self._str_encode(self.string))
        self.encoded_dict = {
            b64encode(self._str_encode(k)):b64encode(self._str_encode(v))
            for k, v in self.dict.items()
        }
    
    def _str_encode(self, x):
        return str(x).encode("utf-8")

    def test_is_number_int(self):
        self.assertTrue(is_number(1))
    
    def test_is_number_float(self):
        self.assertTrue(is_number(0.1))
    
    def test_is_number_string(self):
        self.assertFalse(is_number(self.string))

    def test_is_iterable_iterable(self):
        class TestIterable(Iterable):
            def __iter__(self):
                return (i for i in self.list)

        self.assertTrue(is_iterable(TestIterable()))
    
    def test_is_iterable_list(self):
        self.assertTrue(is_iterable(self.list))
    
    def test_is_iterable_set(self):
        self.assertTrue(is_iterable(set(self.list)))
    
    def test_is_iterable_dict(self):
        self.assertTrue(is_iterable(self.dict))

    def test_is_iterable_string(self):
        self.assertFalse(is_iterable(self.string))
    
    def test_is_iterable_bytestring(self):
        self.assertFalse(is_iterable(self._str_encode(self.string)))

    def test_b64enc_none(self):
        self.assertEqual(b64enc(None), b64encode("".encode("utf-8")))

    def test_b64enc_int(self):
        self.assertEqual(b64enc(self.int), self.encoded_int)
    
    def test_b64enc_float(self):
        self.assertEqual(b64enc(self.float), self.encoded_float)

    def test_b64enc_string(self):
        self.assertEqual(b64enc(self.string), self.encoded_string)
    
    def test_b64enc_list(self):
        self.assertEqual(b64enc(self.list), self.encoded_list)
    
    def test_b64enc_dict(self):
        self.assertEqual(b64enc(self.dict), self.encoded_dict)
    
    def test_b64dec_none(self):
        self.assertEqual(decode(b64dec(None)), "")

    def test_b64dec_string(self):
        self.assertEqual(decode(b64dec(self.encoded_string)), self.string)
    
    def test_b64dec_int(self):
        self.assertEqual(int(decode(b64dec(self.encoded_int))), self.int)
    
    def test_b64dec_float(self):
        self.assertEqual(float(decode(b64dec(self.encoded_float))), self.float)
    
    def test_b64dec_list(self):
        self.assertEqual(decode(b64dec(self.encoded_list)), self.list)
    
    def test_b64dec_dict(self):
        self.assertEqual(decode(b64dec(self.encoded_dict)), self.dict)
    
    def test_decode_string(self):
        self.assertEqual(decode(self._str_encode(self.string)), self.string)
    
    def test_decode_dict(self):
        encoded_dict = {
            self._str_encode(k):self._str_encode(v)
            for k, v in self.dict.items()
        }
        self.assertEqual(decode(encoded_dict), self.dict)
    
    def test_decode_iterable(self):
        encoded_list = [self._str_encode(i) for i in self.list]
        self.assertEqual(decode(encoded_list), self.list)


class RedisPatterIOTest(unittest.TestCase):
    def setUp(self) -> None:
        self._redis_data = {
            "hash": {"__type": "hash", "value": {"foo1": "bar"}},
            "list": {"__type": "list", "value": ["1", "2", "3"]},
            "set": {"__type": "set", "value": ["1", "2", "3"]},
            "string": "test",
            "zset": {"__type": "zset", "value": ["1", "1", "10", "2", "3", "3"]}
        }
        self._redis_ttls = {
            "hash": 10,
            "list": 20,
            "set": 1,
            "string": -1,
            "zset": -1
        }
        self._types = ["hash", "list", "set", "string", "zset"]
        self._redis_pattern_io = RedisPatternIO(
            MockRedis(self._redis_data, self._redis_ttls)
        )
    
    def test_count_keys(self):
        self.assertEqual(self._redis_pattern_io.count_keys(), len(self._redis_data))
    
    def test_get_types(self):
        all_keys = list(self._redis_data.keys())
        returned_value = self._redis_pattern_io.get_types(all_keys)
        expected_value = self._types
        self.assertEqual(returned_value, expected_value)
    
    def test_get_existing_keys_ttls(self):
        keys = list(self._redis_data.keys())
        returned_values = self._redis_pattern_io.get_ttls(keys)
        expected_values = list(self._redis_ttls.values())
        self.assertEqual(returned_values, expected_values)
    
    def test_get_non_existing_keys_ttls(self):
        keys = ["non_existing"]
        returned_values = self._redis_pattern_io.get_ttls(keys)
        expected_values = [-2]
        self.assertEqual(returned_values, expected_values)
    
    def test_get_values(self):
        keys = list(self._redis_data.keys())
        types = self._types
        returned_value = self._redis_pattern_io.get_values(types, keys)
        returned_value = decode(b64dec(returned_value))
        expected_value = [
            {"foo1": "bar"},
            ["1", "2", "3"],
            ["1", "2", "3"],
            "test",
            ["1", "1", "10", "2", "3", "3"]
        ]
        self.assertEqual(returned_value, expected_value)
    
    def test_iter(self):
        idx = 0
        expected_values = [
            ("hash", "hash", {"foo1": "bar"}, 10),
            ("list", "list", ["1", "2", "3"], 20),
            ("set", "set", ["1", "2", "3"], 1),
            ("string", "string", "test", -1),
            ("zset", "zset", ["1", "1", "10", "2", "3", "3"], -1)
        ]
        for type, key, val, ttl in self._redis_pattern_io:
            returned_value = (type, key, decode(b64dec(val)), ttl)
            self.assertEqual(returned_value, expected_values[idx])
            idx += 1

    def test_write_string_with_ttl(self):
        self._redis_pattern_io.write("test_write", "string", b64enc("test_string"), 10)
        cli = self._redis_pattern_io.cli
        self.assertTrue(cli.exists("test_write"))
        self.assertEqual(cli.get("test_write"), "test_string")
        self.assertEqual(cli.type("test_write"), "string")
        self.assertEqual(cli.ttl("test_write"), 10)
    
    def test_write_string_without_ttl(self):
        self._redis_pattern_io.write("test_write", "string", b64enc("test_string"), -1)
        cli = self._redis_pattern_io.cli
        self.assertTrue(cli.exists("test_write"))
        self.assertEqual(cli.get("test_write"), "test_string")
        self.assertEqual(cli.type("test_write"), "string")
        self.assertEqual(cli.ttl("test_write"), -1)

    def test_write_set_with_ttl(self):
        self._redis_pattern_io.write("test_write", "set", b64enc(["1", "2"]), 20)
        cli = self._redis_pattern_io.cli
        self.assertTrue(cli.exists("test_write"))
        self.assertEqual(cli.smembers("test_write"), ["1", "2"])
        self.assertEqual(cli.type("test_write"), "set")
        self.assertEqual(cli.ttl("test_write"), -1) # set doesn't have ttl

    def test_flush(self):
        with self.assertRaises(Exception):
            self._redis_pattern_io.pipe.set("hash", "1")
            self._redis_pattern_io.flush()


class RedisSingleIOTest(unittest.TestCase):
    def setUp(self) -> None:
        self.mock_redis_obj = MockRedis({})
    
    @patch("redis.Redis.from_url")
    def test_init(self, mock_redis):
        RedisSingleIO("", "")


class RedisClusterIOTest(unittest.TestCase):
    def setUp(self) -> None:
        self.mock_rediscluster_obj = MockRedis({})
    
    @patch("rediscluster.RedisCluster.from_url")
    def test_init(self, mock_redis):
        RedisClusterIO("", "")
