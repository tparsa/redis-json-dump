import unittest
from unittest.mock import call, patch, MagicMock, Mock
import redis

from mock.redis import MockRedis
from redis_lib.type_handlers import (
    StringHandler,
    SetHandler,
    ListHandler,
    HashHandler,
    ZSetHandler,
    NoneHandler
)


class StringHandlerTest(unittest.TestCase):
    def setUp(self):
        redis_cache = {
            "foo": "bar", 
            "barfoo": {"__type": "hash", "value": {"Foo": "Bar"}},
            "foo2": {"__type": "set", "value": ["salam", "aleyk"]}
        }

        self.mock_redis_obj = MockRedis(redis_cache)

        self.mock_redis_method = MagicMock()
        self.mock_redis_method.get = Mock(side_effect=self.mock_redis_obj.get)
        self.mock_redis_method.set = Mock(side_effect=self.mock_redis_obj.set)
    
    @patch("redis.Redis")
    def test_call_for_key_exists(self, mock_redis):
        mock_redis.return_value = self.mock_redis_method
        cli = redis.Redis(host="localhost")
        self.assertEqual(StringHandler.call_for(cli, "foo"), "bar")

    @patch("redis.Redis")
    def test_call_for_non_existing_key(self, mock_redis):
        mock_redis.return_value = self.mock_redis_method
        cli = redis.Redis(host="localhost")
        self.assertEqual(StringHandler.call_for(cli, "bar"), None)

    @patch("redis.Redis")
    def test_write_for_non_existing_key(self, mock_redis):
        mock_redis.return_value = self.mock_redis_method
        cli = redis.Redis(host="localhost")
        StringHandler.write(cli, "bar", "foo", -1)
        self.assertEqual(StringHandler.call_for(cli, "bar"), "foo")

    @patch("redis.Redis")
    def test_write_for_existing_key(self, mock_redis):
        mock_redis.return_value = self.mock_redis_method
        cli = redis.Redis(host="localhost")
        StringHandler.write(cli, "foo", "bar2", -1)
        self.assertEqual(StringHandler.call_for(cli, "foo"), "bar2")
    
    @patch("redis.Redis")
    def test_processor_for_existing_key(self, mock_redis):
        mock_redis.return_value = self.mock_redis_method
        cli = redis.Redis(host="localhost")
        value = StringHandler.call_for(cli, "foo")
        self.assertEqual(StringHandler.process_result(value), "bar")
    
    @patch("redis.Redis")
    def test_processor_for_non_existing_key(self, mock_redis):
        mock_redis.return_value = self.mock_redis_method
        cli = redis.Redis(host="localhost")
        value = StringHandler.call_for(cli, "bar")
        self.assertEqual(StringHandler.process_result(value), None)

    @patch("redis.Redis")
    def test_call_for_for_non_string_type(self, mock_redis):
        mock_redis.return_value = self.mock_redis_method
        cli = redis.Redis(host="localhost")
        with self.assertRaises(Exception):
            StringHandler.call_for(cli, "barfoo")


class SetHandlerTest(unittest.TestCase):
    def setUp(self):
        redis_cache = {
            "foo": "bar", 
            "barfoo": {"__type": "hash", "value": {"Foo": "Bar"}},
            "foo2": {"__type": "set", "value": ["salam", "aleyk"]}
        }

        self.mock_redis_obj = MockRedis(redis_cache)

        self.mock_redis_method = MagicMock()
        self.mock_redis_method.sadd = Mock(side_effect=self.mock_redis_obj.sadd)
        self.mock_redis_method.smembers = Mock(side_effect=self.mock_redis_obj.smembers)
    
    @patch("redis.Redis")
    def test_call_for_key_exists(self, mock_redis):
        mock_redis.return_value = self.mock_redis_method
        cli = redis.Redis(host="localhost")
        self.assertEqual(SetHandler.call_for(cli, "foo2"), ["salam", "aleyk"])

    @patch("redis.Redis")
    def test_call_for_non_existing_key(self, mock_redis):
        mock_redis.return_value = self.mock_redis_method
        cli = redis.Redis(host="localhost")
        self.assertEqual(SetHandler.call_for(cli, "bar"), None)

    @patch("redis.Redis")
    def test_write_for_non_existing_key(self, mock_redis):
        mock_redis.return_value = self.mock_redis_method
        cli = redis.Redis(host="localhost")
        SetHandler.write(cli, "bar", "foo", -1)
        self.assertEqual(SetHandler.call_for(cli, "bar"), ["foo"])

    @patch("redis.Redis")
    def test_write_for_existing_key(self, mock_redis):
        mock_redis.return_value = self.mock_redis_method
        cli = redis.Redis(host="localhost")
        SetHandler.write(cli, "foo2", "bar2", -1)
        return_value = SetHandler.call_for(cli, "foo2")
        expected_value = ["salam", "aleyk", "bar2"]
        self.assertEqual(return_value, expected_value)
    
    @patch("redis.Redis")
    def test_processor_for_existing_key(self, mock_redis):
        mock_redis.return_value = self.mock_redis_method
        cli = redis.Redis(host="localhost")
        call_for_value = SetHandler.call_for(cli, "foo2")
        processed_result_value = SetHandler.process_result(call_for_value)
        expected_value = ["salam", "aleyk"]
        self.assertEqual(processed_result_value, expected_value)
    
    @patch("redis.Redis")
    def test_processor_for_non_existing_key(self, mock_redis):
        mock_redis.return_value = self.mock_redis_method
        cli = redis.Redis(host="localhost")
        value = SetHandler.call_for(cli, "bar")
        self.assertEqual(SetHandler.process_result(value), None)

    @patch("redis.Redis")
    def test_call_for_for_non_string_type(self, mock_redis):
        mock_redis.return_value = self.mock_redis_method
        cli = redis.Redis(host="localhost")
        with self.assertRaises(Exception):
            SetHandler.call_for(cli, "barfoo")


class ListHandlerTest(unittest.TestCase):
    def setUp(self):
        redis_cache = {
            "foo": ["bar"],
            "foo2": "bar"
        }

        self.mock_redis_obj = MockRedis(redis_cache)

        self.mock_redis_method = MagicMock()
        self.mock_redis_method.lrange = Mock(side_effect=self.mock_redis_obj.lrange)
        self.mock_redis_method.rpush = Mock(side_effect=self.mock_redis_obj.rpush)
    
    @patch("redis.Redis")
    def test_call_for_key_exists(self, mock_redis):
        mock_redis.return_value = self.mock_redis_method
        cli = redis.Redis(host="localhost")
        self.assertEqual(ListHandler.call_for(cli, "foo"), ["bar"])

    @patch("redis.Redis")
    def test_call_for_non_existing_key(self, mock_redis):
        mock_redis.return_value = self.mock_redis_method
        cli = redis.Redis(host="localhost")
        self.assertEqual(ListHandler.call_for(cli, "bar"), None)

    @patch("redis.Redis")
    def test_write_for_non_existing_key(self, mock_redis):
        mock_redis.return_value = self.mock_redis_method
        cli = redis.Redis(host="localhost")
        ListHandler.write(cli, "bar", "foo", -1)
        self.assertEqual(ListHandler.call_for(cli, "bar"), ["foo"])

    @patch("redis.Redis")
    def test_write_for_existing_key(self, mock_redis):
        mock_redis.return_value = self.mock_redis_method
        cli = redis.Redis(host="localhost")
        ListHandler.write(cli, "foo", "bar2", -1)
        return_value = ListHandler.call_for(cli, "foo")
        expected_value = ["bar", "bar2"]
        self.assertEqual(return_value, expected_value)
    
    @patch("redis.Redis")
    def test_processor_for_existing_key(self, mock_redis):
        mock_redis.return_value = self.mock_redis_method
        cli = redis.Redis(host="localhost")
        call_for_value = ListHandler.call_for(cli, "foo")
        processed_result_value = ListHandler.process_result(call_for_value)
        expected_value = ["bar"]
        self.assertEqual(processed_result_value, expected_value)
    
    @patch("redis.Redis")
    def test_processor_for_non_existing_key(self, mock_redis):
        mock_redis.return_value = self.mock_redis_method
        cli = redis.Redis(host="localhost")
        value = ListHandler.call_for(cli, "bar")
        self.assertEqual(ListHandler.process_result(value), None)

    @patch("redis.Redis")
    def test_call_for_for_non_string_type(self, mock_redis):
        mock_redis.return_value = self.mock_redis_method
        cli = redis.Redis(host="localhost")
        with self.assertRaises(Exception):
            ListHandler.call_for(cli, "foo2")
