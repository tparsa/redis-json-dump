import unittest

from .retention_bucket import RetentionBucket


class RetentionBucketTest(unittest.TestCase):
    def setUp(self) -> None:
        self._retention_bucket = RetentionBucket(
            count=1,
            bucker=(lambda x: x)
        )
    
    def test_reset(self):
        self.assertEqual(self._retention_bucket.count, 1)
        self._retention_bucket.count -= 1
        self.assertEqual(self._retention_bucket.count, 0)
        self._retention_bucket.reset()
        self.assertEqual(self._retention_bucket.count, 1)
