import unittest
from datetime import datetime

from .utils import *


class BuckerTest(unittest.TestCase):
    def setUp(self) -> None:
        self._date = datetime(year=2022, month=12, day=12, hour=10)
    
    def test_y(self):
        self.assertEqual(y(self._date), 2022)
    
    def test_ym(self):
        self.assertEqual(ym(self._date), 202212)
    
    def test_yw(self):
        self.assertEqual(yw(self._date), 202250)
    
    def test_ymd(self):
        self.assertEqual(ymd(self._date), 20221212)
    
    def test_ymdh(self):
        self.assertEqual(ymdh(self._date), 2022121210)

    def test_always(self):
        self.assertEqual(always(self._date), self._date.timestamp())
