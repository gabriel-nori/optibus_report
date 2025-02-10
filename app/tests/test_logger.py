from unittest import TestCase
from app import app_logger

class LoggerTest(TestCase):
    def test_level(self):
        assert app_logger.level_index in range(0,5)