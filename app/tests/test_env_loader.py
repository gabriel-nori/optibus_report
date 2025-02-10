from unittest import TestCase
from app import settings

class TestEnvLoader(TestCase):
    def test_setting(self):
        assert settings.LOG_LEVEL in (
            "debug",
            "info",
            "warning",
            "error",
            "exception",
            "critical",
        )
    
    def test_env_location(self):
        assert '/.env' in settings.env_file_path