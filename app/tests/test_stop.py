from app.tests.utils.sample_dataset import get_sample_dataset
from unittest import TestCase
from app.models import Stop
import pandas as pd

class TestStop(TestCase):
    __stop: Stop = Stop()
    def setUp(self):
        self.__stop.load(get_sample_dataset()["stops"])


    def test_frame_load(self):
        assert isinstance(self.__stop.filter_id("1sSp"), pd.DataFrame)


    def test_id_get(self):
        assert self.__stop.filter_id("1sSp").at[0, "stop_name"] == "1st and Spring"
    

    def test_filter_equals(self):
        filtered = self.__stop.filter("stop_name", "Hope and 9th")
        assert len(filtered) == 1
        assert filtered["stop_id"].values == "9thHo"


    def test_query(self):
        assert len(self.__stop.query("stop_name == 'Hope and 9th'")) == 1