from app.tests.utils.sample_dataset import get_sample_dataset
from unittest import TestCase
from app.models import Duty
import pandas as pd

class TestVehicle(TestCase):
    __duty: Duty = Duty()
    def setUp(self):
        self.__duty.load(get_sample_dataset()["duties"])


    def test_frame_load(self):
        assert isinstance(self.__duty.filter_id(3), pd.DataFrame)


    def test_id_get(self):
        filtered = self.__duty.filter_id(3)
        assert filtered[(filtered["duty_id"] == 3) & (filtered["vehicle_event_sequence"] == 0)]["duty_event_type"].values == "vehicle_event"
    


    def test_query(self):
        assert len(self.__duty.query("duty_id == 3 & vehicle_event_sequence == 0")) == 1
        assert self.__duty.query("duty_id == 3 & vehicle_event_sequence == 0")["duty_event_type"].values == "vehicle_event"