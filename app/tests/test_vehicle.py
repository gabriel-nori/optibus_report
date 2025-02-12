from app.tests.utils.sample_dataset import get_sample_dataset
from unittest import TestCase
from app.models import Vehicle
import pandas as pd

class TestVehicle(TestCase):
    __vehicle: Vehicle = Vehicle()
    def setUp(self):
        self.__vehicle.load(get_sample_dataset()["vehicles"])


    def test_frame_load(self):
        assert isinstance(self.__vehicle.filter_id("45"), pd.DataFrame)


    def test_id_get(self):
        filtered = self.__vehicle.filter_id("45")
        assert filtered[(filtered["duty_id"] == "81") & (filtered["vehicle_event_sequence"] == "0")]["vehicle_event_type"].values == "pre_trip"
    


    def test_query(self):
        assert len(self.__vehicle.query("duty_id == '81' & vehicle_event_sequence == '0'")) == 1
        assert self.__vehicle.query("duty_id == '81' & vehicle_event_sequence == '0'")["vehicle_event_type"].values == "pre_trip"