from app.tests.utils.sample_dataset import get_sample_dataset
from unittest import TestCase
from app.models import Trip
import pandas as pd

class TestVehicle(TestCase):
    __trip: Trip = Trip()
    def setUp(self):
        self.__trip.load(get_sample_dataset()["trips"])


    def test_frame_load(self):
        assert isinstance(self.__trip.filter_id(5306808), pd.DataFrame)


    def test_id_get(self):
        filtered = self.__trip.filter_id(5306808)
        assert filtered[(filtered["trip_id"] == 5306808) & (filtered["route_number"] == "492")]["destination_stop_id"].values == "MTC"
    


    def test_query(self):
        assert len(self.__trip.query("trip_id == 5306808 & route_number == '492'")) == 1
        assert self.__trip.query("trip_id == 5306808 & route_number == '492'")["destination_stop_id"].values == "MTC"