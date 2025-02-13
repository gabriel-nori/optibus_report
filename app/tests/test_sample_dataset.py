from app.tests.utils.sample_dataset import get_sample_dataset
from unittest import TestCase
import random
import json

class TestProcessor(TestCase):
    __sample_dataset: json = None
    __expected_keys: list[str] = [
        'stops',
        'trips',
        'vehicles',
        'duties',
    ]

    def setUp(self):
        self.__sample_dataset = get_sample_dataset()

    def test_keys(self):
        assert self.__expected_keys == list(self.__sample_dataset.keys())

    
    def test_stops(self):
        stops = self.__sample_dataset["stops"]
        stops_key_list = [
            'stop_id',
            'stop_name',
            'latitude',
            'longitude',
            'is_depot',
        ]
        size = len(stops)

        assert size > 0
        # Assert that all the stop items have the required keys
        for item in stops:
            assert stops_key_list == list(item.keys())

    
    def test_trips(self):
        trips = self.__sample_dataset["trips"]
        trips_key_list = [
            'trip_id',
            'route_number',
            'origin_stop_id',
            'destination_stop_id',
            'departure_time',
            'arrival_time',
        ]
        size = len(trips)

        assert size > 0
        # Assert that all the trips items have the required keys.
        # Important: Some trips may have subtrips
        for item in trips:
            assert set(trips_key_list).issubset(set(list(item.keys())))
    

    def test_vehicles(self):
        vehicles = self.__sample_dataset["vehicles"]
        vehicles_key_list: list[str] = [
            "vehicle_id",
            "vehicle_events"
        ]
        vehicle_sub_keys_list: list[str] = [
            "vehicle_event_sequence",
            "vehicle_event_type",
            "start_time",
            "end_time",
            "origin_stop_id",
            "destination_stop_id",
            "trip_id",
            "duty_id",
        ]

        for vehicle in vehicles:
            assert vehicles_key_list == list(vehicle.keys())

    
    def test_duties(self):
        duties = self.__sample_dataset["duties"]
        duties_key_list: list[str] = [
            "duty_id",
            "duty_events"
        ]

        for duty in duties:
            assert duties_key_list == list(duty.keys())