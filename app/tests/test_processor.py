from app.tests.utils.sample_dataset import get_sample_dataset
from app.processor.duty.processor import Processor
from app.config import settings
from unittest import TestCase
import pandas as pd
import time
import os

class TestProcessor(TestCase):
    __processor: Processor = Processor(data = get_sample_dataset())

    def test_data_loaded(self):
        assert self.__processor.is_loaded() == True

    def test_obt(self):
        assert isinstance(self.__processor.get_obt(), pd.DataFrame)

    
    def test_export(self):
        filename = "test_obt"
        self.__processor.export_file(self.__processor.get_obt(), filename) == f"{settings.FILE_OUTPUT_PATH}{filename}.xlsx"
        self.__processor.export_file(self.__processor.get_obt(), "") == f"{settings.FILE_OUTPUT_PATH}.xlsx"

    def test_get_start_end(self):
        p = Processor(data = get_sample_dataset())

        assert (('0.03:25', 'Pomona'), ('0.11:39', 'Pomona')) == p._Processor__get_start_end(
            self.__processor.get_obt().query(f"duty_id == 1")
        )

        assert (('0.04:00', 'Pomona'), ('0.12:32', 'Pomona')) == p._Processor__get_start_end(
            self.__processor.get_obt().query(f"duty_id == 5")
        )
    

    def test_duty_start_end(self):
        filename = f"test_duty_start_end_{time.time()}"
        assert f"{filename}.xlsx" not in os.listdir(settings.FILE_OUTPUT_PATH)
        assert isinstance(self.__processor.duty_start_end(export=False), pd.DataFrame)
        assert f"{filename}.xlsx" not in os.listdir(settings.FILE_OUTPUT_PATH)
        test_frame = self.__processor.duty_start_end(filename=filename, export=True)
        assert isinstance(test_frame, pd.DataFrame)
        assert f"{filename}.xlsx" in os.listdir(settings.FILE_OUTPUT_PATH)
        os.remove(f"{settings.FILE_OUTPUT_PATH}{filename}.xlsx")

        assert len(test_frame['Duty Id'].unique()) == len(get_sample_dataset()['duties'])

        first_id = test_frame.query("`Duty Id` == 1")
        assert len(first_id) == 1
        assert first_id['Duty Id'].values == 1
        assert first_id['Start Time'].values == "03:25"
        assert first_id['End Time'].values == "11:39"

        last_id = test_frame.query("`Duty Id` == 144")
        assert len(last_id) == 1
        assert last_id['Duty Id'].values == 144
        assert last_id['Start Time'].values == "12:00"
        assert last_id['End Time'].values == "21:04"
    

    def test_duty_start_end_name(self):
        filename = f"test_duty_start_end_name_{time.time()}"
        assert f"{filename}.xlsx" not in os.listdir(settings.FILE_OUTPUT_PATH)
        assert isinstance(self.__processor.duty_start_end_name(export=False), pd.DataFrame)
        assert f"{filename}.xlsx" not in os.listdir(settings.FILE_OUTPUT_PATH)
        test_frame = self.__processor.duty_start_end_name(filename=filename, export=True)
        assert isinstance(test_frame, pd.DataFrame)
        assert f"{filename}.xlsx" in os.listdir(settings.FILE_OUTPUT_PATH)
        os.remove(f"{settings.FILE_OUTPUT_PATH}{filename}.xlsx")
        
        assert len(test_frame['Duty Id'].unique()) == len(get_sample_dataset()['duties'])

        first_id = test_frame.query("`Duty Id` == 1")
        assert len(first_id) == 1
        assert first_id['Duty Id'].values == 1
        assert first_id['Start Time'].values == "03:25"
        assert first_id['End Time'].values == "11:39"
        assert first_id['Start stop description'].values == "Pomona Yard"
        assert first_id['End stop description'].values == "Pomona Yard"

        midddle = test_frame.query("`Duty Id` == 100")
        assert len(midddle) == 1
        assert midddle['Duty Id'].values == 100
        assert midddle['Start Time'].values == "12:03"
        assert midddle['End Time'].values == "21:49"
        assert midddle['Start stop description'].values == "Pomona Yard"
        assert midddle['End stop description'].values == "Pomona Yard"

        last_id = test_frame.query("`Duty Id` == 144")
        assert len(last_id) == 1
        assert last_id['Duty Id'].values == 144
        assert last_id['Start Time'].values == "12:00"
        assert last_id['End Time'].values == "21:04"
        assert last_id['Start stop description'].values == "Pomona Yard"
        assert last_id['End stop description'].values == "Pomona Yard"
    

    def test_duty_break(self):
        filename = f"test_duty_break_{time.time()}"
        assert f"{filename}.xlsx" not in os.listdir(settings.FILE_OUTPUT_PATH)
        assert isinstance(self.__processor.duty_breaks(export=False), pd.DataFrame)
        assert f"{filename}.xlsx" not in os.listdir(settings.FILE_OUTPUT_PATH)
        test_frame = self.__processor.duty_breaks(filename=filename, export=True)
        assert isinstance(test_frame, pd.DataFrame)
        assert f"{filename}.xlsx" in os.listdir(settings.FILE_OUTPUT_PATH)
        os.remove(f"{settings.FILE_OUTPUT_PATH}{filename}.xlsx")

        first_id = test_frame.query("`Duty Id` == 1")
        assert len(first_id) == 3

        midddle = test_frame.query("`Duty Id` == 100")
        assert len(midddle) == 3

        last_id = test_frame.query("`Duty Id` == 139")
        assert len(last_id) == 2
    

    def test_get_property(self):
        p = Processor(data = get_sample_dataset())
        assert '0.03:25' == p._Processor__get_property(
            self.__processor.get_obt().query(f"duty_id == 1"),
            "start_time"
        )

        assert '0.04:00' == p._Processor__get_property(
            self.__processor.get_obt().query(f"duty_id == 5"),
            "start_time"
        )

        assert '0.04:20' == p._Processor__get_property(
            self.__processor.get_obt().query(f"duty_id == 5"),
            "end_time"
        )

        assert 'Pomona' == p._Processor__get_property(
            self.__processor.get_obt().query(f"duty_id == 5"),
            "end_id"
        )

        assert 'Pomona' == p._Processor__get_property(
            self.__processor.get_obt().query(f"duty_id == 5"),
            "start_id"
        )