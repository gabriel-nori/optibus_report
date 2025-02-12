from app.tests.utils.sample_dataset import get_sample_dataset
from app.processor.duty.processor import Processor
from app.config import settings
from unittest import TestCase
import pandas as pd

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