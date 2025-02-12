from app.models import Duty, Stop, Trip, Vehicle
from app import settings
import json
import os


class Processor():
    """
    This class is a processor to generate reports based on trips,
    duties, vehicles and stops.
    The class instantiate each model and then analyze each one,
    to create the report
    """
    __stops: Stop = Stop()
    __trips: Trip = Trip()
    __vehicles: Vehicle = Vehicle()
    __duties: Duty = Duty()
    __sub_trips = None
    __data_loaded: bool = False
    __filenames: list[str] = []
    __base_path: str = settings.FILE_INGESTION_PATH
    __operations: list[str] = []


    def __init__(self, filenames: list[str] = None, auto_operations: list[str] = None):
        if not filenames:
            for filename in os.listdir(settings.FILE_INGESTION_PATH):
                # Check if the file ends with '.json'
                if filename.endswith('.json'):
                    print(os.path.join(filename))
                    self.__filenames.append(filename)
        else:
            self.__filenames = filenames
        
        if auto_operations:
            self.__operations = auto_operations

    def start(self):
        """
        This is the start of the processing. In here we need to setup configs and create data models.
        Each of these functionalities must have its own method or classes
        """
        filename = "mini_json_dataset.json" # This is a placeholder just to test

        # Load all the dataframes using the provided JSON
        self.load_dfs(self.__filenames[0])
        
    
    def load_dfs(self, filename: str):
        path = self.__base_path + filename
        with open(path) as file:
            data = json.load(file)
            self.__stops.load(data['stops'])
            self.__trips.load(data['trips'])
            self.__vehicles.load(data['vehicles'])
            self.__duties.load(data['duties'])

    def duty_start_end(self):
        pass
