from app import settings
import pandas as pd
import json

class Stop():
    __df = None

    def load(self, data):
        self.__df = pd.DataFrame(pd.json_normalize(data))
        self.__df.set_index('stop_id', inplace=True)
    

    def get(self)-> pd.DataFrame:
        return self.__df


class Trip():
    __df = None

    def load(self, data):
        """
        Improve this later to allow to separte the subtrips
        """
        self.__df = pd.DataFrame(pd.json_normalize(data))
        self.__df.set_index('trip_id', inplace=True)
    

    def get(self)-> pd.DataFrame:
        return self.__df


class Vehicle():
    __df = None

    def load(self, data):
        all_vehicle_events = []
        for vehicle in data:
            # Get the vehicle ID
            vehicle_id = vehicle['vehicle_id']
            # Normalize the nested vehicle events into separate rows
            events_df = pd.json_normalize(vehicle['vehicle_events'])
            # Add the vehicle_id to each event row
            events_df['vehicle_id'] = vehicle_id
            # Append the DataFrame of vehicle events to the list
            all_vehicle_events.append(events_df)

        # Concatenate all vehicle event DataFrames into a single DataFrame
        self.__df = pd.concat(all_vehicle_events, ignore_index=True)
    

    def get(self)-> pd.DataFrame:
        return self.__df



class Duty():
    __df = None

    def load(self, data):
        all_duty_events = []
        for duty in data:
            # Get the vehicle ID
            duty_id = duty['duty_id']
            # Normalize the nested duties events into separate rows
            events_df = pd.json_normalize(duty['duty_events'])
            # Add the duty_id to each event row
            events_df['duty_id'] = duty_id
            # Append the DataFrame of duties events to the list
            all_duty_events.append(events_df)

        # Concatenate all duties event DataFrames into a single DataFrame
        self.__df = pd.concat(all_duty_events, ignore_index=True)
    

    def get(self)-> pd.DataFrame:
        return self.__df
    
    
    def filter(self):
        return




class Processor():
    __stops = Stop()
    __trips = Trip()
    __vehicles = Vehicle()
    __duties = Duty()
    __sub_trips_df = None

    def start(self):
        """
        This is the start of the processing. In here we need to setup configs and create data models.
        Each of these functionalities must have its own method or classes
        """
        filename = "mini_json_dataset.json"
        self.load_dfs(filename)

        print(self.__stops.get())
        print(self.__trips.get())
        print(self.__vehicles.get())
        print(self.__duties.get())
        
    
    def load_dfs(self, filename: str):
        path = settings.FILE_INGESTION_PATH + filename
        with open(path) as file:
            data = json.load(file)
            self.__stops.load(data['stops'])
            self.__trips.load(data['trips'])
            self.__vehicles.load(data['vehicles'])
            self.__duties.load(data['duties'])
