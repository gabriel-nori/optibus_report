from app.models import Duty, Stop, Trip, Vehicle
from app import settings
import pandas as pd
import json
import os


class Processor():
    """
    This class is a processor to generate reports based on trips,
    duties, vehicles and stops.
    The class instantiate each model and then analyze each one,
    to create the report
    """
    __data: json = None
    __stops: Stop = Stop()
    __trips: Trip = Trip()
    __vehicles: Vehicle = Vehicle()
    __duties: Duty = Duty()
    __sub_trips = None
    __data_loaded: bool = False
    __filename: str = None
    __base_load_path: str = settings.FILE_INGESTION_PATH
    __base_save_path: str = settings.FILE_OUTPUT_PATH
    __operations: list[str] = []
    __obt: pd.DataFrame = pd.DataFrame()
    __start_time_order: list[str] = [
        "pre_trip",
        "depot_pull_out",
        "service_trip",
        "sign_on"
    ]
    __end_time_order: list[str] = [
        "depot_pull_in",
        "taxi"
    ]
    __column_map: dict[dict[str]] = {
        "pre_trip": {
            "start_time": "start_time_y",
            "end_time": "end_time_y",
            "start_id": "origin_stop_id_y",
            "end_id": "destination_stop_id_y"
        },
        "depot_pull_out": {
            "start_time": "start_time_y",
            "end_time": "end_time_y",
            "start_id": "origin_stop_id_y",
            "end_id": "destination_stop_id_y"
        },
        "service_trip": {
            "start_time": "departure_time",
            "end_time": "arrival_time",
            "start_id": "origin_stop_id",
            "end_id": "destination_stop_id"
        },
        "sign_on": {
            "start_time": "start_time_x",
            "end_time": "end_time_x",
            "start_id": "origin_stop_id_x",
            "end_id": "destination_stop_id_x"
        },
        "depot_pull_in": {
            "start_time": "start_time_y",
            "end_time": "end_time_y",
            "start_id": "origin_stop_id_y",
            "end_id": "destination_stop_id_y"
        },
        "taxi": {
            "start_time": "start_time_x",
            "end_time": "end_time_x",
            "start_id": "origin_stop_id_x",
            "end_id": "destination_stop_id_x"
        },
        "dead_head": {
            "start_time": "start_time_y",
            "end_time": "end_time_y",
            "start_id": "origin_stop_id_y",
            "end_id": "destination_stop_id_y"
        },
    }


    def __init__(
            self,
            filename: str = None,
            auto_operations: list[str] = None,
            data: json = None
        ):
        if data:
            self.__data = data
        else:
            if not filename:
                raise Exception("No file provided")
            self.__filename = filename
        
        if auto_operations:
            self.__operations = auto_operations

        self.load_dfs()


    def is_loaded(self)-> bool:
        return self.__data_loaded


    def get_obt(self)-> pd.DataFrame:
        return self.__obt


    def start(self):
        """
        This is the start of the processing. In here we need to setup configs and create data models.
        Each of these functionalities must have its own method or classes
        """
        # Load the dataframe using the provided JSON
        self.load_dfs()
        self.duty_start_end()
        self.duty_start_end_name()
        
    
    def load_dfs(self):
        if self.__data_loaded:
            return
        
        if self.__data is None:
            path = self.__base_load_path + self.__filename
            with open(path) as file:
                self.__data = json.load(file)

        self.__stops.load(self.__data['stops'])
        self.__trips.load(self.__data['trips'])
        self.__vehicles.load(self.__data['vehicles'])
        self.__duties.load(self.__data['duties'])

        self.__generate_obt()

        self.__data_loaded = True
    

    def export_file(
            self, data: pd.DataFrame,
            filename: str,
            file_type: str = "xlsx"
        )-> str|None:
        file = f"{self.__base_save_path}{filename}.{file_type}"
        try:
            data.to_excel(file, index=False, engine='openpyxl')
            return file
        except:
            return None

    
    def __generate_obt(self):
        obt = pd.merge(
            self.__duties.get(),
            self.__vehicles.get(),
            on=['vehicle_event_sequence', 'vehicle_id', 'duty_id'],
            how='left'
        )
        obt = pd.merge(
            obt,
            self.__trips.get(),
            on=['trip_id'],
            how='left'
        )
        self.__obt = obt
    

    def __get_event_type(self, df: pd.DataFrame) -> str:
        """Determine the type of event based on DataFrame content."""
        if df['duty_event_type'].values[0] == 'vehicle_event':
            return df['vehicle_event_type'].values[0]
        return df['duty_event_type'].values[0]
    

    def __get_start_end(self, df: pd.DataFrame)-> tuple[str, str]:
        """
        This method returns a tuple of tuple containing time and location.
        The following example is a valid return:
        (('0.03:25', 'Pomona'), ('0.11:39', 'Pomona'))
        The tuple structure is as follows:
        ((start_time, start_stop_id), (end_time, end_stop_id))
        """
        # Get the first and last entries for the duty
        start = df.head(1)
        end = df.tail(1)
        start_type = self.__get_event_type(start)
        end_type = self.__get_event_type(end)

        # First, verify if it is vehicle, taxi or sign_on event
        # If it is vehicle, verify waht type and get data

        start_data = start[[
            self.__column_map[start_type]["start_time"],
            self.__column_map[start_type]["start_id"],
        ]].values[0]
        
        end_data = end[[
            self.__column_map[end_type]["end_time"],
            self.__column_map[end_type]["end_id"]
        ]].values[0]

        return (tuple(start_data), tuple(end_data))


    def duty_start_end(
            self,
            list_locations: bool = False,
            filename: str = None,
            export: bool = True
        )-> pd.DataFrame:
        filename = filename if filename else "start_and_end_time"
        start_times: list[str] = []
        end_times: list[str] = []
        duties: list[int] = []
        start_ids: list[str] = []
        end_ids: list[str] = []

        for duty_id in self.__obt['duty_id'].unique():
            df = self.__obt.query(f"duty_id == {duty_id}")

            start, end = self.__get_start_end(df)
            start_time = start[0].split(".")[1]
            end_time = end[0].split(".")[1]

            if list_locations:
                start_ids.append(start[1])
                end_ids.append(end[1])

            start_times.append(start_time)
            end_times.append(end_time)
            duties.append(duty_id)

        df_dict = {
            "Duty Id": duties,
            "Start Time": start_times,
            "End Time": end_times
        }
        
        if list_locations:
            df_dict["start_stop_id"] = start_ids
            df_dict["end_stop_id"] = end_ids

        df = pd.DataFrame(df_dict)
        if export:
            self.export_file(df, filename)
        return df


    def duty_start_end_name(self, filename: str = None, export: bool = True)-> pd.DataFrame:
        filename = filename if filename else "start_and_end_time_name"
        df = self.duty_start_end(list_locations=True, export=False)
        df = pd.merge(
            df,
            self.__stops.get(),
            left_on=['start_stop_id'],
            right_on=['stop_id'],
            how='inner'
        )
        df = pd.merge(
            df,
            self.__stops.get(),
            left_on=['end_stop_id'],
            right_on=['stop_id'],
            how='inner'
        )

        df = df[[
            'Duty Id',
            'Start Time',
            'End Time',
            'stop_name_x',
            'stop_name_y',
        ]].rename(
            columns={
                'stop_name_x': 'Start stop description',
                'stop_name_y': 'End stop description',
            }
        )
        if export:
            self.export_file(df, filename)
        return df

