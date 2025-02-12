import pandas as pd


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
    

    def filter_id(self, _id)-> pd.DataFrame:
        return self.filter('vehicle_id', _id)
    

    def filter(self, key, value):
        return self.__df[self.__df[key] == value]
    

    def query(self, query)-> pd.DataFrame:
        return self.__df.query(query)