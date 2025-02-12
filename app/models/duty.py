import pandas as pd


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
    

    def filter_id(self, _id)-> pd.DataFrame:
        return self.filter('duty_id', _id)
    

    def filter(self, key, value):
        return self.__df[self.__df[key] == value]