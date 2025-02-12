import pandas as pd


class Trip():
    __df = None

    def load(self, data):
        """
        Improve this later to allow to separte the subtrips
        """
        self.__df = pd.DataFrame(pd.json_normalize(data))
        self.__df.set_index('trip_id', inplace=False)
        self.__df['trip_id']=self.__df['trip_id'].astype('Int64')
    

    def get(self)-> pd.DataFrame:
        return self.__df
    

    def filter_id(self, _id)-> pd.DataFrame:
        return self.filter('trip_id', _id)
    

    def filter(self, key, value):
        return self.__df[self.__df[key] == value]
    

    def query(self, query)-> pd.DataFrame:
        return self.__df.query(query)