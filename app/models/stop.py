import pandas as pd


class Stop():
    __df = None

    def load(self, data):
        self.__df = pd.DataFrame(pd.json_normalize(data))
        self.__df.set_index('stop_id', inplace=False)
    

    def get(self)-> pd.DataFrame:
        return self.__df
    

    def filter_id(self, _id)-> pd.DataFrame:
        return self.filter('stop_id', _id)
    

    def filter(self, key, value)-> pd.DataFrame:
        return self.__df[self.__df[key] == value]
    

    def query(self, query)-> pd.DataFrame:
        return self.__df.query(query)