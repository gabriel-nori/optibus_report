import pandas as pd


class DataFrameTools():
    __df = None


    def __init__(self):
        self.__df = None


    def load(self, data):
        self.__df = pd.DataFrame(pd.json_normalize(data))
    
    
    def get(self)-> pd.DataFrame:
        return self.__df
    

    def filter(self, key, value):
        return self.__df[self.__df[key] == value]