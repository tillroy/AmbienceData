# -*- coding: utf-8 -*-
from datetime import datetime

from scrapy.item import Item, Field



class LayerContainer(object):
    def __init__(self):
        self.container = dict()

    @staticmethod
    def __check_dt_validity(dt):
        if isinstance(dt, datetime):
            date = str(dt)
            return date
        else:
            raise ValueError("Wrong type")

    def add_layer(self, name, value):
        _date = self.__check_dt_validity(name)
        self.container[_date] = value

    def get_layers(self):
        return self.container


class AppItem(Item):
    #  Primary field
    source = Field()
    source_id = Field()
    data_time = Field()
    scrap_time = Field()
    data_value = Field()
    forecast_data = Field()
    st_name = Field()

    #  Calculated fields
    aqi = Field()

    #  Housekeeping fields
    url = Field()
    project = Field()
    spider = Field()
    server = Field()
    date = Field()


if __name__ == "__main__":
    ai = AppItem()
    print(ai.get("data_value"))