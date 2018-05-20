# coding:utf-8

# Aurthor: Cai, Jiefei

import datetime

class DatetimeUtil():
    format_map = {}
    format_map_Desc = {}
    def __init__(self):
        self.format_map = {
            'YYYYMMDD':'%Y%m%d',
            'YYYY-MM-DD':'%Y-%m-%d',
            'YYYY-MM-DD HH:MM:SS':'%Y-%m-%d %H:%M:%S'
        }
        # for i in self.format_map:
        #     self.format_map_Desc[self.format_map[i]] = i

    def getToday(self,Dt_format='YYYYMMDD'):
        if Dt_format in self.format_map:
            Dt_format = self.format_map[Dt_format]
        dt_Today = datetime.datetime.strftime(datetime.datetime.today(), Dt_format)
        return dt_Today

    def DateAdd(self,dt,OffSet,SetType='D',Dt_format='YYYYMMDD'):
        if Dt_format in self.format_map:
            Dt_format = self.format_map[Dt_format]
        Date_dt = datetime.datetime.strptime(dt,Dt_format)
        if SetType.upper() == 'D':
            redate = Date_dt + datetime.timedelta(OffSet)
        return redate.strftime(format=Dt_format)