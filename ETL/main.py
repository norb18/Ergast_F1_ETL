import extract_source_from_api as E
import transform_data as T
import load as L
import pandas as pd
import load_mongo as LM

class ETLHandler:
    def __init__(self) -> None:
        pass

    def ExtractDrivers(self):
        self.drivers = E.handle_drivers('2020/drivers')
        return self.drivers
    
    
    def ExtractStandingList(self):
        self.standinglist = E.handle_standingList('2020/driverStandings')
        return self.standinglist
    
    def ExtractRaces(self):
        self.races = E.handle_races('2020/races')
        return self.races
    
    def ExtractCircuits(self):
        self.circuits = E.handle_circuits('2020/circuits')
        return self.circuits

    def load_mongo(self):
        lm = LM.Mongoloader()
        lm.loadToMongoDB(self.ExtractDrivers(),'drivers')
        lm.loadToMongoDB(self.ExtractCircuits(), 'circuits')
        lm.loadToMongoDB(self.ExtractStandingList(), 'standinglist')
        lm.loadToMongoDB(self.ExtractRaces(), 'races')
        lm.closeConnection()

    def call_TL(self):
        l = L.Load()
        l.load_database(T.transfrom_data(self.ExtractDrivers),'Drivers')
        l.load_database(T.transfrom_data(self.ExtractRaces),'Races')
        l.load_database(T.transfrom_data(self.ExtractStandingList),'Standing')
        l.close_connection()

etl = ETLHandler()
#etl.load_mongo()
etl.call_TL()