import extract_source_from_api as E
import transform_data as T
import load as L
import pandas as pd
import load_mongo as LM
import pyodbc

class ETLHandler:
    def __init__(self) -> None:
        self.cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER=DESKTOP-O8UH4U5;DATABASE=Test;Trusted_Connection=yes;')
        self.cursor = self.cnxn.cursor()
        pass

    def ExtractDrivers(self):
        self.drivers = E.handle_drivers('2020/drivers')
    
    def ExtractStandingList(self):
        self.standinglist = E.handle_standingList('2020/driverStandings')
    
    def ExtractRaces(self):
        self.races = E.handle_races('2020/races')
    
    def ExtractCircuits(self):
        self.circuits = E.handle_circuits('2020/circuits')
    
    def LoadSQL(self, name):
            if name == 'Drivers':
                print(self.drivers.columns)
                print(self.drivers) 
                self.cursor.execute("TRUNCATE TABLE Test.dbo.Drivers")
                for index,row in self.drivers.iterrows():
                    self.cursor.execute("INSERT INTO Test.dbo.Drivers (driverID,number,code,url,givenName,familyName,DOB,nationality) values(?,?,?,?,?,?,?,?)", row.driverId, row.permanentNumber, row.code,row.url,row.givenName,row.familyName,row.dateOfBirth,row.nationality)
                self.cnxn.commit()
            elif name == 'Races':
                print(self.races.columns)
                print(self.races)
                self.cursor.execute("TRUNCATE TABLE Test.dbo.Races")
                for index,row in self.races.iterrows():
                    self.cursor.execute("INSERT INTO Test.dbo.Races (season,round,url,raceName,date,time,circuitID,circuitName,circuitCountry) values(?,?,?,?,?,?,?,?,?)", row.season,row['round'],row.url,row.raceName,row.date,row.time,row['Circuit.circuitId'],row['Circuit.circuitName'], row['Circuit.Location.country'])
                self.cnxn.commit()
            elif name == 'Standing':
                print(self.standinglist.columns)
                print(self.standinglist)
                self.cursor.execute("TRUNCATE TABLE Test.dbo.Standings")
                for index,row in self.standinglist.iterrows():
                    self.cursor.execute("INSERT INTO Test.dbo.Standings (position,points,wins,givenName,familyName) values(?,?,?,?,?)", row.position, row.points, row.wins, row['Driver.givenName'],row['Driver.familyName'])
                self.cnxn.commit()



    # def load_mongo(self):
    #     lm = LM.Mongoloader()
    #     lm.loadToMongoDB(self.ExtractDrivers(),'drivers')
    #     lm.loadToMongoDB(self.ExtractCircuits(), 'circuits')
    #     lm.loadToMongoDB(self.ExtractStandingList(), 'standinglist')
    #     lm.loadToMongoDB(self.ExtractRaces(), 'races')
    #     lm.closeConnection()


    def call_TL(self):
        self.ExtractDrivers()
        self.ExtractRaces()
        self.ExtractStandingList()
        self.LoadSQL('Drivers')
        self.LoadSQL('Races')
        self.LoadSQL('Standing')

etl = ETLHandler()
#etl.load_mongo()
etl.call_TL()