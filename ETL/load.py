import pyodbc
import pandas as pd


class Load:
    def __init__(self):
        self.cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER=DESKTOP-O8UH4U5;DATABASE=Test;Trusted_Connection=yes;')
        self.cursor = self.cnxn.cursor()

    def load_database(self,df,name):
        try:
            if name == 'Drivers':    
                for index, row in df.iterrows():
                    self.cursor.execute("INSERT INTO Test.dbo.Drivers (driverID,number,code,url,givenName,familyName,DOB,nationality) values(?,?,?,?,?,?,?,?)", row.DepartmentID, row.Name, row.GroupName)
                self.cnxn.commit()
            elif name == 'Races':
                for index, row in df.iterrows():
                    self.cursor.execute("INSERT INTO Test.dbo.Races (season,round,url,raceName,date,time,circuitID,circuitName,circuitCity,circuitCountry) values(?,?,?,?,?,?,?,?,?,?)", row.DepartmentID, row.Name, row.GroupName)
                self.cnxn.commit()
            elif name == 'Standing':
                for index, row in df.iterrows():
                    self.cursor.execute("INSERT INTO Test.dbo.Standings (position,points,wins,givenName,familyName,number,driverCode) values(?,?,?,?,?,?,?)", row.DepartmentID, row.Name, row.GroupName)
                self.cnxn.commit()
        except Exception as e:
            print('Insert into SQL Server db has failed. :', e)
        
    def close_connection(self):
        self.cursor.close()