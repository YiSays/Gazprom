import os
from datetime import datetime
import pandas as pd
from SMRTDB import Session, File, Reading

# File handler for .SMRT files (default folder .data/)
class SMRT:
    def __init__(self, name=None, folder='data/'):
        self.name = name
        self.folder = folder
        if self.name == None:
            self.name = self.select_file()
        self.path = self.folder+self.name
        self.valid = False
        self.header = None
        self.header = self.get_header()
        self.readings = None
        self.session = Session()
    
    def select_file(self, path=None):
        if path == None:
            path = self.folder
        files = {str(x[0]):x[1] for x in enumerate(sorted(os.listdir(path)), 1) if x[1].endswith('.SMRT')}
        print("Following SMRT files are available in Folder <data/>:")
        for k,v in files.items():
            print(k,v)
        index = input("Choose the index of SMRT file to parse: ")
        try:
            assert index in files
        except:
            print('Input is out of the range of index...')
        return files[index]
    
    def parse(self):
        self.get_header()
        self.get_readings()

    def get_header(self):
        path = self.path
        if self.header == None:
            cols = ['Record', 'Type', 'CompanyID', 'Date', 'Time', 'Ref']
            dtypes = {
                'Record': str,
                'Type': str,
                'CompanyID': str,
                'Date': str,
                'Time': str,
                'Ref': str
            }
            df = pd.read_csv(path, header=None, names=cols, dtype=dtypes, nrows=1)
            self.header = df
            print(df)
                   
        return self.header
    
    def get_readings(self):
        path = self.path
        if self.readings == None:
            cols = ['Record', 'MeterID', 'Date', 'Time', 'Reading']
            dtypes = {
                'Record': str,
                'MeterID': str,
                'Date': str,
                'Time': str,
                'Reading': float
            }
            df = pd.read_csv(path, header=None, names=cols, dtype=dtypes, skiprows=1, skipfooter=1, engine='python')
            self.readings = df
        return self.readings
   
    def db_insert(self):
        self.insert_header()
        self.insert_readings()
    
    def insert_header(self, entry=File):
        header = self.header.loc[0].to_dict()
        ts_string = header['Date'] + ':' + header['Time']
        ts = datetime.strptime(ts_string, '%Y%m%d:%H%M%S')
        row = entry(ref=header['Ref'], type=header['Type'], companyid=header['CompanyID'], createdts=ts)
        self.session.add(row)
        try:
            self.session.commit()
        except Exception as e:
            print(e)
            print(f"SMRT Record {self.header.loc[0,'Ref']} exists in database.")
            print('insert_header session rolled back.')
        else:
            self.session.rollback()
    
    def insert_readings(self, entry=Reading):
        file = self.session.query(File).filter_by(ref=self.header.loc[0,'Ref']).one()
        if file:
            for _, row in self.readings.iterrows():
                ts_string = row['Date'] + ':' + row['Time']
                ts = datetime.strptime(ts_string, '%Y%m%d:%H%M')
                dbrow = entry(meterid=row['MeterID'], readingdate=row['Date'],
                                timestamp=ts, value=row['Reading'], filename=file)
                self.session.merge(dbrow)
            
            try:
                self.session.commit()
            except Exception as e:
                print(e)
                print(f"insert reading data for file {file.ref} \nsession rolled back.")
            else:
                self.session.rollback()
        
        else:
            self.insert_header()

