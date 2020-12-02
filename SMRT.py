<<<<<<< HEAD
import os, re
from datetime import datetime
import pandas as pd
from SMRTDB import Session, File, Reading

# File handler for .SMRT files (default folder .data/)
class SMRT:

    def __init__(self, path='data/'):

        self.path = path
        self.files = self.get_files()
        self.valid = False
        self.headers = pd.DataFrame()
        self.readings = pd.DataFrame()
        self.last_file_ref = None
        self.session = Session()
    
    def get_files(self):
        files = {str(x[0]):x[1] for x in enumerate(sorted(os.listdir(self.path)), 1) if x[1].endswith('.SMRT')}
        print("Following SMRT files to be parsed and uploaded:")
        for k,v in files.items():
            print(k,v)
        return [self.path+x for x in files.values()]
    
    def parse(self):
        for f in self.files:
            self.validate(f)
            self.get_header(f)
            self.get_readings(f)
            self.valid=False

    def validate(self, file=None):
        if not self.valid:
            df = pd.read_csv(file, header=None, names=['Record'], usecols=[0])
            structure = df['Record'].unique()
            structrue_check = (structure == ['HEADR', 'CONSU', 'TRAIL'])
            if structrue_check.all():
                self.valid = True
            else:
                print(f'{file} Record Structure is INVALID')
                print(f'please check {structrue_check[~structrue_check]}.')
        else:
            print('SMRT Record structur is VALID.')
       
    def get_header(self, file=None):
        if self.valid:
            cols = ['Record', 'Type', 'CompanyID', 'Date', 'Time', 'Ref']
            dtypes = {
                'Record': str,
                'Type': str,
                'CompanyID': str,
                'Date': str,
                'Time': str,
                'Ref': str
            }
            try:
                df = pd.read_csv(file, header=None, names=cols, dtype=dtypes, nrows=1)
                df.loc[0,'Ref'] = re.match(r'(PN|DV)\d{6}', df.loc[0,'Ref']).group()
                self.headers = self.headers.append(df, ignore_index=True)
                self.last_file_ref = df.loc[0,'Ref']
                print(df)
            except Exception as e:
                print(e)
                print('Error at SMRT HEADR')
    
    def get_readings(self, file=None):
        if self.valid:
            cols = ['Record', 'MeterID', 'Date', 'Time', 'Reading']
            dtypes = {
                'Record': str,
=======
import os
import pandas as pd

class File:
    def __init__(self, name=None, folder='data/'):
        self.name = name
        self.folder = folder
        if self.name == None:
            self.name = self.choose_file()
        self.path = self.folder+self.name
        self.valid = False
        self.header = None
        self.readings = None
    
    def choose_file(self, path=None):
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

    def get_header(self):
        path = self.path
        if self.header == None:
            cols = ['Record', 'Type', 'CompanyID', 'Date', 'Time', 'Ref']
            df = pd.read_csv(path, header=None, names=cols, nrows=1)
            self.header = df
        return self.header
    
    def get_readings(self):
        path = self.path
        if self.readings == None:
            cols = ['Type', 'MeterID', 'Date', 'Time', 'Reading']
            dtypes = {
                'Type': str,
>>>>>>> e7d15b9a183c3ce10462622d460dd9aa7e2e08f6
                'MeterID': str,
                'Date': str,
                'Time': str,
                'Reading': float
            }
<<<<<<< HEAD
            df = pd.read_csv(file, header=None, names=cols, dtype=dtypes, skiprows=1, skipfooter=1, engine='python')
            df['FileRef'] = self.last_file_ref
            self.readings = self.readings.append(df, ignore_index=True)

    def db_insert(self):

        self.insert_headers()
        self.insert_readings()
    
    def insert_headers(self, entry=File):
        for _, row in self.headers.iterrows():
            ts_string = row['Date'] + ':' + row['Time']
            ts = datetime.strptime(ts_string, '%Y%m%d:%H%M%S')
            dbrow = entry(ref=row['Ref'], type=row['Type'], companyid=row['CompanyID'], createdts=ts)
            self.session.add(dbrow)
        try:
            self.session.commit()
            print('File headers inserted into Table files.')
        except Exception as e:
            print(e)
            print('insert_header session rolled back.')
        else:
            self.session.rollback()
    
    def insert_readings(self, entry=Reading):
        self.readings.sort_values('FileRef', inplace=True)
        self.readings.drop_duplicates(subset=['MeterID','Date','Time'],keep='last',inplace=True)
        for _, row in self.readings.iterrows():
            file = self.session.query(File).filter_by(ref=row['FileRef']).one()
            ts_string = row['Date'] + ':' + row['Time']
            ts = datetime.strptime(ts_string, '%Y%m%d:%H%M')
            dbrow = entry(meterid=row['MeterID'], readingdate=row['Date'],
                            timestamp=ts, value=row['Reading'], filename=file)
            self.session.merge(dbrow)
        
        try:
            self.session.commit()
            print('Meter readings inserted into Table readings.')
        except Exception as e:
            print(e)
            print(f"insert reading data for file {file.ref} \nsession rolled back.")
        else:
            self.session.rollback()


class View:
    
    def __init__(self, table=Reading):
        if table:
            self.table = table
        self.session = Session()
        
    
    def to_dataframe(self, field=None, value=None):

        if field and value:
            s = self.session.query(self.table).filter(getattr(self.table, field)==value)
        else:
            s = self.session.query(self.table)

        return pd.read_sql(s.statement, self.session.bind)
        



=======
            df = pd.read_csv(path, header=None, names=cols, dtype=dtypes, skiprows=1, skipfooter=1, engine='python')
            self.readings = df
        return self.readings
>>>>>>> e7d15b9a183c3ce10462622d460dd9aa7e2e08f6
