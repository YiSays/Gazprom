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
                'MeterID': str,
                'Date': str,
                'Time': str,
                'Reading': float
            }
            df = pd.read_csv(path, header=None, names=cols, dtype=dtypes, skiprows=1, skipfooter=1, engine='python')
            self.readings = df
        return self.readings
