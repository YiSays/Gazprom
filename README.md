<p align="center">

   <img src="https://www.gazprom-energy.co.uk/static/img/layout/logo.png" width="200">
</p>

<p></p>

# Gazprom SMRT File Parser

One of the primary ways the energy industry communicates information is via file flows between the shipper (Us) and the Central Data Service Provider.

This Python module is able to parse the SMRT files and load the data into the target database (sqlite or postgresql).

It as two parts, one for the database schema/model and the other one for data parsing/uploading.

---

## Data Model / Database Schema

The DB Model has been defined and created by [SMRTDB.py](SMRTDB.py) using ORM library **SQLAlchemy**, with a connection to a [testing database file](db/smrt.db) which could be easily relocated to the prodution database after dev stage.

 - The database (smrt.db) has two tables *TABLE files* (class **File** in ORM) and *TABLE readings* (class **Reading** in ORM).
 
 - *TABLE files* is the header data from SMRT file, and *TABLE readings* is the reading data. There is a one to many relationship, mapping *TABLE files* to *TABLE readings*.

***NOTE:*** To reset the database, just run the [SMRTDB.py](SMRTDB.py)

```
$ python3 SMRTDB.py
```
---
## SMRT Parser
The [SMRT.py](SMRT.py) works as the parser for SMRT file in the [data location](data/). A simple tutorial is provided as below:

### Make a parser
A parser is generated as an instance of **SMRT()** class. When a parse is made, all the SMRT files available are printed out for review.

```python
>>> from SMRT import SMRT

>>> parser = SMRT()
Following SMRT files to be parsed and uploaded:
1 PN000001.SMRT
2 PN000002.SMRT
3 PN000003.SMRT
4 PN000004.SMRT
5 PN000005.SMRT
6 PN000006.SMRT
7 PN000007.SMRT
8 PN000008.SMRT
9 PN000009.SMRT
10 PN000010.SMRT
>>> 
```
---

### Parse data
To get all the data available by 'parse' method of the parser. The header info will be printed out automatically for review. The reading data has been cleansed by overwriting the reading values from a given meter at the same datetime.
```python
>>> parser.parse()
  Record  Type CompanyID      Date    Time       Ref
0  HEADR  SMRT       GAZ  20191016  102939  PN000001
  Record  Type CompanyID      Date    Time       Ref
0  HEADR  SMRT       GAZ  20191016  102941  PN000002
  Record  Type CompanyID      Date    Time       Ref
0  HEADR  SMRT       GAZ  20191016  102942  PN000003
  Record  Type CompanyID      Date    Time       Ref
0  HEADR  SMRT       GAZ  20191016  102943  PN000004
  Record  Type CompanyID      Date    Time       Ref
0  HEADR  SMRT       GAZ  20191016  102944  PN000005
  Record  Type CompanyID      Date    Time       Ref
0  HEADR  SMRT       GAZ  20191016  102945  PN000006
  Record  Type CompanyID      Date    Time       Ref
0  HEADR  SMRT       GAZ  20191016  102946  PN000007
  Record  Type CompanyID      Date    Time       Ref
0  HEADR  SMRT       GAZ  20191016  102947  PN000008
  Record  Type CompanyID      Date    Time       Ref
0  HEADR  SMRT       GAZ  20191016  102948  PN000009
  Record  Type CompanyID      Date    Time       Ref
0  HEADR  SMRT       GAZ  20191016  102949  PN000010
>>> 
```
---
### Review Data
SMRT data has been parsed and saved in dataframe format as attributes of 'headers' and 'readings' to be inserted into database.
```python
>>> parser.headers
  Record  Type CompanyID      Date    Time       Ref
0  HEADR  SMRT       GAZ  20191016  102939  PN000001
1  HEADR  SMRT       GAZ  20191016  102941  PN000002
2  HEADR  SMRT       GAZ  20191016  102942  PN000003
3  HEADR  SMRT       GAZ  20191016  102943  PN000004
4  HEADR  SMRT       GAZ  20191016  102944  PN000005
5  HEADR  SMRT       GAZ  20191016  102945  PN000006
6  HEADR  SMRT       GAZ  20191016  102946  PN000007
7  HEADR  SMRT       GAZ  20191016  102947  PN000008
8  HEADR  SMRT       GAZ  20191016  102948  PN000009
9  HEADR  SMRT       GAZ  20191016  102949  PN000010
>>>
>>> parser.readings
     index Record    MeterID      Date  Time  Reading   FileRef
0       46  CONSU  000000003  20191014  0200     3.65  PN000001
1       47  CONSU  000000003  20191014  0300     5.53  PN000001
2       48  CONSU  000000003  20191014  0400     3.28  PN000001
3       49  CONSU  000000003  20191014  0500     0.18  PN000001
4       50  CONSU  000000003  20191014  0600     6.30  PN000001
..     ...    ...        ...       ...   ...      ...       ...
510   1735  CONSU  000000002  20191016  0000     8.57  PN000010
511   1734  CONSU  000000002  20191015  2300    11.45  PN000010
512   1733  CONSU  000000002  20191015  2200    12.14  PN000010
513   1742  CONSU  000000002  20191016  0700     4.43  PN000010
514   1796  CONSU  000000005  20191016  0700    12.07  PN000010

[515 rows x 7 columns]
>>>  
```
---
Upload into database

To upload the data into databse, just use the 'db_insert' method of the parser.

```python
>>> parser.db_insert()
10 File headers inserted into TABLE files.
515 meter readings inserted into TABLE readings.
>>> 
```
---
## SMRT Data Query

Simple query funtion is provided as a **View() class** in the same SMRT module.

A table class should be given as the parameter in the View() class. File is for TABLE files (SMRT header) and Reading is for TABLE readgins (SMRT readings).
*NOTE* Reading table will be queried by default.

### View data in dataframe
A 'to_dataframe' method is provide to return the query result into a dataframe, for further analysis with Pandas.

```python
>>> from SMRT import View, File, Reading
>>> query = View(File)
>>> query.to_dataframe()
   id       ref  type companyid           createdts                 insertedts
0   1  PN000001  SMRT       GAZ 2019-10-16 10:29:39 2020-12-03 10:59:24.302879
1   2  PN000002  SMRT       GAZ 2019-10-16 10:29:41 2020-12-03 10:59:24.303625
2   3  PN000003  SMRT       GAZ 2019-10-16 10:29:42 2020-12-03 10:59:24.303726
3   4  PN000004  SMRT       GAZ 2019-10-16 10:29:43 2020-12-03 10:59:24.303814
4   5  PN000005  SMRT       GAZ 2019-10-16 10:29:44 2020-12-03 10:59:24.303897
5   6  PN000006  SMRT       GAZ 2019-10-16 10:29:45 2020-12-03 10:59:24.303977
6   7  PN000007  SMRT       GAZ 2019-10-16 10:29:46 2020-12-03 10:59:24.304054
7   8  PN000008  SMRT       GAZ 2019-10-16 10:29:47 2020-12-03 10:59:24.304131
8   9  PN000009  SMRT       GAZ 2019-10-16 10:29:48 2020-12-03 10:59:24.304206
9  10  PN000010  SMRT       GAZ 2019-10-16 10:29:49 2020-12-03 10:59:24.304281
>>> 
```
---
### Query with filter
A simple fiter function is provided as well. In the 'to_dataframe' methode, first parameter is <filter_field>, and second is <filter_value>, so a filtered query will be returned in dataframe.
```python
>>> query = View(Reading)
>>> # or query = View() # Reading is provided by default
>>> query.to_dataframe('meterid', '000000005')
     id    meterid readingdate           timestamp  value  fileid
0   245  000000005    20191014 2019-10-14 02:00:00   9.86       7
1   234  000000005    20191014 2019-10-14 03:00:00  14.98       7
2   257  000000005    20191014 2019-10-14 04:00:00   1.91       7
3   233  000000005    20191014 2019-10-14 05:00:00   0.36       7
4   249  000000005    20191014 2019-10-14 06:00:00   7.49       7
..  ...        ...         ...                 ...    ...     ...
59  281  000000005    20191016 2019-10-16 13:00:00   8.81       7
60   88  000000005    20191016 2019-10-16 14:00:00  15.43       5
61   84  000000005    20191016 2019-10-16 15:00:00   0.55       5
62   89  000000005    20191016 2019-10-16 16:00:00  10.78       5
63   53  000000005    20191016 2019-10-16 17:00:00  18.99       4

[64 rows x 6 columns]
>>> 
```