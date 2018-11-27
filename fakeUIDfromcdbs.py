
# coding: utf-8

# In[12]:

# @see <a href="http://en.wikipedia.org/wiki/Verhoeff_algorithm">More Info</a>
# @see <a href="http://en.wikipedia.org/wiki/Dihedral_group">Dihedral Group</a>
# @see <a href="http://mathworld.wolfram.com/DihedralGroupD5.html">Dihedral Group Order 10</a>
# @author Hermann Himmelbauer
import warnings
warnings.filterwarnings("ignore", message="numpy.dtype size changed")
warnings.filterwarnings("ignore", message="numpy.ufunc size changed")

import sys,glob,gc,csv,pandas as pd,re

#file="X:\C1A-GUJBASE\cdbs.csv"
#file="/media/hp41880/SeagateBackupPlusDrive/C1A-GUJBASE/cdbs.csv"
file="d:/cdbs.csv"
#columns=[ "NUM", "Name","Local_add","Perm_add","Date_of_Act","PoI_type","PoI_No","PoA_type","PoA_No","Status","Conn_Type","Gender","filename"]

columns=["NUM","Name","Local_add","Perm_add","Date_of_Act","formatted_Date_of_Act","PoI_type","PoI_No","PoA_type","PoA_No","POS_Name","POS_Add","Status","Conn_Type","Gender","filename"]

verhoeff_table_d = (
    (0,1,2,3,4,5,6,7,8,9),
    (1,2,3,4,0,6,7,8,9,5),
    (2,3,4,0,1,7,8,9,5,6),
    (3,4,0,1,2,8,9,5,6,7),
    (4,0,1,2,3,9,5,6,7,8),
    (5,9,8,7,6,0,4,3,2,1),
    (6,5,9,8,7,1,0,4,3,2),
    (7,6,5,9,8,2,1,0,4,3),
    (8,7,6,5,9,3,2,1,0,4),
    (9,8,7,6,5,4,3,2,1,0))
verhoeff_table_p = (
    (0,1,2,3,4,5,6,7,8,9),
    (1,5,7,6,2,8,3,0,9,4),
    (5,8,0,3,7,9,6,1,4,2),
    (8,9,1,6,0,4,3,5,2,7),
    (9,4,5,3,1,2,6,8,7,0),
    (4,2,8,6,5,7,3,9,0,1),
    (2,7,9,3,8,0,6,4,1,5),
    (7,0,4,6,9,1,3,2,5,8))
verhoeff_table_inv = (0,4,3,2,1,5,6,7,8,9)

def calcsum(number):
    """For a given number returns a Verhoeff checksum digit"""
    c = 0
    for i, item in enumerate(reversed(str(number))):
        c = verhoeff_table_d[c][verhoeff_table_p[(i+1)%8][int(item)]]
    return verhoeff_table_inv[c]

def checksum(number):
    """For a given number generates a Verhoeff digit and
    returns number + digit"""
    c = 0
    for i, item in enumerate(reversed(str(number))):
        c = verhoeff_table_d[c][verhoeff_table_p[i % 8][int(item)]]
    return c

def generateVerhoeff(number):
    """For a given number returns number + Verhoeff checksum digit"""
    return "%s%s" % (number, calcsum(number))

def validateVerhoeff(number):
    """Validate Verhoeff checksummed number (checksum is last digit)"""
    return checksum(number) == 0

def validateUID(number):
    p=bool(re.findall(r"^\d{12}$",str(number)))
    if p==False:
        return "not valid 12 digit UID"
    else:
        return validateVerhoeff(number)

booleanDictionary = {True: 'TRUE', False: 'FALSE'}


# In[ ]:
my_csv_len=0

i=0

for chunk in pd.read_csv(file,sep=',',chunksize=10000,names=columns,engine='python'):
    i=i+len(chunk)
    if i<30510000:
        print(i)
        continue
    for index,row in chunk.iterrows():
        PoI_No=row['PoI_No']
        PoA_No=row['PoA_No']
        if not bool(re.findall(r"^\d{12}$",str(PoI_No))):
            if not bool(re.findall(r"^\d{12}$",str(PoA_No))):
                chunk.ix[index,'PoI_No_verhoeff_check']="both PoI,PoA are not UID"
                chunk.ix[index,'PoA_No_verhoeff_check']="both PoI,PoA are not UID"
                #chunk.drop(index, inplace=True)
                #print("both PoI,PoA are not UID",index)
                continue
        PoA_No_verhoeff_check=validateUID(PoA_No)
        PoI_No_verhoeff_check=validateUID(PoI_No)
        #print(PoI_No_verhoeff_check,PoA_No_verhoeff_check)
        #print PoI_No,PoI_No_verhoeff_check,PoA_No,PoA_No_verhoeff_check
        #print(PoI_No,PoA_No)
        chunk.ix[index,'PoI_No_verhoeff_check']=validateUID(PoI_No)
        chunk.ix[index,'PoA_No_verhoeff_check']=validateUID(PoA_No)
    chunk.PoI_No_verhoeff_check = chunk.PoI_No_verhoeff_check.replace(booleanDictionary)
    chunk.PoA_No_verhoeff_check = chunk.PoA_No_verhoeff_check.replace(booleanDictionary)
    chunk.drop(chunk[chunk.PoA_No_verhoeff_check == 'both PoI,PoA are not UID'].index, inplace=True)
    chunk.drop(chunk[(chunk.PoA_No_verhoeff_check == 'TRUE') & (chunk.PoI_No_verhoeff_check == 'TRUE')].index, inplace=True)
    chunk.drop(chunk[(chunk.PoA_No_verhoeff_check == 'TRUE') & (chunk.PoI_No_verhoeff_check == 'not valid 12 digit UID')].index, inplace=True)
    chunk.drop(chunk[(chunk.PoA_No_verhoeff_check == 'not valid 12 digit UID') & (chunk.PoI_No_verhoeff_check == 'TRUE')].index, inplace=True)
    my_csv_len=my_csv_len+len(chunk)
    if len(chunk)>1:
        with open('d:/my_csv.csv', 'a',encoding='utf8') as f:
            chunk.to_csv(f, header=False)
#    with open('d:/my_csv.csv', 'a',encoding='utf8') as f:
#        chunk.to_csv(f, header=False)
    print("found--",my_csv_len,"--out of--",i," and last NUM is--",chunk.NUM.tail(1)," inside ",chunk.filename.tail(1))



