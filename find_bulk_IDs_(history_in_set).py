
import pandas as pd,gc,sqlite3,csv,time
path =r'D:\data science\\'

conn=sqlite3.connect(path+'analysis_after_ID_cleaned1.sqlite')
c=conn.cursor()

def return_related_IDs(ID_No_to_search):
    b=c.execute("SELECT PoI_No,PoA_No FROM cdbs WHERE PoI_No = ? OR PoA_No = ?",(ID_No_to_search,ID_No_to_search)).fetchall()
    IDs1_set=set()    
    for i in b:
        for j in i:
            if len(j)>3:
                IDs1_set.add(j)
    IDs1=list(IDs1_set)      #1st iteration complete    
    if len(IDs1_set)==1:
        return IDs1

    IDs2_set=set()
    for i in IDs1_set:
        if i==ID_No_to_search:
            IDs2_set.add(i)
        else:
            b=c.execute("SELECT PoI_No,PoA_No FROM cdbs WHERE PoI_No = ? OR PoA_No = ?",(i,i)).fetchall()
            for k in b:
                for l in k:
                    if len(l)>3:
                        IDs2_set.add(l)
    IDs2=list(IDs2_set)    #2nd iteration is complete
    
    NewIDs_set=IDs2_set-IDs1_set
    
    if len(NewIDs_set)==0:
        return IDs2
    else:
        #print("inside 3rd iteration")
        #print(len(NewIDs_set))
        for i in NewIDs_set:
            b=c.execute("SELECT PoI_No,PoA_No FROM cdbs WHERE PoI_No = ? OR PoA_No = ?",(i,i)).fetchall()
            for k in b:
                for l in k:
                    if len(l)>3:
                        IDs2.append(l)
        return list(set(IDs2))
    


uniq_ID=c.execute("SELECT * FROM uniq_IDs where length(ID_No)>1").fetchall()
len_uniq_ID=str(len(uniq_ID))

df=pd.DataFrame(columns=['row','PoI_No', 'No of Related_PoI', 'freq','related_PoI'])

history=set()
ID_count=0

start_time=time.time()
row=0 

for j in uniq_ID:
    for i in j:
        if len(i)<4:
            continue
        row=row+1
        i=str(i)
        ID_count=ID_count+1
        #print(i)
        if i in history:
            continue
        z=(return_related_IDs(i))

        n=0
        

        for y in z:
            y=str(y)

            query="SELECT COUNT(*) FROM cdbs WHERE PoI_No='"+y+"' OR PoA_No='"+y+r"'"
            v=c.execute(query).fetchall()
            n=n+v[0][0]
            history.add(y)
            ID_count=ID_count+1
        
        
#        h=history['ID'].unique().tolist()    
        dic = {'row':[row+len(z)],'PoI_No':[i], 'No of Related_PoI':[len(z)], 'freq':[n],'related_PoI':[z]}
        dd = pd.DataFrame(dic)
        df=df.append(dd)
        
    #    print(row , df)  
#        f = open(path+'output3.csv', 'a')
#        df.to_csv(f, header=False, encoding='utf-8')
#        f.close()
        
        if row % 500 == 0:
#            history.to_csv(path+"history.csv", mode='a',encoding='utf-8')
            print("\n\n searching for--"+str(row)+" th record in uniq_ID--"+str(i)+"--total uniq id--"+len_uniq_ID+" serched   "+str(ID_count))
            f=open(path+'output3.csv','a')
            df.to_csv(f,header=None,encoding='utf-8')
            f.close()
            
            df=pd.DataFrame(columns=['row','PoI_No', 'No of Related_PoI', 'freq','related_PoI'])



conn.commit()
conn.close()  

