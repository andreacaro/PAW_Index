# coding: utf-8

# 2015-01-02
# elabora indici.py
#
# calcola la serie storica degli indici

import MySQLdb
import sys
import os
import time
import datetime
from dateutil.parser import *
import socket 					#solo per alzare socke.error
import requests
from bs4 import BeautifulSoup
from bs4 import SoupStrainer

try:
    conn = MySQLdb.connect(db='andreacaro$Investimenti', host='mysql.server', user='andreacaro', passwd='MySQL01')
    cur = conn.cursor()

except MySQLdb.Error, e:
    print "Error %d: %s" % (e.args[0],e.args[1])
    sys.exit(1)

class IndexPoint:
	def __init__(self, data=time.strftime("%H:%M:%S"), IndexID=None, ID=None, peso=None):
		self.data=data
		self.IndexID=IndexID
		self.ID=ID
		self.peso=peso

# inserisce in una tupla l'elenco degli indici
query='select distinct c.IndexID from CompoIndici c inner join AnagIndici a on c.IndexID=a.IndexID order by 1'
cur.execute(query)

# uso results come passaggio temporaneo per la trasformazione in lista
results=cur.fetchall()
ListaIndexId=[x[0] for x in results]

# per ogni indice estrae le date da caricare su IndexValues
for Idx in ListaIndexId:
    query='select distinct p.data from Prezzi p left join CompoIndici c on p.data=c.data \
            where p.data >= (select DataIniz from AnagIndici where IndexID=%s) and p.data not in (select Data from TempCalcIdx where IndexID=%s) order by 1'
    cur.execute(query,(Idx, Idx))
    results=cur.fetchall()
    ListaDate=[x[0] for x in results]
    print ' Elenco date IndexID ' + str(Idx)
    print ListaDate

    for i, data in enumerate(ListaDate):
        print i
        print data
        # 1° step, capire se è primo insert o meno
        query="select * from TempCalcIdx where IndexID=%s"
        cur.execute(query,(Idx))
        check=cur.fetchall()

        if not check: #modo più pitonico di tutti per verificare se la lista esiste o è vuota
            print 'sto per inserire il primo record'
            # inserisce il primo record su IndexValues
            query="insert into IndexValues select Datainiz, indexid, startingvalue, startingvalue, startingvalue,0,0,0 from AnagIndici where IndexID=%s"
            cur.execute(query,(Idx))
            conn.commit()

            # inserisce il primo record su TempCalcIdx
            query="insert into TempCalcIdx (Data, TipoInsert, IndexID, ID, PxT, Ctv, Peso_f) \
                select c.Data, 'First', c.IndexID, c.id, p.prezzo, StartingValue/c.peso,c.peso  \
                from CompoIndici c left join Prezzi p on c.id=p.id and c.data=p.data \
                                    inner join AnagIndici a on c.IndexID=a.IndexID \
                where c.data=%s and c.indexid=%s"
            cur.execute(query,(data,Idx))
            conn.commit()

        else:
            # 2° step, capire se è un giorno normale o di ribilanciamento
            query="select * from CompoIndici where Data=%s and IndexID=%s "
            cur.execute(query,(data,Idx))
            check_b=cur.fetchall()

            if not check_b:
                # giorni normali, in quanto non di ribilanciamento # join con Temp è a -1 giorno, con Px è a T con zero, peso_f di T diventa peso_i
                query="insert into TempCalcIdx (Data, TipoInsert, IndexID, ID, Peso_i, Saldo, PxT, Ctv, Div_Gross, Div_Net, Perf_px, Perf_tr_gross, Perf_tr_net) \
                        select p.Data,'Recalculated',t.indexid,t.id,t.peso_f,t.Ctv/t.PxT, p.prezzo \
                            , p.prezzo*t.Ctv/t.PxT,d.Gross_Amount, d.Net_Amount, p.prezzo/t.PxT, (p.prezzo+d.Gross_Amount)/t.PxT, (p.prezzo+d.Net_Amount)/t.PxT  \
                        from TempCalcIdx t left join Prezzi p on t.id=p.id left join Dividendi d on p.Data=d.ExDate and p.ID=d.ID \
                        where t.indexid=%s and t.data=%s and p.data=%s"
                cur.execute(query,(Idx,ListaDate[i-1],ListaDate[i]))
                conn.commit()

                # query="select * from TempCalcIdx"
                # cur.execute(query)
                # print cur.fetchall()

                query="update TempCalcIdx as T inner join (select Data, IndexID, sum(ctv) as Ctv from TempCalcIdx) TC \
                        on T.IndexId=TC.IndexId and T.Data=TC.Data set T.Peso_f=T.Ctv/TC.Ctv  where T.IndexId=%s and T.Data=%s"
                cur.execute(query,(Idx, data))
                conn.commit()

query="select * from TempCalcIdx where IndexID=1 order by 1,3,4"
cur.execute(query)
rows=cur.fetchall()

f = open('tempor.csv', 'w')
#f.write ('Test 1')
#print >> f, ' \n Test 2'
for row in rows:
#    print str(row)
    f.write ('\n' + str(row))
#    f.write ('\n' + str(row))
f.close()

#for row in rows:
#    print row
conn.close()