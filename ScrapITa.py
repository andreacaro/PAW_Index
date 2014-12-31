# coding: utf-8

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

except MySQLdb.Error, e:
    print "Error %d: %s" % (e.args[0],e.args[1])
    sys.exit(1)

class titolo:
	def __init__(self, id=None, isin=None, prezzo=None, fonte=None, peso=None, data=time.strftime("%H:%M:%S")):
		self.id=id
		self.isin=isin
		self.prezzo=prezzo
		self.fonte=fonte
		self.peso=peso
		self.data=data

def carica_url_req(url):
    ChiaveRicerca=SoupStrainer('table',{'summary':'Price data'})
    try:
        page = requests.get(url,timeout=15)
        soup=BeautifulSoup(page.text.encode('utf-8'), parse_only=ChiaveRicerca)
        return soup
    except socket.timeout:
		print 'errore'

def elabora_url(titolo,soup):
    try:
    	px=soup.findAll('tr')[1].findAll('td')[0].string
    	if px==None:
    		titolo.prezzo=0
    	else:
    	    prezzo=px.replace('(BTF)','')
            prezzo=prezzo.replace('.','')
            prezzo=prezzo.replace(',','.')
            #prende la data
            dt_temp=soup.findAll('tr')[1].findAll('td')[2].string
            dt_temp=dt_temp[0:8]
            dt=parse(dt_temp,dayfirst=True).date()
            titolo.data=dt
            titolo.prezzo=prezzo
    except:
        pass

def insert_px (data, id, prezzo):
    query='insert into Prezzi (Data, ID, Prezzo, FonteID) select %s,%s,%s,%s'
    cur.execute(query,(data.strftime('%Y-%m-%d'), id, prezzo, 11))
    #conn.commit()

def insert_log (descrizione):
    query='insert into Log (Data, Modulo, Descrizione) select %s,%s,%s'
    cur.execute(query,(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), os.path.basename(__file__),descrizione))
    #conn.commit()

# # # # # # # # # # # # # # # # # # # # # # #
# fine dichiarazione funzioni, inizio codice
# # # # # # # # # # # # # # # # # # # # # # #

cur = conn.cursor()
# prima scrittura LOG
insert_log('START')

cur.execute("select a.id, a.isin from AnagTitoli a where a.Obsoleto=0") #check una tantum sull'anagrafica
#cur.execute("select c.id, a.isin from CompoIndici c left join AnagTitoli a on c.id=a.id where c.Data =(select max(Data) from CompoIndici)")
rows=cur.fetchall()

#crea una lista di istanze oggetto titolo e la popola con id, isin e url
ListaIds=[]
for row in rows:
    # Istanzia gli oggetti dentro la lista
	ListaIds.append(titolo(id=row[0],isin=row[1]))

# iizia scraping
for i, tit in enumerate(ListaIds):
    try:
        url='http://www.borsaitaliana.it/borsa/azioni/scheda.html?isin=' + tit.isin + '&lang=it'
    except TypeError:
        insert_log('errore con id ' + str(tit.id))
        print 'errore con id ' + str(tit.id)
        print 'prob. ID non in anagrafica'
        continue

    print url
    soup=carica_url_req(url)
    #da correggere individuazione pagina non caricata
    if soup==None or len(soup)==1:
        # print 'errore con id ' + tit.id
        # pagina non caricata
        insert_log('mancato caricamento pagina id ' + str(tit.id))
        PageLoaded=0
        query='insert into LogScraper (DataEx, Module, ID, Isin, url, PageLoaded) select %s,%s,%s,%s,%s,%s'
        cur.execute(query,(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), os.path.basename(__file__),tit.id, tit.isin, url, PageLoaded))
    else:
        # pagina caricata correttamente
        PageLoaded=1
        elabora_url(tit,soup)
        if tit.prezzo<>None:
            query='insert into LogScraper (DataEx, Module, ID, Isin, url, PageLoaded, Scraped) select %s,%s,%s,%s,%s,%s,%s'
            cur.execute(query,(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), os.path.basename(__file__),tit.id, tit.isin, url, PageLoaded, 1))

            # essendo l'IF in cui è tutto ok oltre a popolare log carico anche i prezzi
            try:
                insert_px(tit.data,tit.id, tit.prezzo)
            except:
                insert_log('errore generico su insert id ' + str(tit.id))
                print 'errore generico su insert'  + str(tit.id)
                continue
            #query='insert into Prezzi (Data, ID, Prezzo, Fonte) select %s,%s,%s,%s'
            #cur.execute(query,(tit.data.strftime('%Y-%m-%d'), tit.id, tit.prezzo, os.path.basename(__file__)))

        else:
            query='insert into LogScraper (DataEx, Module, ID, Isin, url, PageLoaded) select %s,%s,%s,%s,%s,%s'
            cur.execute(query,(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), os.path.basename(__file__),tit.id, tit.isin, url, PageLoaded))

    #scrive tentativo log
    #query='insert into LogScraper (DataEx, Module, ID, Isin, url, PageLoaded, Scraped) select %s,%s,%s,%s,%s,%s,%s'
    #cur.execute(query,(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), os.path.basename(__file__),tit.id, tit.isin, url, PageLoaded, Scraped))
    cur.fetchall()
    conn.commit()

# fine scrittura LOG
insert_log('END')
conn.commit()