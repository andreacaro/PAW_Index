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
# moduli per importare mail
import smtplib
from email.mime.multipart import MIMEMultipart
from email.MIMEText import MIMEText

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
    	#if px==None:
    	#	titolo.prezzo=0
    	if px<>None:
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

def send_message(TEXT):
	FROM = "Index"
	TO = ['andrea.caroggio@gmail.com','caroggio@nextampartners.com'] #must be a list
	gmail_user = "calcolo.indici@gmail.com"
	gmail_pwd = "Indici01"
	SUBJECT = 'Test invio indice'
	#TEXT = 'Ciao,\n questa mail e\' stata generata automaticamente. \n Non mi rispondere perche\' nessuno mi controllera\'. \n Per suggerimenti e reclami sai gia\' a chi rivolgerti.'

	 # Prepare actual message
	#message = """\From: %s\nTo: %s\n\n%s
	#""" % (FROM, ", ".join(TO), TEXT)

	msg = MIMEMultipart('alternative')
	server = smtplib.SMTP("smtp.gmail.com", 587)
	server.ehlo()
	server.starttls()
	server.login(gmail_user, gmail_pwd)

	# commentato, serve ad allegare file
	#filename = 'TestIndice.txt'
	#f = file(filename)
	#attachment = MIMEText(f.read())
	#attachment.add_header('Content-Disposition', 'attachment', filename=filename)
	#msg.attach(attachment)

	content = MIMEText(TEXT, 'plain')
	msg.attach(content)
	msg['Subject'] = SUBJECT
	server.sendmail(FROM, TO, msg.as_string())

# # # # # # # # # # # # # # # # # # # # # # #
# fine dichiarazione funzioni, inizio codice
# # # # # # # # # # # # # # # # # # # # # # #

# Verifica che tipo di parametro viene passato
# 0 esecuzione scraping
# 1 esecuzione di prova per verificare solo se ci sono problei con i titoli

try:
    TipoExec=sys.argv[1]
    TipoExec=int(TipoExec)
except:
    TipoExec=0

try:
    conn = MySQLdb.connect(db='andreacaro$Investimenti', host='mysql.server', user='andreacaro', passwd='MySQL01')

except MySQLdb.Error, e:
    print "Error %d: %s" % (e.args[0],e.args[1])
    sys.exit(1)

cur = conn.cursor()
# prima scrittura LOG
insert_log('START') if TipoExec==0 else insert_log('START check ' + str(TipoExec))

if TipoExec==0:
    # nella versione di scraping prende tutta l'anag
    cur.execute("select a.id, a.isin from AnagTitoli a where a.Obsoleto=0") #check una tantum sull'anagrafica
if TipoExec==1:
    # nel check verifica solo i titoli degli indici
    cur.execute("select c.id, a.isin from CompoIndici c left join AnagTitoli a on c.id=a.id where c.Data =(select max(Data) from CompoIndici) group by c.id,a.isin")
if TipoExec==2:
    # check ad hoc
    cur.execute("select a.id, a.isin from AnagTitoli a where a.id in(20041,20017,20051,20073,20236,20250,20286)")
rows=cur.fetchall()

#crea una lista di istanze oggetto titolo e la popola con id, isin e url
ListaIds=[]
for row in rows:
    # Istanzia gli oggetti dentro la lista
	ListaIds.append(titolo(id=row[0],isin=row[1]))

#crea una lista con gli ISIN che non stati caricati
ListaISINErr=[]

# iizia scraping
for i, tit in enumerate(ListaIds):
    print 'Giro num ' + str(i+1) + ' di ' + str(len(ListaIds))
    try:
        url='http://www.borsaitaliana.it/borsa/azioni/scheda.html?isin=' + tit.isin + '&lang=it'
    except TypeError:
        insert_log('errore con url id ' + str(tit.id))
        print 'errore con url id ' + str(tit.id)
        print 'prob. ID non in anagrafica'
        continue

    soup=carica_url_req(url)
    #da correggere individuazione pagina non caricata
    if soup==None or len(soup)==1:
        ListaISINErr.append('errore url ' + tit.isin)
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
        #print tit.prezzo
        if tit.prezzo<>None:
            query="insert into LogScraper (DataEx, Module, ID, Isin, url, PageLoaded, Scraped) select %s,%s,%s,%s,%s,%s,1"
            cur.execute(query,(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), os.path.basename(__file__),tit.id, tit.isin, url, PageLoaded))

            # essendo l'IF in cui è tutto ok oltre a popolare log carico anche i prezzi
            try:
                if TipoExec==0: insert_px(tit.data,tit.id, tit.prezzo)
            except:
                insert_log('errore generico su insert id ' + str(tit.id))
                print 'errore generico su insert id'  + str(tit.id)
                continue
            #query='insert into Prezzi (Data, ID, Prezzo, Fonte) select %s,%s,%s,%s'
            #cur.execute(query,(tit.data.strftime('%Y-%m-%d'), tit.id, tit.prezzo, os.path.basename(__file__)))

        else:
            insert_log('manca quotazione per ID ' + str(tit.id))
            ListaISINErr.append('errore prezzo ISIN ' + tit.isin)
            query="insert into LogScraper (DataEx, Module, ID, Isin, url, PageLoaded, Scraped) select %s,%s,%s,%s,%s,%s,0"
            cur.execute(query,(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), os.path.basename(__file__),tit.id, tit.isin, url, PageLoaded))

    #scrive tentativo log
    #query='insert into LogScraper (DataEx, Module, ID, Isin, url, PageLoaded, Scraped) select %s,%s,%s,%s,%s,%s,%s'
    #cur.execute(query,(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), os.path.basename(__file__),tit.id, tit.isin, url, PageLoaded, Scraped))
    cur.fetchall()
    conn.commit()

# verifica i dati degli indici
query='select c.id, a.isin, p.prezzo \
        from CompoIndici c left join AnagTitoli a on c.id=a.id \
                            left join Prezzi p on c.id=p.id \
        where c.Data =(select max(Data) from CompoIndici) and p.Data=(select max(Data) from Prezzi)'

# fine scrittura LOG
insert_log('END')
conn.commit()
conn.close

if (TipoExec==0 or TipoExec==1):
    if len(ListaISINErr)==0:
        send_message('Tutto ok, tutti i titoli sono disponibili')
    else:
        send_message('Errore con gli isin ' + ListaISINErr)
