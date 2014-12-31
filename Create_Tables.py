import MySQLdb
import sys
import csv
import os
import time

try:
    conn = MySQLdb.connect(db='andreacaro$Investimenti', host='mysql.server', user='andreacaro', passwd='MySQL01')

except MySQLdb.Error, e:
    print "Error %d: %s" % (e.args[0],e.args[1])
    sys.exit(1)

with conn:
    cur = conn.cursor()
    # to show which table exists in the db
    # cur.execute("SHOW TABLES")
    # tables = cur.fetchall()
    # print tables

    # files = filter(os.path.isfile, os.listdir( os.curdir ) ) # shows files only
    # files = os.listdir(os.curdir) # shows files and directory
    # print files

    # cur.execute('DROP TABLE IF EXISTS AnagTitoli')
    # cur.execute('CREATE TABLE AnagTitoli ()')


    # crea Anag
    FileImport = csv.reader(open('Index_Calculation/Anag_ItaF_2014.12.27.csv', "rU"))
    cur.execute('DROP TABLE IF EXISTS AnagTitoli')
    cur.execute('create table AnagTitoli (Data date, ID int, ISIN varchar(12),Descrizione varchar(50))')
    for row in FileImport:
        #print(row[0], row[1],row[2])
        cur.execute('insert into AnagTitoli VALUES (' + time.strftime("'%Y-%m-%d'") +',%s, %s, %s)', row)

    # crea CompoIndici
    FileImport = csv.reader(open('Index_Calculation/Compo_ItaF_2014.12.27.csv', "rU"))
    cur.execute('DROP TABLE IF EXISTS CompoIndici')
    cur.execute('create table CompoIndici (Data date, IndexDes varchar(15), ID int, Peso float)')
    for row in FileImport:
        #print(row[0], row[1],row[2])
        cur.execute('insert into CompoIndici VALUES (' + time.strftime("'%Y-%m-%d'") +',%s, %s, %s)', row)

    # crea Prezzi
    cur.execute('DROP TABLE IF EXISTS Prezzi')
    cur.execute('create table Prezzi (Data date, ID int, Prezzo float, Fonte varchar(15))')

conn.close()