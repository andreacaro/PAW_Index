import MySQLdb
import sys
import csv
import time

try:
    conn = MySQLdb.connect(db='andreacaro$Investimenti', host='mysql.server', user='andreacaro', passwd='MySQL01')

except MySQLdb.Error, e:
    print "Error %d: %s" % (e.args[0],e.args[1])
    sys.exit(1)

with conn:
    cur = conn.cursor()

    # crea Anag
    FileImport = csv.reader(open('Index_Calculation/Anag_ItaF_2014.12.27.csv', "rU"))
    cur.execute('DROP TABLE IF EXISTS AnagTitoli')
    cur.execute('create table AnagTitoli (Data date, ID int, ISIN varchar(12),Descrizione varchar(50))')
    for row in FileImport:
        #print(row[0], row[1],row[2])
        cur.execute('insert into AnagTitoli VALUES (' + time.strftime("'%Y-%m-%d'") +',%s, %s, %s)', row)


conn.close()