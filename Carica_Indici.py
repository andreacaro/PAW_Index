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

    # crea CompoIndici
    FileImport = csv.reader(open('Index_Calculation/Pesi Indici 2014.12.30.csv', "rU"))
    for row in FileImport:
        #print(row[0], row[1],row[2])
        cur.execute('insert into CompoIndici VALUES (%s,%s, %s, %s)', row)

conn.close()