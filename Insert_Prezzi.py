# -*- coding: utf-8 -*-

# Import_Prezzi.py
#
# serve a gestire in un unico modulo/funzione l'inserimento dei prezzi su DB

try:
    conn = MySQLdb.connect(db='andreacaro$Investimenti', host='mysql.server', user='andreacaro', passwd='MySQL01')


def insert_px (data, id, prezzo, fonte):
    query='insert into Prezzi (Data, ID, Prezzo, Fonte) select %s,%s,%s,%s'
    cur.execute(query,(data.strftime('%Y-%m-%d'), id, prezzo, fonte)
