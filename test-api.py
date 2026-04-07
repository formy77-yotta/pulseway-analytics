import psycopg2
pg = psycopg2.connect('postgresql://postgres:McNWCNyepgDdLtgGSzKaOKfawBLIFzys@hopper.proxy.rlwy.net:43881/railway')
cur = pg.cursor()
cur.execute('DROP TABLE IF EXISTS ticket_notes CASCADE')
pg.commit()
print('Tabella eliminata OK')
pg.close()