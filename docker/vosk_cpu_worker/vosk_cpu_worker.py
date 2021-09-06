import os
import socket
from init_server import stt_server

server_object = stt_server('http://127.0.0.1/transcribe')

print('worker_id:', server_object.cpu_id)

server_object.source_id=1 # call centre
mysql_conn = server_object.connect_mysql(server_object.source_id)
with mysql_conn:
	query = "select linkedid from PT1C_cdr_MICO limit 1;"
	cursor = mysql_conn.cursor()
	cursor.execute(query)
	for row in cursor.fetchall():
		print('mysql_conn[1]',row[0])

server_object.source_id=2 # call centre
mysql_conn = server_object.connect_mysql(server_object.source_id)
with mysql_conn:
	query = "select calldate from cdr limit 1;"
	cursor = mysql_conn.cursor()
	cursor.execute(query)
	for row in cursor.fetchall():
		print('mysql_conn[2]',row[0])



print('exit')
