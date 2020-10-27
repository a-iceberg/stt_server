# Sentiment analysis
# https://demo.deeppavlov.ai/#/ru/sentiment

from deeppavlov import build_model, configs
import pandas as pd
from init_server import stt_server
import time
import requests
import datetime

BATCH_SIZE = 1000

def send_to_telegram(chat,message):
	try:
		print('Telegram:',message)
		headers = {
			"Origin": "http://scriptlab.net",
			"Referer": "http://scriptlab.net/telegram/bots/relaybot/",
			'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.110 Safari/537.36'
			}
		url     = "http://scriptlab.net/telegram/bots/relaybot/relaylocked.php?chat="+chat+"&text="+urllib.parse.quote_plus(message)
		return requests.get(url,headers = headers)
	except Exception as e:
		return str(e)

def update_record(server_object, df):
	
	query = ''
	
	for index, row in df.iterrows():
		if row.sentiment == 'negative':
			neg = 1
			pos = 0
		else:
			neg = 0
			pos = 1
		query += "update transcribations set "
		query += "sentiment = '"+row.sentiment+"', "
		query += "sentiment_neg = "+str(neg)+", "
		query += "sentiment_pos = "+str(pos)+" "
		query += "where id = "+str(row.id)+";"
	
	cursor = server_object.conn.cursor()
	cursor.execute(query)
	server_object.conn.commit()
	

server_object = stt_server(0)

line = 0

while True:
	
	try:
		query = """
			select top """+str(BATCH_SIZE)+""" 
			id,	text, sentiment
			from transcribations 
			where sentiment is NULL and text!=''
			order by transcribation_date, start
			"""

		df = pd.read_sql(query, server_object.conn)
		if len(df)==0:
			print(datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S'), 'sleeping 10s..')
			time.sleep(10)
		else:
			print( 'solving '+str(len(df))+' records' )

			#TODO: move model over the cycle (test)
			model = build_model(configs.classifiers.rusentiment_bert, download=False) #download first time
			df['sentiment'] = model(df.text)

			update_record(server_object, df)
	except Exception as e:
		send_to_telegram('106129214',str(datetime.datetime.now())+' stt sentiment error: '+str(e))