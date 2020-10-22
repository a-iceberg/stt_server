# Sentiment analysis
# https://demo.deeppavlov.ai/#/ru/sentiment

from deeppavlov import build_model, configs
import pandas as pd
from init_server import stt_server
import time

BATCH_SIZE = 1000

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
	
	query = """
		select top """+str(BATCH_SIZE)+""" 
		id,	text, sentiment
		from transcribations 
		where sentiment is NULL and text!=''
		order by transcribation_date, start
		"""

	df = pd.read_sql(query, server_object.conn)
	
	if len(df)>0:
		
		#else:
		print( 'solving '+str(len(df))+' records' )

		#TODO: move model over the cycle (test)
		model = build_model(configs.classifiers.rusentiment_bert, download=False) #download first time
		df['sentiment'] = model(df.text)

		update_record(server_object, df)
	
	print(datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S'), 'sleeping 600s..')
	time.sleep(600)
