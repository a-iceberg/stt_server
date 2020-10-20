# Sentiment analysis
# https://demo.deeppavlov.ai/#/ru/sentiment

from deeppavlov import build_model, configs
import pandas as pd
from init_server import stt_server

BATCH_SIZE = 3
	
#"update transcribations set sentiment = 'test', sentiment_pos = 0, sentiment_neg = 1 where audio_file_name='in_9163652085_2020-10-09-07-11-39rxtx.wav' and side = 0 and start = 10.83 and transcribation_date='2020-10-09 08:36:55':
#"select top 10 * from transcribations where sentiment is NULL and text!=''"

server_object = stt_server(0)

query = """
    select top """+str(BATCH_SIZE)+""" 
    id,
    text,
    sentiment
    from transcribations 
    where sentiment is NULL and text!=''
    """

print('solving..')
df = pd.read_sql(query, server_object.conn)
model = build_model(configs.classifiers.rusentiment_bert, download=True) #download first time
res = model(df.text)
df['sentiment'] = model(df.text)

print(df)

print('Happy end! exit..')