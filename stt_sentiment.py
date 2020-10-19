# Sentiment analysis
# https://demo.deeppavlov.ai/#/ru/sentiment

from deeppavlov import build_model, configs
import json
import pandas as pd
import datetime

BATCH_SIZE = 100
input_file_name = 'in.json'
output_file_name = 'out.csv'

def solve_sentences(df):

	sentences = df.text

	model = build_model(configs.classifiers.rusentiment_bert, download=True) #download first time
	print('sentences',sentences.shape)
	res = model(sentences)

	df['res'] = res

	return df
	
#"update transcribations set sentiment = 'test', sentiment_pos = 0, sentiment_neg = 1 where audio_file_name='in_9163652085_2020-10-09-07-11-39rxtx.wav' and side = 0 and start = 10.83 and transcribation_date='2020-10-09 08:36:55':
#"select top 10 * from transcribations where sentiment is NULL and text!=''"

print('solving..')
df_res = pd.DataFrame(columns=['batch','month','from','text','res'])
for d in df.batch.unique():	
	print('batch',d)
	df_res = df_res.append( solve_sentences(df[df.batch==int(d)]) )

print('save sentiment data to csv..')
df_res.to_csv(output_file_name)

print('Happy end! exit..')