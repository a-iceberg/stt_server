from vosk import Model, KaldiRecognizer, SetLogLevel
import wave
import json
import pandas as pd
import time
import os
import uuid

model_path = '/home/alex/projects/vosk-api/python/example/model'

def transcribe_to_sql(filepath, filename, conn, settings, side):
		
	# read file
	wf = wave.open(filepath+filename, "rb")

	# read model
	model = Model(model_path)
	rec = KaldiRecognizer(model, wf.getframerate())

	# recognizing
	current_frame = 0

	while True:
		data = wf.readframes(4000)
		if len(data) == 0:
			break
		if rec.AcceptWaveform(data):
			accept = json.loads(rec.Result())
			if accept['text'] !='':
				
				accept_start = accept['result'][0]['start']
				accept_text = accept['text']
				
				# save to sql
				cursor = conn.cursor()
				current_date = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
				sql_query = "insert into transcribations (filename, date, date_y, date_m, date_d, text, start, side) "
				sql_query += "values ('"+filename+"','"+current_date+"','"+date_y+"','"+date_m+"','"+date_d+"','"+accept_text+"','"+accept_start+"',"+str(side)+");"
				cursor.execute(sql_query)
				conn.commit()
				

def get_stt_df(filename,side,model_path):

		# prepare dataframe
		index = ['','','','']
		columns = ['start','end','text','side']
		df = pd.DataFrame(index=index, columns=columns)
		df = df.dropna()

		# read file
		wf = wave.open(filename, "rb")
		if wf.getnchannels() != 1 or wf.getsampwidth() != 2 or wf.getcomptype() != "NONE":
				print ("Audio file must be WAV format mono PCM.")
				exit (1)

		# read model
		model = Model(model_path)
		rec = KaldiRecognizer(model, wf.getframerate())

		# recognizing
		current_frame = 0

		while True:
			data = wf.readframes(4000)
			if len(data) == 0:
					break
			if rec.AcceptWaveform(data):
					accept = json.loads(rec.Result())
					if accept['text'] !='':
							accept_start = accept['result'][0]['start']
							accept_text = accept['text']
							df.loc[current_frame] = [accept_start,'end',accept_text,side]
							print(current_frame)
							current_frame+=1
							#if current_frame>3: # debug
							#       break

		return df

def mine_task(file_path):

	# recognize
	df_a = get_stt_df(file_path+'_l.wav','>>',model_path)
	df_b = get_stt_df(file_path+'_r.wav','<<',model_path)

	# merge and sort
	df_c = pd.concat([df_a, df_b]).sort_values('start')
	result = ''
	for i in range (0,len(df_c)):
		row = df_c.iloc()[i]
		result += row.side+' '+row.text+'\n'

	return result
	
def get_file_splitted(audio_path,script_path):
	
	temp_storage_path = script_path+'files/'
	filename = str(uuid.uuid4())

	os.system('ffmpeg -i '+audio_path+' -ar 16000 -af "pan=mono|c0=FL" '+temp_storage_path+out_path+'_l.wav')
	os.system('ffmpeg -i '+audio_path+' -ar 16000 -af "pan=mono|c0=FR" '+temp_storage_path+out_path+'_r.wav')

	return out_path
	