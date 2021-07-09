import io
from google.cloud import speech_v1p1beta1
from google.cloud import speech
from google.oauth2 import service_account
import pickle
import wave
# python3.7 -m pip install -U https://github.com/alphacep/vosk-api/releases/download/0.3.30/vosk-0.3.30-py3-none-linux_x86_64.whl
from vosk import Model, KaldiRecognizer, SetLogLevel
import json
import sys
import pandas as pd
import jiwer
import matplotlib.pyplot as plt
import datetime
import os
import numpy as np
import telebot

# python3.7 -m pip install jiwer

# file_path = '/media/alex/nvme-a/word_1/'
file_path = '/mnt/share/audio_call/saved_for_analysis/wer/'
# model_path = '/media/alex/nvme-a/vosk-model-ru-0.10'
model_path = '/mnt/share/audio_call/model_v0/model'
script_path = '/home/alex/projects/call_centre_stt_server/'
tg_group = '-1001443983697'

def transcribe_google(file_path):
    
    # https://cloud.google.com/speech-to-text
    # https://cloud.google.com/speech-to-text/docs/reference/rest/v1/RecognitionConfig?hl=ru#AudioEncoding
    # https://cloud.google.com/speech-to-text/docs/samples/speech-transcribe-async#speech_transcribe_async-python
    # conda deactivate
    # python3.7 -m pip install --upgrade google-cloud-speech

    credentials_json = 'iceberg-318906-dbe1223570ba.json'
    creds = service_account.Credentials.from_service_account_file(
    credentials_json,
    scopes=['https://www.googleapis.com/auth/cloud-platform'])
    language = 'ru-RU'
    client = speech_v1p1beta1.SpeechClient(credentials=creds)
    sample_rate_hertz = 8000
    encoding = speech_v1p1beta1.RecognitionConfig.AudioEncoding.MP3

    config = {
        "language_code": language,
        "sample_rate_hertz": sample_rate_hertz,
        "encoding": encoding,
    }
    with io.open(file_path, 'rb') as audio_file:
        content = audio_file.read()
        audio = speech_v1p1beta1.RecognitionAudio(content=content)

        response = client.recognize(config = config, audio = audio)
        results = []
        for result in response.results:
            alternative = result.alternatives[0]
            results.append(alternative.transcript)
    return ''.join([str(item) for item in results]).lower()


def transcribe_vosk(file_name, model_path):

    # https://alphacephei.com/vosk/
    phrases_list = []

     # read file
    wf = wave.open(file_name, "rb")

    # read model
    model = Model(model_path)
    rec = KaldiRecognizer(model, wf.getframerate())

    # recognizing
    while True:

        conf_score = []

        data = wf.readframes(4000)
        if len(data) == 0:
            break

        if rec.AcceptWaveform(data):
            accept = json.loads(rec.Result())
            if accept['text'] !='':
                #print(accept['text'])
                phrases_list.append(accept['text'])
    return phrases_list

def get_files(path):
    for root, dirs, files in os.walk(path):
        files.sort()
        return files

def evaluate(df, name):
    
    wer = []
    mer = []
    wil = []
    for row in range(len(df)):
        measures = error(df.iloc[row].reference_text, df.iloc[row][name + '_text'])
        wer.append(measures['wer'])
        mer.append(measures['mer'])
        wil.append(measures['wil'])
    df[name + '_wer'] = wer
    df[name + '_mer'] = mer
    df[name + '_wil'] = wil

    return df

def error(ground_truth, hypothesis):
    
    wer = jiwer.wer(ground_truth, hypothesis)
    mer = jiwer.mer(ground_truth, hypothesis)
    wil = jiwer.wil(ground_truth, hypothesis)

    # faster, because `compute_measures` only needs to perform the heavy lifting once:
    measures = jiwer.compute_measures(ground_truth, hypothesis)
    return measures

def send_report(evaluation, script_path, tg_group):
    try:
        mycolors = ['tab:red', 'tab:blue', 'tab:green', 'tab:orange', 'tab:brown', 'tab:gray']
        fig, ax = plt.subplots(1,1,figsize=(16, 9), dpi= 80)
        columns = evaluation.columns[1:]
        for i, column in enumerate(columns):
            plt.plot(evaluation.date.values, evaluation[column].values, lw=1.5, color=mycolors[i], label=column)
        plt.xticks(evaluation.date.values, rotation=60)
        plt.title('Model error rate\nlower - better')
        plt.legend()
        plt.savefig(script_path+'evaluation.png')

        with open(script_path+'telegram_token.key', 'r') as file:
            token = file.read().replace('\n', '')
            file.close()
        bot = telebot.TeleBot(token)
        with open(script_path+'report.png', 'rb') as data_file:
            print('sending photo to ', tg_group)
            bot.send_photo(tg_group, data_file)
    except Exception as e:
        print('send_report error:', str(e))

files = get_files(file_path)
param_date = sys.argv[1]
if param_date =='default':
    current_date = (datetime.datetime.now() + datetime.timedelta(days=-1)).strftime('%Y-%m-%d')
else:
    current_date = sys.argv[1]
evals_wer = []
evals_mer = []
evals_wil = []
for file in files:
    if current_date+'_' in file:
        print('file:', file)
        text_google = transcribe_google(file_path+file).replace('  ',' ')
        text_vosk = ' '.join(transcribe_vosk(file_path+file, model_path)).replace('  ',' ')
        measures = error(text_google, text_vosk)
        print(measures)
        evals_wer.append(measures['wer'])
        evals_mer.append(measures['mer'])
        evals_wil.append(measures['wil'])
        os.unlink(file_path+file) # should be disabled due premission troubles

print('avg: wer', np.average(evals_wer), 'mer', np.average(evals_mer), 'wil', np.average(evals_wil))
print('med: wer', np.median(evals_wer), 'mer', np.median(evals_mer), 'wil', np.median(evals_wil))

current = pd.DataFrame(columns = ['date', 'avg_wil', 'avg_wer', 'avg_mer', 'med_wil', 'med_wer', 'med_mer'])
current['date'] = pd.to_datetime(current_date).date()
current['avg_wil'] = [np.average(evals_wil)]
current['avg_wer'] = [np.average(evals_wer)]
current['avg_mer'] = [np.average(evals_mer)]
current['med_wil'] = [np.median(evals_wil)]
current['med_wer'] = [np.median(evals_wer)]
current['med_mer'] = [np.median(evals_mer)]

row = dict()
row['date'] = pd.to_datetime(current_date).date()
row['avg_wil'] = np.average(evals_wil)
row['avg_wer'] = np.average(evals_wer)
row['avg_mer'] = np.average(evals_mer)
row['med_wil'] = np.median(evals_wil)
row['med_wer'] = np.median(evals_wer)
row['med_mer'] = np.median(evals_mer)
current  = pd.DataFrame([row], columns=row.keys())

evaluation_file = script_path + 'evaluation.csv'
evaluation = pd.read_csv(evaluation_file)
evaluation = pd.concat([evaluation, current], axis = 0)
evaluation.to_csv(evaluation_file, index = False)

send_report(evaluation, script_path, tg_group)

print(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'job complete')
