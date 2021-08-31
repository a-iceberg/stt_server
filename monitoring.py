import pymssql as ms_sql
import datetime
import requests
import urllib
import pandas as pd
import matplotlib.pyplot as plt
import telebot
import os
from collections import namedtuple
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns


def connect_mssql():
    ms_sql_base = 'voice_ai'
    ms_sql_server = '10.2.4.124'
    ms_sql_login = 'ICECORP\\1c_sql'

    with open('/home/alex/projects/call_centre_stt_server/sql.pass', 'r') as file:
        ms_sql_pass = file.read().replace('\n', '')
        file.close()

    return ms_sql.connect(ms_sql_server,
                          ms_sql_login,
                          ms_sql_pass,
                          ms_sql_base)


def read_sql(query):
    return pd.read_sql(query, con=connect_mssql(), parse_dates=None)


def queue_len():
    result = ''
    ms_sql_conn = connect_mssql()
    with ms_sql_conn:
        query = 'select count(distinct filename) as queued from queue;'
        cursor = ms_sql_conn.cursor()
        cursor.execute(query)
        for row in cursor.fetchall():
            result += 'В очереди: ' + str(row[0])
    return result


def queue_by_cpu():
    result = 'Очередь по ядрам:\n'
    ms_sql_conn = connect_mssql()
    with ms_sql_conn:
        query = 'select '
        query += 'cpu_id, '
        query += 'count(distinct filename) as filename '
        query += 'from queue group by cpu_id order by cpu_id;'

        cursor = ms_sql_conn.cursor()
        cursor.execute(query)
        for row in cursor.fetchall():
            result += '['+str(row[0])+']: '+str(row[1])+'\n'
    return result


def transcribed_yesterday():
    result = 'Распознанно за вчера: '

    currentdate = datetime.datetime.today()
    start_of_day = currentdate.combine(currentdate.date(), currentdate.min.time())
    yesterday = start_of_day + datetime.timedelta(days=-1)
    df = str(yesterday)
    dt = str(start_of_day)
    f_0 = '%Y-%m-%d %H:%M:%S'
    f_1 = '%Y-%m-%dT%H:%M:%S'
    date_from = datetime.datetime.strptime(df, f_0).strftime(f_1)
    date_toto = datetime.datetime.strptime(dt, f_0).strftime(f_1)

    ms_sql_conn = connect_mssql()
    with ms_sql_conn:
        query = "select count(distinct audio_file_name) as transcribed from transcribations"
        query += " where transcribation_date>'" + date_from + "'"
        query += " and transcribation_date<'" + date_toto + "';"
        cursor = ms_sql_conn.cursor()
        cursor.execute(query)
        for row in cursor.fetchall():
            result += str(row[0])
    return result


def sentiment_queue():
    result = 'Очередь тональности:\n'
    ms_sql_conn = connect_mssql()
    with ms_sql_conn:
        query = "select count(id) from transcribations where sentiment is NULL and text!='';"
        cursor = ms_sql_conn.cursor()
        cursor.execute(query)
        for row in cursor.fetchall():
            result += str(row[0])
    return result


def earliest_records():
    result = 'Самые ранние записи в очереди (1 - кц, 2 - мрм):\n'
    ms_sql_conn = connect_mssql()
    with ms_sql_conn:
        query = "select source_id, min(record_date) from queue group by source_id order by source_id;"
        cursor = ms_sql_conn.cursor()
        cursor.execute(query)
        for row in cursor.fetchall():
            result += str(row[0]) + ': ' + str(row[1]) + '\n'
    return result


def send_to_telegram(chat_id, message):
    with open('/home/alex/projects/call_centre_stt_server/telegram_token.key', 'r') as file:
        token = file.read().replace('\n', '')
        file.close()
    session = requests.Session()
    session.get(
        'https://api.telegram.org/bot' + token + '/sendMessage?chat_id=' + chat_id + '&text=' + urllib.parse.quote_plus(
            message))


def read_sql(query):
    return pd.read_sql(query, con=connect_mssql(), parse_dates=None)


def colorator(source_id):
    return 'red' if source_id == 1 else 'green'


def disk_usage(path):
    """Return disk usage statistics about the given path.

    Returned valus is a named tuple with attributes 'total', 'used' and
    'free', which are the amount of total, used and free space, in Gbytes.
    """
    _ntuple_diskusage = namedtuple('usage', 'total used free')
    st = os.statvfs(path)
    free = st.f_bavail * st.f_frsize
    total = st.f_blocks * st.f_frsize
    used = (st.f_blocks - st.f_bfree) * st.f_frsize
    return str(_ntuple_diskusage(total//(1024**3), used//(1024**3), free//(1024**3)))


def queue_time_vs_date(group):

    query = "select"
    query += " source_id,"
    query += " DATEDIFF(second, date, getdate()) as queued_seconds_from_now,"
    query += " DATEDIFF(second, record_date, getdate()) as recorded_seconds_from_now"
    query += " from queue order by record_date;"

    queue = read_sql(query)

    if len(queue) > 0:

        queue['color'] = queue['source_id'].apply(colorator)

        q_a = queue[queue.source_id == 1]
        q_b = queue[queue.source_id == 2]

        # ratio = max(queue.queued_seconds_from_now) / max(queue.recorded_seconds_from_now) * 10
        fig, ax = plt.subplots(1, 1, figsize=(15, 10), dpi=80)

        ax.scatter(
            q_a.queued_seconds_from_now,
            q_a.recorded_seconds_from_now,
            color=q_a['color'],
            label="call",
            marker='x'
        )
        ax.scatter(
            q_b.queued_seconds_from_now,
            q_b.recorded_seconds_from_now,
            color=q_b['color'],
            label="mrm",
            marker='.'
        )
        currentdate = datetime.datetime.today()
        currentdate = currentdate.strftime('%Y.%m.%d %H:%M:%S')
        # datetime.datetime.strptime(df, f_0).strftime(f_1)
        ax.set_title('Очередь ' + currentdate, fontsize=18)

        # Set common labels
        ax.set_xlabel('Добавлено, сек. назад', fontsize=18)
        ax.set_ylabel('Записано, сек. назад', fontsize=18)

        plt.legend(bbox_to_anchor=(1, 1), loc='upper left', ncol=1)
        plt.savefig('queue.png')

        with open('/home/alex/projects/call_centre_stt_server/telegram_token.key', 'r') as file:
            token = file.read().replace('\n', '')
            file.close()
        bot = telebot.TeleBot(token)
        data_file = open('queue.png', 'rb')
        # bot.send_photo(group, data_file, caption="queue_time_vs_date")
        bot.send_photo(group, data_file)


def summarization():
    message = ''
    query = "SELECT count(linkedid) from summarization"
    query += " where isnull(load_msk,0) = 0 or isnull(load_spb,0) = 0 or isnull(load_reg,0) = 0;"
    df = read_sql(query)
    message += '\nСуммаризации, ожидающие загрузку: ' + str(df.iloc()[0][0])
    
    message += '\n== За вчера =='
    
    currentdate = datetime.datetime.today()
    start_of_day = currentdate.combine(currentdate.date(), currentdate.min.time())
    yesterday = start_of_day + datetime.timedelta(days=-1)
    df = str(yesterday)
    dt = str(start_of_day)
    f_0 = '%Y-%m-%d %H:%M:%S'
    f_1 = '%Y-%m-%dT%H:%M:%S'
    date_from = datetime.datetime.strptime(df, f_0).strftime(f_1)
    date_toto = datetime.datetime.strptime(dt, f_0).strftime(f_1)
    
    query = "SELECT count(linkedid) from summarization"
    query += " where sum_date>'" + date_from + "'"
    query += " and sum_date<'" + date_toto + "';"
    df = read_sql(query)
    message += '\nНовых суммаризаций: ' + str(df.iloc()[0][0])
    
    for city in ['load_msk', 'load_spb', 'load_reg']:
        query = "SELECT count(linkedid) from summarization"
        query += " where sum_date>'" + date_from + "'"
        query += " and sum_date<'" + date_toto + "'"
        query += " and isnull("+city+",0) = 1;"
        df = read_sql(query)
        message += '\n'+city+': ' + str(df.iloc()[0][0])      
    
    return message
	

def summarization_plot(group):
    print('summarization_plot temporary disabled')
    return
    query = "select min(record_date) from queue where not isnull(record_date,'')='';"

    queue_first_record = read_sql(query)
    queue_first_record = str(queue_first_record.iloc()[0][0])
    if queue_first_record == 'None':
        queue_first_record = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
    else:
        if type(queue_first_record)==type(''):
            queue_first_record = datetime.datetime.fromisoformat(queue_first_record)
        queue_first_record = queue_first_record.strftime('%Y-%m-%dT%H:%M:%S')

    start_time = (datetime.datetime.now() + datetime.timedelta(days=-2)).strftime('%Y-%m-%dT%H:%M:%S')
    print('summarization plot from', start_time)
    query = "SELECT distinct linkedid, record_date from summarization where"
    query += " record_date>'"+start_time+"' and not text='';"
    summarized = read_sql(query)
    query = "SELECT distinct linkedid, record_date from transcribations where"
    query += " record_date>'"+start_time+"' and record_date<'"+queue_first_record+"' and not text='';"
    transcribed = read_sql(query)
    if len(summarized)==0 or len(transcribed==0):
        print('summarized, transcribed', len(summarized)==0, len(transcribed==0), 'summarization_plot canceled')
    df = pd.merge(transcribed, summarized, how = 'left', on = 'linkedid')
    
    def isnat(s):
        return not s is pd.NaT
    df['summarized'] = df.record_date_y.apply(isnat)

    def start_of_minute(d):
        return d.strftime('%Y-%m-%dT%H:00:00')

    df.record_date_x = df.record_date_x.apply(start_of_minute)

    df.drop('record_date_y', 1, inplace = True)
    df.drop('linkedid', 1, inplace = True)

    sum_count = df[df.summarized].groupby('record_date_x').count()
    sum_count.reset_index(inplace = True)
    sum_count.columns = ['date', 'summarized']

    unsum_count = df[df.summarized == False].groupby('record_date_x').count()
    unsum_count.reset_index(inplace = True)
    unsum_count.columns = ['date', 'unsummarized']

    df = pd.merge(sum_count,unsum_count, how = 'outer', on = 'date')

    df.summarized.fillna(0, inplace=True)
    df.unsummarized.fillna(0, inplace=True)

    def crop_date(d):
        return d[:13].replace('T', ' ')
    df.date = df.date.apply(crop_date)


    header = 'Суммаризации \n_с '+str(start_time).replace('T', ' ')+'\nпо '+str(queue_first_record).replace('T', ' ')

    #header = 'Суммаризации'
    columns = df.columns[1:]
    mycolors = ['tab:blue', 'tab:red']

    # Draw Plot and Annotate
    fig, ax = plt.subplots(1,1,figsize=(16, 9), dpi = 80)

    labs = columns.values.tolist()

    # Prepare data
    x  = df['date'].values.tolist()
    y0 = df[columns[0]].values.tolist()
    y1 = df[columns[1]].values.tolist()
    y = np.vstack([y0, y1])

    # Plot for each column
    labs = columns.values.tolist()
    ax = plt.gca()
    ax.stackplot(x, y, labels=labs, colors=mycolors, alpha=0.8)

    # Decorations
    ax.set_title(header, fontsize=18)
    ax.legend(fontsize=10, ncol=4)
    plt.grid(alpha=0.5)

    # Lighten borders
    plt.gca().spines["top"].set_alpha(0)
    plt.gca().spines["bottom"].set_alpha(.3)
    plt.gca().spines["right"].set_alpha(0)
    plt.gca().spines["left"].set_alpha(.3)
    plt.gca().set_xticklabels(labels = df.date, rotation=30)
    # plt.show()
    plt.savefig('/home/alex/projects/call_centre_stt_server/summarization.png')

    with open('/home/alex/projects/call_centre_stt_server/telegram_token.key', 'r') as file:
        token = file.read().replace('\n', '')
        file.close()
    bot = telebot.TeleBot(token)
    data_file = open('/home/alex/projects/call_centre_stt_server/summarization.png', 'rb')
    # bot.send_photo(group, data_file, caption="queue_time_vs_date")
    bot.send_photo(group, data_file)

def summarization_queue_state(group):
    header = 'Очередь суммаризации'
    query = "SELECT CAST(sum_date AS DATE) as date, count(distinct linkedid) as linkedid"
    query += " FROM summarization"
    query += " group by CAST(sum_date AS DATE)"
    query += " order by CAST(sum_date AS DATE);"
    df = read_sql(query)
    fig, ax = plt.subplots(figsize=(16,10), dpi= 80)    
    sns.stripplot(df.date, df.linkedid, jitter=0.25, size=8, ax=ax, linewidth=.5)
    plt.gca().set_xticklabels(labels = df.date, rotation=90)
    # Decorations
    plt.grid(linestyle='--', alpha=0.5)
    plt.title(header, fontsize=22)
    # plt.show()
    plt.savefig('/home/alex/projects/call_centre_stt_server/summarization_2.png')
    with open('/home/alex/projects/call_centre_stt_server/telegram_token.key', 'r') as file:
        token = file.read().replace('\n', '')
        file.close()
    bot = telebot.TeleBot(token)
    data_file = open('/home/alex/projects/call_centre_stt_server/summarization_2.png', 'rb')
    # bot.send_photo(group, data_file, caption="queue_time_vs_date")
    bot.send_photo(group, data_file)

summarization_queue_state('-1001443983697')
# summarization_plot('-1001443983697')

msg = 'Состояние системы расшифровки аудиозаписей\n'
msg += transcribed_yesterday() + '\n'
msg += queue_len() + '\n'
msg += queue_by_cpu() + '\n'
msg += sentiment_queue()+ '\n'
msg += earliest_records()+ '\n'
msg += '/ ' + disk_usage('/')+ '\n'
msg += '/mnt/share/audio/ ' + disk_usage('/mnt/share/audio/')+ '\n'
msg += '/mnt/share/audio_master/ ' + disk_usage('/mnt/share/audio_master/')+ '\n'
msg += '/mnt/share/audio_call/ ' + disk_usage('/mnt/share/audio_call/')
msg += summarization()
print(msg)
send_to_telegram('-1001443983697', msg)
queue_time_vs_date('-1001443983697')
