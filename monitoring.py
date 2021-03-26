import pymssql as ms_sql
import datetime
import requests
import urllib
import pandas as pd
import matplotlib.pyplot as plt
import telebot


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


msg = 'Состояние системы расшифровки аудиозаписей\n'
msg += transcribed_yesterday() + '\n'
msg += queue_len() + '\n'
msg += queue_by_cpu() + '\n'
msg += sentiment_queue()+ '\n'
msg += earliest_records()
print(msg)
send_to_telegram('-1001443983697', msg)
queue_time_vs_date('-1001443983697')