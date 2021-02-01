import pymssql as ms_sql
import datetime
import requests
import urllib


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
        query = 'select cpu_id, count(distinct filename) as filename from queue group by cpu_id order by cpu_id;'
        cursor = ms_sql_conn.cursor()
        cursor.execute(query)
        for row in cursor.fetchall():
            result += str(row[0])+': '+str(row[1])+'\n'
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


def send_to_telegram(chat_id, message):
    with open('/home/alex/projects/call_centre_stt_server/telegram_token.key', 'r') as file:
        token = file.read().replace('\n', '')
        file.close()
    session = requests.Session()
    session.get(
        'https://api.telegram.org/bot' + token + '/sendMessage?chat_id=' + chat_id + '&text=' + urllib.parse.quote_plus(
            message))


msg = 'Состояние системы расшивки аудиозаписей\n'
msg += transcribed_yesterday() + '\n'
msg += queue_len() + '\n'
msg += queue_by_cpu() + '\n'
msg += sentiment_queue()
print(msg)
send_to_telegram('-1001443983697', msg)

