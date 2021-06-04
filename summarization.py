import pymssql
import pandas as pd
import redis
import time
import datetime

REDIS_IP = '10.2.5.212'
BATCH_SIZE = 10
MAX_TEXT_SIZE = 1023


def ms_sql_con():
    sql_name = 'voice_ai'
    sql_server = '10.2.4.124'
    sql_login = 'ICECORP\\1c_sql'

    with open('sql.pass','r') as file:
        sql_pass = file.read().replace('\n', '')
        file.close()

    return pymssql.connect(
            server = sql_server,
            user = sql_login,
            password = sql_pass,
            database = sql_name,
            autocommit=True
        )


def read_sql(query):
    return pd.read_sql(query, con=ms_sql_con(), parse_dates=None)


def concatenate_linkedid_side(side, record_date, linkedid):
    query = "SELECT text from transcribations where "
    query += " side="+str(side)+" and "
    query += " record_date = '"+str(record_date)+"' and "
    query += " linkedid = '"+str(linkedid)+"';"
    text_df = read_sql(query)
    phrases_count = len(text_df)
    text_full = ', '.join([row.text for _id, row in text_df.iterrows()])
    return text_full, phrases_count


def summarize(text, phrases_count):
    if phrases_count<2 or len(text)<255:
        return text
    subscriber = redis.StrictRedis(host=REDIS_IP)
    publisher = redis.StrictRedis(host=REDIS_IP) 
    pub = publisher.pubsub()
    sub = subscriber.pubsub()
    sub.subscribe('summarus_client')
    # send
    publisher.publish("summarus_server", text)
    # receive
    while True:
        message = sub.get_message()
        if message and message['type']!='subscribe':
            return message['data'].decode("utf-8")
        time.sleep(1)


def sum_to_sql(linkedid, recor_date, side, text, phrases_count, text_length):
    current_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    query = "insert into summarization(linkedid, record_date, sum_date, side, text, phrases_count, text_length) "
    query += " values("
    query += "'"+str(linkedid)+"',"
    query += "'"+str(recor_date)+"',"
    query += "'"+str(current_date)+"',"
    query += str(side)+","
    query += "'"+str(text[:MAX_TEXT_SIZE])+"',"
    query += "'"+str(phrases_count)+"',"
    query += "'"+str(text_length)+"'"
    query += ");"

    conn = ms_sql_con()  
    cursor = conn.cursor()
    cursor.execute(query)


while True:

    # obtain datetime limits
    query = "select min(record_date) from queue;"
    df = read_sql(query)
    queue_first_record = str(df.iloc()[0][0])
    query = "select max(record_date) from summarization;"
    df = read_sql(query)
    summarization_first_record = str(df.iloc()[0][0])

    query = "SELECT distinct top "+str(BATCH_SIZE)+" record_date, linkedid"
    query += " from transcribations"
    query += " where "
    query += " record_date < '"+queue_first_record+"' and"
    query += " not linkedid in (select distinct linkedid from summarization)"
    query += " order by record_date desc;"
    df = read_sql(query)

    for _id, row in df.iterrows():
        
        for side in range(2):
            text_full, phrases_count = concatenate_linkedid_side(side, row.record_date, row.linkedid)
            text_short = summarize(text_full, phrases_count)
            sum_to_sql(row.linkedid, row.record_date, side, text_short, phrases_count, len(text_full))
    break # TODO: remove
