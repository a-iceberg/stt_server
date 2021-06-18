import pymssql
import pandas as pd
import redis
import time
import datetime
import difflib

REDIS_IP = '10.2.5.212'
BATCH_SIZE = 1000
MAX_TEXT_SIZE = 1023
SCRIPT_PATH = '/home/alex/projects/call_centre_stt_server/'


def ms_sql_con():
    sql_name = 'voice_ai'
    sql_server = '10.2.4.124'
    sql_login = 'ICECORP\\1c_sql'

    with open(SCRIPT_PATH+'sql.pass','r') as file:
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
    query = "SELECT text, source_id from transcribations where "
    query += " side="+str(side)+" and "
    query += " record_date = '"+str(record_date)+"' and "
    query += " linkedid = '"+str(linkedid)+"';"
    text_df = read_sql(query)
    phrases_count = len(text_df)
    text_full = ', '.join([row.text for _id, row in text_df.iterrows()])
    return text_full, phrases_count, min(text_df.source_id)


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


def sum_to_sql(linkedid, recor_date, side, text, phrases_count, text_length, source_id):    
    current_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    query = "insert into summarization"
    query += "(linkedid, record_date, sum_date, side, text, phrases_count, text_length, source_id) "
    query += " values("
    query += "'"+str(linkedid)+"',"
    query += "'"+str(recor_date)+"',"
    query += "'"+str(current_date)+"',"
    query += str(side)+","
    query += "'"+str(text[:MAX_TEXT_SIZE]).replace("'","").replace('"','')+"',"
    query += "'"+str(phrases_count)+"',"
    query += "'"+str(text_length)+"',"
    query += ""+str(source_id)+""
    query += ");"

    # debug ++
    #print(current_date, linkedid, side)
    #print(query)
    # debug ++

    conn = ms_sql_con()  
    cursor = conn.cursor()
    cursor.execute(query)


def get_jaccard_sim(str1, str2): 
    a = set(str1.split()) 
    b = set(str2.split())
    c = a.intersection(b)
    denominator = (len(a) + len(b) - len(c))
    if denominator > 0:
        return float(len(c) / denominator)
    else:
        return 0


print('=== start ===')

# obtain datetime limits
query = "select linkedid from queue;"
df = read_sql(query)
if len(df):
    query = "select min(record_date) from queue where not isnull(record_date,'')='';"
    df = read_sql(query)
    queue_first_record = str(df.iloc()[0][0])
else:
    queue_first_record = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

"""query = "select max(record_date) from summarization;"
df = read_sql(query)
summarization_first_record = str(df.iloc()[0][0])"""

print(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'select from transcribations..')

# concatenate transcribations
query = "SELECT distinct top "+str(BATCH_SIZE)+" record_date, linkedid"
query += " from transcribations"
query += " where "
query += " record_date < '"+queue_first_record+"' and"
query += " not linkedid in (select distinct linkedid from summarization)"
query += " order by record_date desc;"
df = read_sql(query)

print(
    datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
    'received:', len(df),
    'from:', min(df.record_date),
    'to:', max(df.record_date)
    )

for _id, row in df.iterrows():
    
    for side in range(2):
        print(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 
        'concatenating',
            row.record_date,
            row.linkedid,
            side
        )
        text_full, phrases_count, source_id = concatenate_linkedid_side(side, row.record_date, row.linkedid)
        text_short = summarize(text_full.replace(',',' - '), phrases_count)

        # stage 1: find better approach
        text_short = summarize(text_full, phrases_count)
        #best_result = difflib.SequenceMatcher(None, text_full, text_short).ratio()
        # https://www.machinelearningmastery.ru/overview-of-text-similarity-metrics-3397c4601f50/
        best_result = get_jaccard_sim(text_full, text_short)
        # best_replacer = [',']
        for replacer in [' ', ' - ']:
            try_short = summarize(text_full.replace(',', replacer), phrases_count)
            #try_result = difflib.SequenceMatcher(None, text_full, try_short).ratio()
            try_result = get_jaccard_sim(text_full, text_short)
            if try_result > best_result:
                text_short = try_short
                best_result = try_result
                # best_replacer = replacer

        #for wrong in wrong_words:
        #    if wrong in text_short:
        #        text_short = summarize(text_full.replace(',',' '), phrases_count)
        #        break
        
        # fix wrong summarizations
        wrong_words = ['погиб', 'смерть', 'путин'] # high frequency newspaper words
        # stage 2: just crop if wrong words still in summarization
        for wrong in wrong_words:
            if wrong in text_short:
                print(row.linkedid, side, 'replaced, because found', wrong, 'in:', text_short)
                text_short = text_full[:1023]
                break

        sum_to_sql(row.linkedid, row.record_date, side, text_short, phrases_count, len(text_full), source_id)

print(
    datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
    'job complete:', len(df),
    'from:', min(df.record_date),
    'to:', max(df.record_date)
    )
