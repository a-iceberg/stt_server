import pymssql
import pandas as pd
import redis
import time
import datetime
import difflib

REDIS_IP = '10.2.5.212'
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


def summarize(text, phrases_count):
    print(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'summarizing')
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


def commit(df):
    insert = ''
    delete = ''
    for idx, row in df.iterrows():    
        
        insert += "insert into summarization"
        insert += "(linkedid, record_date, sum_date, side, text, phrases_count, text_length, source_id) "
        insert += " values("
        insert += "'"+str(row.linkedid)+"',"
        insert += "'"+str(row.record_date)+"',"
        insert += "'"+str(row.sum_date)+"',"
        insert += ('1' if str(row.side) == 'True' else '0')+","
        insert += "'"+str(row.text)+"',"
        insert += "'"+str(row.phrases_count)+"',"
        insert += "'"+str(row.text_length)+"',"
        insert += str(row.source_id)
        insert += ");"

        delete += "delete from summarization_queue where"
        delete += " linkedid='"+str(row.linkedid)+"' and"
        delete += " side="+('1' if str(row.side) == 'True' else '0')+";"

    conn = ms_sql_con()  
    cursor = conn.cursor()
    try:
        cursor.execute(insert+delete)
    except Exception as e:
        print(e)
        print(insert)
        print(delete)


def get_jaccard_sim(str1, str2): 
    a = set(str1.split()) 
    b = set(str2.split())
    c = a.intersection(b)
    denominator = (len(a) + len(b) - len(c))
    if denominator > 0:
        return float(len(c) / denominator)
    else:
        return 0


def summarize_by_row(row):
    return summarize(row.text, row.phrases_count)


def jaccard_sim_by_row(row, wrong_words):
    for wrong in wrong_words:
        if wrong in row.text_short:
            print('jaccard_sim_by_row ratet as 0:', wrong)
            return 0
    return get_jaccard_sim(row.text, row.text_short)


def replace_wrong_by_row(row, wrong_words):
    for wrong in wrong_words:
        if wrong in row.text_short:
            print('replace_wrong_by_row replaced:', wrong)
            return row.text[:MAX_TEXT_SIZE]
    return row.text_short


print('=== start ===')

query = "SELECT top 300"
query += " linkedid, record_date, side, phrases_count, text_length, text, version, source_id, "
query += " '' as text_short, 0 as jaccard_sim"
query += " from summarization_queue"
query += " order by record_date, linkedid, side, version;"
df = read_sql(query)

# summarize
df.text_short = df.apply(summarize_by_row, axis=1)

print(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'preparing')
# evaluate error
wrong_words = ['погиб', 'смерть', 'путин'] # high frequency and not relevant newspaper words
df.jaccard_sim = df.apply(jaccard_sim_by_row, axis=1, wrong_words = wrong_words)
jsims = pd.DataFrame(df.groupby(by=['linkedid','side']).max().jaccard_sim)
jsims.reset_index(inplace = True)

# drop wroworst results
df = pd.merge(df, jsims, how = 'inner', on = ['linkedid','side', 'jaccard_sim'])

# group the same results
jfirst = pd.DataFrame(df.groupby(by=['linkedid','side']).min().version)
jfirst.reset_index(inplace = True)
df = pd.merge(df, jfirst, how = 'inner', on = ['linkedid','side', 'version'])

# replace wrong words
df.text_short = df.apply(replace_wrong_by_row, axis=1, wrong_words = wrong_words)

print(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'saving')

# save
current_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
df['sum_date'] = [current_date for i in range(len(df))]
df.drop(['jaccard_sim', 'text', 'version'], axis = 1, inplace = True)
df.rename(columns={'text_short': 'text'}, inplace=True)
commit(df)

print(
    datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
    'job complete:', len(df),
    'from:', min(df.record_date),
    'to:', max(df.record_date)
    )
