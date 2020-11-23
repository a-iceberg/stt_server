import pymssql as ms_sql
import pymysql as my_sql

ms_sql_name = 'voice_ai'
ms_sql_server = '10.2.4.124'
ms_sql_login = 'ICECORP\\1c_sql'

my_sql_name = 'MICO_96'
my_sql_server = '10.2.4.146'
my_sql_login = 'asterisk'

with open(self.script_path+'sql.pass', 'r') as file:
    ms_sql_pass = file.read().replace('\n', '')
    file.close()

with open(self.script_path+'mysql.pass', 'r') as file:
    my_sql_pass = file.read().replace('\n', '')
    file.close()

ms_sql_conn = connect_sql()
my_sql_conn = connect_mysql()

file_name = ''
# read filename from transcribations
with ms_sql_conn:
    query = """select top 1
    audio_file_name,
    date_y,
    date_m,
    date_d
    from transcribations
    where text!='' and
    linkedid is null and
    audio_file_name!=''
    order by
    date_y,
    date_m,
    date_d,
    transcribation_date;"""
    cursor = ms_sql_conn.cursor()
    cursor.execute(query)
    for row in cursor.fetchall():
        file_name = row[0]
        date_y = row[1]
        date_m = row[2]
        date_d = row[3]

if file_name == '':
    print('empty filename. exit')
    exit()

# read linkedid from CDR
linkedid = ''
filename = file_name.replace('rxtx.wav', '')
idy, idm, idd = int(self.date_y), int(self.date_m), int(self.date_d)
date_from = datetime.datetime(idy, idm, idd)
date_toto = date_from+datetime.timedelta(days=1)
sdf = str(date_from)
f_0 = '%Y-%m-%d %H:%M:%S'
f_1 = '%Y-%m-%dT%H:%M:%S'
date_from = datetime.datetime.strptime(sdf, f_0).strftime(f_1)
date_toto = datetime.datetime.strptime(str(date_toto), f_0).strftime(f_1)
with my_sql_conn:
    cursor = my_sql_conn.cursor()
    query = """select
        linkedid
        from PT1C_cdr_MICO as PT1C_cdr_MICO
        where
        calldate>'"""+date_from+"""' and
        calldate<'"""+date_toto+"""' and
        PT1C_cdr_MICO.recordingfile LIKE '%"""+filename+"""%'
        limit 1"""

	cursor = self.mysql_conn.cursor()
    cursor.execute(query)
    for row in cursor.fetchall():
        linkedid = row[0]
if linkedid == '':
    print('linkedid for '+filename+' is empty. exit')
    exit()

# update transcribations
with my_sql_conn:
    cursor = my_sql_conn.cursor()
    query = """update transcribations
    set linkedid = """+linkedid+"""
    where audio_file_name="""+file_name+';'
    print(query)
    # cursor.execute(sql_query)
    # self.conn.commit()
