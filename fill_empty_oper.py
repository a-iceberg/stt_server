import pymssql as ms_sql
import pymysql as my_sql
import datetime
import progressbar


def connect_mysql():
    my_sql_base = 'MICO_96'
    my_sql_server = '10.2.4.146'
    my_sql_login = 'asterisk'

    with open('mysql.pass', 'r') as file:
        my_sql_pass = file.read().replace('\n', '')
        file.close()

    return my_sql.connect(my_sql_server,
                          my_sql_login,
                          my_sql_pass,
                          my_sql_base)


def connect_mssql():
    ms_sql_base = 'voice_ai'
    ms_sql_server = '10.2.4.124'
    ms_sql_login = 'ICECORP\\1c_sql'

    with open('sql.pass', 'r') as file:
        ms_sql_pass = file.read().replace('\n', '')
        file.close()

    return ms_sql.connect(ms_sql_server,
                          ms_sql_login,
                          ms_sql_pass,
                          ms_sql_base)


def read_dst(file_name, date_y, date_m, date_d):
    #query = "select SUBSTRING(dstchannel, 5, 4) from PT1C_cdr_MICO limit 10"
    my_sql_conn = connect_mysql()
    # read linkedid from CDR
    linkedid = ''
    filename = file_name.replace('rxtx.wav', '')
    idy, idm, idd = int(date_y), int(date_m), int(date_d)
    date_from = datetime.datetime(idy, idm, idd)
    date_toto = date_from+datetime.timedelta(days=1)
    sdf = str(date_from)
    f_0 = '%Y-%m-%d %H:%M:%S'
    f_1 = '%Y-%m-%dT%H:%M:%S'
    date_from = datetime.datetime.strptime(sdf, f_0).strftime(f_1)
    date_toto = datetime.datetime.strptime(str(date_toto), f_0).strftime(f_1)
    with my_sql_conn:
        query = """select
            SUBSTRING(dstchannel, 5, 4) as dst
            from PT1C_cdr_MICO as PT1C_cdr_MICO
            where
            calldate>'"""+date_from+"""' and
            calldate<'"""+date_toto+"""' and
            PT1C_cdr_MICO.recordingfile LIKE '%"""+filename+"""%'
            limit 1"""
        cursor = my_sql_conn.cursor()
        cursor.execute(query)
        for row in cursor.fetchall():
            dst = row[0]

    if dst == '':
        print('linkedid for '+filename+' is empty. exit')
        exit()

    return dst


def read_job_len():
    ms_sql_conn = connect_mssql()
    # read job length
    job_len = 0
    with ms_sql_conn:
        query = """select
        count(distinct audio_file_name)
        from transcribations
        where audio_file_name!='' and
        text!='' and
        dst is null;"""
        cursor = ms_sql_conn.cursor()
        cursor.execute(query)
        for row in cursor.fetchall():
            job_len = row[0]

    if job_len == 0:
        print('nothing to do. exit')
        exit()

    return job_len


def update_transcribations(file_name, dst):
    ms_sql_conn = connect_mssql()
    with ms_sql_conn:
        cursor = ms_sql_conn.cursor()
        query = """update transcribations
        set dst = '"""+dst+"""'
        where audio_file_name='"""+file_name+"';"
        print(query)
        #cursor.execute(query)
        #ms_sql_conn.commit()


def main():
    job_len = read_job_len()

    bar = progressbar.ProgressBar(maxval=job_len).start()
    step = 0
    # while(True):
    # file_name, date_y, date_m, date_d = read_file_name()
    ms_sql_conn = connect_mssql()
    file_name = ''
    # read filename from transcribations
    with ms_sql_conn:
        query = """select
        audio_file_name,
        date_y,
        date_m,
        date_d
        from transcribations
        where text!='' and
        dst is null and
        audio_file_name!=''
        order by
        date_y,
        date_m,
        date_d,
        transcribation_date;"""
        cursor = ms_sql_conn.cursor()
        cursor.execute(query)
        for row in cursor.fetchall():
            bar.update(step)
            file_name = row[0]
            date_y = row[1]
            date_m = row[2]
            date_d = row[3]
            dst = read_dst(file_name, date_y, date_m, date_d)
            update_transcribations(file_name, dst)
            step += 1
            break

if __name__ == '__main__':
    main()
