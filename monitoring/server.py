# 1C table -> http post -> mysql
import asyncio
import uuid
import pandas as pd
import pymysql
import re
import pymssql
import numpy as np
import datetime
import matplotlib.pyplot as plt
import telebot
import requests
import urllib
import seaborn as sns
from aiohttp import web
from sqlalchemy import create_engine
from os import unlink


PORT = '8083'


async def call_test(request):
    print('call_test')
    return web.Response(
        text='ok',
        content_type="text/html")


async def call_connections(request):

    #group = '106129214'
    group = '-1001443983697'  # telegram group id

    def plot_grouped(df, header, tg_group):

        df = df.drop(['base_name', '_merge', 'call_date'], axis=1)
        df = df.groupby(['day', 'ak', 'miko', 'mrm', 'incoming', 'outcoming']).count()
        # df.groupby('param')['group'].nunique()
        for i in range(6):
            df.reset_index(level=0, inplace=True)
        df.incoming *= df.linkedid
        df.outcoming *= df.linkedid
        df.mrm *= df.linkedid
        df.miko *= df.linkedid
        df.ak *= df.linkedid

        grp = []
        tmp = df[['day', 'incoming']].groupby('day').sum()
        tmp.reset_index(level=0, inplace=True)
        grp.append(tmp)
        tmp = df[['day', 'outcoming']].groupby('day').sum()
        tmp.reset_index(level=0, inplace=True)
        grp.append(tmp)
        tmp = df[['day', 'ak']].groupby('day').sum()
        tmp.reset_index(level=0, inplace=True)
        grp.append(tmp)
        tmp = df[['day', 'miko']].groupby('day').sum()
        tmp.reset_index(level=0, inplace=True)
        grp.append(tmp)
        tmp = df[['day', 'mrm']].groupby('day').sum()
        tmp.reset_index(level=0, inplace=True)
        grp.append(tmp)
        tmp = df[['day', 'linkedid']].groupby('day').sum()
        tmp.reset_index(level=0, inplace=True)
        grp.append(tmp)

        df = grp[0]
        for i in range(5):
            df = df.merge(grp[i + 1], on='day', how='left')

        calls_max = df.linkedid.max() * 3
        # Decide Colors
        mycolors = ['tab:blue', 'tab:orange', 'tab:green', 'tab:grey', 'red']

        # Draw Plot and Annotate
        fig, ax = plt.subplots(1, 1, figsize=(16, 9), dpi=80)
        columns = df.columns[1:6]
        labs = columns.values.tolist()

        # Prepare data
        x = df['day'].values.tolist()
        y0 = df[columns[0]].values.tolist()
        y1 = df[columns[1]].values.tolist()
        y2 = df[columns[2]].values.tolist()
        y3 = df[columns[3]].values.tolist()
        y4 = df[columns[4]].values.tolist()
        y = np.vstack([y0, y1, y2, y3, y4])

        # Plot for each column
        labs = columns.values.tolist()
        ax = plt.gca()
        ax.stackplot(x, y, labels=labs, colors=mycolors, alpha=0.8)

        # Decorations
        ax.set_title(header, fontsize=18)
        ax.set(ylim=[0, calls_max])
        ax.legend(fontsize=10, ncol=4)
        plt.xticks(x, rotation=60)
        plt.grid(alpha=0.5)

        # Lighten borders
        plt.gca().spines["top"].set_alpha(0)
        plt.gca().spines["bottom"].set_alpha(.3)
        plt.gca().spines["right"].set_alpha(0)
        plt.gca().spines["left"].set_alpha(.3)

        # plt.show()
        plt.savefig('report.png')

        with open('telegram_token.key', 'r') as file:
            token = file.read().replace('\n', '')
            file.close()
        bot = telebot.TeleBot(token)
        with open('report.png', 'rb') as data_file:
            # bot.send_photo(group, data_file, caption="queue_time_vs_date")
            print('sending photo to ', tg_group)
            bot.send_photo(tg_group, data_file)


    def plot_lag(header, columns, tg_group):
        mycolors = ['tab:blue', 'tab:orange', 'red']

        # Draw Plot and Annotate
        fig, ax = plt.subplots(1,1,figsize=(16, 9), dpi = 80)

        labs = columns.values.tolist()

        # Prepare data
        x  = df['record_hour'].values.tolist()
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

        # plt.show()
        plt.savefig('report.png')

        with open('telegram_token.key', 'r') as file:
            token = file.read().replace('\n', '')
            file.close()
        bot = telebot.TeleBot(token)
        with open('report.png', 'rb') as data_file:
            print('sending photo to ', tg_group)
            bot.send_photo(tg_group, data_file)


    def queue_tasks_report(source_id, header, tg_group):
        today = datetime.datetime.now().date()
        yesterday = today - datetime.timedelta(days=1)
        date_from = yesterday.strftime('%Y.%m.%d %H:%M:%S')
        date_toto = today.strftime('%Y.%m.%d %H:%M:%S')
        query = "SELECT distinct"
        query += " cast('"+date_from+"' as datetime) as start_date,"
        query += " linkedid as linkedid_count,"
        query += " queue_date"
        query += " FROM transcribations"
        query += " WHERE transcribation_date > '"+date_from+"'"
        query += " and transcribation_date < '"+date_toto+"'"
        query += " and source_id = "+str(source_id)
        query += " and not queue_date is Null;"
        df = pd.read_sql(query, con = trans_conn)
        df['time'] = (df.queue_date - df.start_date)
        df.time = df.time.apply(lambda x: round(x.seconds/60))
        df = df.drop(['start_date', 'queue_date'], axis=1)
        df = df.groupby(df.time).count()
        df.reset_index(level=0, inplace=True)
        start_time = datetime.datetime(yesterday.year, yesterday.month, yesterday.day, 0, 0, 0)
        (start_time + datetime.timedelta(minutes = 120)).time()
        df['queued'] = df.time.apply(lambda x: (start_time + datetime.timedelta(minutes = x)).time())
        fig, ax = plt.subplots(figsize=(16,10), dpi= 80)    
        sns.stripplot(df.queued, df.linkedid_count, jitter=0.25, size=8, ax=ax, linewidth=.5)
        plt.gca().set_xticklabels(labels = df.queued, rotation=30)
        # Decorations
        plt.grid(linestyle='--', alpha=0.5)
        plt.title(header, fontsize=22)
        # plt.show()
        plt.savefig('report.png')

        with open('telegram_token.key', 'r') as file:
            token = file.read().replace('\n', '')
            file.close()
        bot = telebot.TeleBot(token)
        with open('report.png', 'rb') as data_file:
            print('sending photo to ', tg_group)
            bot.send_photo(tg_group, data_file)


    def send_to_telegram(chat_id, message):
        with open('telegram_token.key', 'r') as file:
            token = file.read().replace('\n', '')
            file.close()
        session = requests.Session()
        session.get(
            'https://api.telegram.org/bot' + token + '/sendMessage?chat_id=' + chat_id + '&text=' + urllib.parse.quote_plus(
                message))


    try:
        
        report =''
        # calls
        with open('mysql_local.pass','r') as file:
            calls_pass = file.read().replace('\n', '')
            file.close()
        calls_conn = pymysql.connect(
                    host = '10.2.4.87',
                    user = 'root',
                    passwd = calls_pass,
                    db = '1c',
                    autocommit = True
                )
        calls_cursor = calls_conn.cursor()

        # transcribations
        with open('sql.pass','r') as file:
            trans_pass = file.read().replace('\n', '')
            file.close()
        trans_conn = pymssql.connect(
                    server = '10.2.4.124',
                    user = 'ICECORP\\1c_sql',
                    password = trans_pass,
                    database = 'voice_ai',
                    #autocommit=True
                )
        trans_cursor = trans_conn.cursor()

        # = = = connections report = = =
        seven_days = datetime.datetime.now().date() - datetime.timedelta(days=7)
        date_from = seven_days.strftime('%Y:%m:%d %H:%M:%S')
        # calls
        query = "SELECT"
        query += " date(call_date) as day,"
        query += " date(call_date) as call_date,"
        query += " ak,"
        query += " miko,"
        query += " mrm,"
        query += " incoming,"
        query += " not incoming as outcoming,"
        query += " linkedid,"
        query += " base_name"
        query += " from calls"
        query += " where date(call_date)>='"+date_from+"';"
        calls = pd.read_sql(query, con=calls_conn)
        date_min = calls.call_date.min()
        date_max = calls.call_date.max()

        date_from = datetime.datetime.strptime(str(date_min), '%Y-%m-%d').strftime('%Y%m%d %H:%M:%S.000')
        date_toto = datetime.datetime.strptime(str(date_max), '%Y-%m-%d').strftime('%Y%m%d %H:%M:%S.000')

        # transcribations
        query = "SELECT distinct cast(record_date as date) as day, linkedid from transcribations"
        query += " where cast(record_date as date)>='" + date_from + "' and cast(record_date as date)<='" + date_toto + "';"
        trans = pd.read_sql(query, con=trans_conn)

        df_all = pd.merge(calls, trans, on='linkedid', how="outer", indicator=True)
        df_all['day'] = df_all.day_x

        # remove tech records ++
        df_all.base_name = df_all.base_name.str.lower()
        tech = pd.merge(
            df_all[df_all.base_name == '1c_service'],
            df_all[df_all.base_name == '1c_service_spb'],
            on='linkedid', how="inner"
        )
        tech = pd.merge(
            tech,
            df_all[df_all.base_name == '1c_service_region'],
            on='linkedid', how="inner"
        )
        df_all = pd.DataFrame(df_all[~df_all.base_name.isnull()])
        df_all = df_all[~df_all.linkedid.isin(tech.linkedid.unique())].sort_values('linkedid')
        # remove tech records --

        report += 'Связь звонков и расшифровок за вчера:'

        yesterday = datetime.datetime.now().date() - datetime.timedelta(days=1)
        report += '\nЗвонков: ' + str(len(calls[calls.day == yesterday].linkedid.unique()))
        report += '\nРасшифровок: ' + str(len(trans[trans.day == str(yesterday)].linkedid.unique()))
        mask = (df_all._merge == 'both') & (df_all.day == yesterday)
        report += '\nСвязь установлена: ' + str(len(df_all[mask].linkedid.unique()))
        mask = (df_all._merge == 'left_only') & (df_all.day == yesterday)
        report += '\nИдентификатор расшифровки не найден среди звонков: ' + str(len(df_all[mask].linkedid.unique()))

        send_to_telegram(group, report)

        df = df_all[df_all._merge == 'both']
        plot_grouped(df, 'Соединение установлено', group)

        df = df_all[df_all._merge == 'left_only']
        plot_grouped(df, 'Соединение не установлено', group)

        # = = = lag report = = =
        today = datetime.datetime.now().date()
        yesterday = today - datetime.timedelta(days=1)
        date_from = yesterday.strftime('%Y.%m.%d %H:%M:%S')
        date_toto = today.strftime('%Y.%m.%d %H:%M:%S')
        query = "SELECT distinct"
        query += " DATEPART(HOUR, record_date) as record_hour,"
        query += " DATEDIFF(second,record_date, queue_date) as rq,"
        query += " DATEDIFF(second,queue_date, transcribation_date) as qt,"
        query += " CASE WHEN source_id = 1 then 1 else 0 end as call,"
        query += " CASE WHEN source_id = 2 then 1 else 0 end as mrm"
        query += " FROM transcribations"
        query += " WHERE transcribation_date > '"+date_from+"'"
        query += " and transcribation_date < '"+date_toto+"'"
        query += " and not queue_date is Null;"
        df = pd.read_sql(query, con = trans_conn)
        df['кц от записи до постановки в очередь']=df.rq*df.call
        df['кц от постановки в очередь до расшифровки']=df.qt*df.call
        df['мрм от записи до постановки в очередь']=df.rq*df.mrm
        df['мрм от постановки в очередь до расшифровки']=df.qt*df.mrm
        df.drop(['rq','qt','call','mrm'], axis = 1, inplace = True)
        df = pd.DataFrame(df.groupby(['record_hour']).median()/60/60)
        df['record_hour'] = df.index
        plot_lag('Длительность расшифровки записей КЦ (ч.)', df.columns[0:2], group)
        plot_lag('Длительность расшифровки записей МРМ (ч.)', df.columns[2:4], group)
        
        # = = = queue tasks report = = =
        queue_tasks_report(1, 'Поступление в очередь КЦ (количество linkedid в минуту)', group)
        queue_tasks_report(2, 'Поступление в очередь МРМ (количество linkedid в минуту)', group)

    except Exception as e:
        report += ' Error: ' + str(e)
        send_to_telegram(group, 'call centre monitoring error: ' + str(e))

    return web.Response(
        text=report,
        content_type="text/html")


async def call_log(request):
    print('call_log')
    # data -> df
    filename = str(uuid.uuid4())+'.csv'
    with open(filename, 'w') as source_file:
        source_file.write(await request.text())
        source_file.close()
    dateparser = lambda x: datetime.datetime.strptime(x, "%d.%m.%Y %H:%M:%S")
    df = pd.read_csv(filename, ';', parse_dates=['call_date'], date_parser=dateparser)
    unlink(filename)

    def get_base_name(val):
        return re.findall(r'"(.*?)"', val)[1]
    df.base_name = df.base_name.apply(get_base_name)
    df.linkedid = df.linkedid.str.replace('.WAV', '')

    # df -> mysql
    with open('mysql_local.pass', 'r') as file:
        mysql_pass = file.read().replace('\n', '')
        file.close()
    engine = create_engine('mysql+pymysql://root:' + mysql_pass + '@10.2.4.87:3306/1c', echo=False)
    df.to_sql(name='calls', con=engine, index=False, if_exists='append')

    answer = 'inserted: '+str(len(df))
    return web.Response(
        text=answer,
        content_type="text/html")

app = web.Application(client_max_size=1024**4)
app.router.add_route('GET', '/test', call_test)
app.router.add_route('POST', '/log', call_log)
app.router.add_route('GET', '/connections', call_connections)

loop = asyncio.get_event_loop()
handler = app.make_handler()
f = loop.create_server(handler, port=PORT)
srv = loop.run_until_complete(f)
print('serving on', srv.sockets[0].getsockname())
try:
    loop.run_forever()
except KeyboardInterrupt:
    print("serving off...")
finally:
    loop.run_until_complete(handler.finish_connections(1.0))
    srv.close()
