import pymssql
import pymysql
import pandas as pd
import numpy as np
import datetime
import matplotlib.pyplot as plt
import telebot
import requests
import urllib


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
    plt.savefig('/home/alex/projects/call_centre_stt_server/monitoring/report.png')

    with open('/home/alex/projects/call_centre_stt_server/monitoring/telegram_token.key', 'r') as file:
        token = file.read().replace('\n', '')
        file.close()
    t_bot = telebot.TeleBot(token)
    with open('/home/alex/projects/call_centre_stt_server/monitoring/report.png', 'rb') as data_file:
        # bot.send_photo(group, data_file, caption="queue_time_vs_date")
        print('sending photo to ', tg_group)
        t_bot.send_photo(chat_id = tg_group, photo = data_file)


group = '-1001443983697'  # telegram group id


def send_to_telegram(chat_id, message):
    with open('/home/alex/projects/call_centre_stt_server/monitoring/telegram_token.key', 'r') as file:
        token = file.read().replace('\n', '')
        file.close()
    session = requests.Session()
    session.get(
        'https://api.telegram.org/bot' + token + '/sendMessage?chat_id=' + chat_id + '&text=' + urllib.parse.quote_plus(
            message))


# calls
with open('/home/alex/projects/call_centre_stt_server/monitoring/mysql_local.pass','r') as file:
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
with open('/home/alex/projects/call_centre_stt_server/monitoring/sql.pass','r') as file:
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

# calls
query = "SELECT date(call_date) as day, date(call_date) as call_date, ak, miko, mrm, incoming, not incoming as outcoming, linkedid, base_name from calls;"
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

report = 'Связь звонков и расшифровок за вчера:'

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
