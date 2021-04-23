# 1C table -> http post -> mysql
import asyncio
from aiohttp import web
import uuid
import pandas as pd
from os import unlink
import pymysql
PORT = '8083'

async def call_test(request):
    print('call_test')
    return web.Response(
        text='ok',
        content_type="text/html")

async def call_log(request):

    print('call_compute')
    # data -> df
    filename = str(uuid.uuid4())+'.csv'
    with open(filename, 'w') as source_file:
        source_file.write(await request.text())
        source_file.close()
    df = pd.read_csv(filename, ';', dtype={'id': 'int', 'name': 'str'})
    unlink(filename)

    # df -> mysql
    with open('mysql_local.pass', 'r') as file:
        mysql_pass = file.read().replace('\n', '')
        file.close()
    conn = pymysql.connect(
        host='10.2.4.87',
        user='root',
        passwd=mysql_pass,
        db='1c',
        autocommit=True
    )
    cursor = conn.cursor()
    query = "show tables;"
    cursor.execute(query)
    for row in cursor.fetchall():
        print(row)

    answer = 'list:'
    #for nom_id, nom_name in df.values:
    #    answer += '\n' + str(nom_id) + ';'+nom_name

    return web.Response(
        text=answer,
        content_type="text/html")

app = web.Application()
app.router.add_route('GET', '/test', call_test)
app.router.add_route('POST', '/log', call_log)

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
