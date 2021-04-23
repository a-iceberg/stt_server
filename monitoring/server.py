# 1C table -> http post -> mysql
import asyncio
from aiohttp import web
import uuid
import pandas as pd
from os import unlink
import pymysql
from sqlalchemy import create_engine
from datetime import datetime
import re
PORT = '8083'


async def call_test(request):
    print('call_test')
    return web.Response(
        text='ok',
        content_type="text/html")


async def call_log(request):
    print('call_log')
    # data -> df
    filename = str(uuid.uuid4())+'.csv'
    with open(filename, 'w') as source_file:
        source_file.write(await request.text())
        source_file.close()
    dateparser = lambda x: datetime.strptime(x, "%d.%m.%Y %H:%M:%S")
    df = pd.read_csv(filename, ';', parse_dates=['call_date'], date_parser=dateparser)
    unlink(filename)

    def get_base_name(val):
        return re.findall(r'"(.*?)"', val)[1]
    df.base_name = df.base_name.apply(get_base_name)

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
