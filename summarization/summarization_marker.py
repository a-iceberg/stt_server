import asyncio
from aiohttp import web
import uuid
import pandas as pd
from os import unlink
#import ssl
#PORT = '8443' # ssl
PORT = '8085'

# Quick'n'dirty SSL certificate generation:
#
# openssl genrsa -out webhook_pkey.pem 2048
# openssl req -new -x509 -days 3650 -key webhook_pkey.pem -out webhook_cert.pem
#
# When asked for "Common Name (e.g. server FQDN or YOUR name)" you should reply
# with the same value in you put in WEBHOOK_HOST with www

async def call_test(request):
    print('call_test')
    return web.Response(
        text='ok',
        content_type="text/html")

async def call_mark(request):
    print('call_mark')
    # save data
    filename = str(uuid.uuid4())+'.csv'
    with open(filename, 'w') as source_file:
        source_file.write(await request.text())
        source_file.close()
    df = pd.read_csv(filename, ';', dtype={'linkedid': 'str'})
    unlink(filename)
    #df.id += 10
    #df.name = 'hello from python: '+df.name
    answer = 'list:'
    for nom_id, nom_name in df.values:
        answer += '\n' + str(nom_id) + ';'+nom_name

    return web.Response(
        text=answer,
        content_type="text/html")

## Build ssl context
#ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
#ssl_context.load_cert_chain(cert_pub, cert_key)

app = web.Application()
app.router.add_route('GET', '/test', call_test)
app.router.add_route('POST', '/mark', call_mark)

loop = asyncio.get_event_loop()
handler = app.make_handler()
#f = loop.create_server(handler, port=PORT, ssl=ssl_context)
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
