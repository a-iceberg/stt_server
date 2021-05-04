FROM python:3.8.9

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY server.py .
COPY mysql_local.pass .
COPY sql.pass .
COPY telegram_token.key .

CMD [ "python", "./server.py" ]
