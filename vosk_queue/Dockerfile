FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt /app

RUN pip3 install -r requirements.txt

COPY queue_server.py /app
COPY init_queue.py /app

ENTRYPOINT ["python3"]
CMD ["queue_server.py"]
