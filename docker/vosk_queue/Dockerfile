FROM python:latest
WORKDIR /app
COPY requirements.txt /app
RUN pip3 install -r requirements.txt --no-cache-dir
COPY queue_server.py /app
COPY init_queue.py /app
ENTRYPOINT ["python3"]
CMD ["queue_server.py"]
