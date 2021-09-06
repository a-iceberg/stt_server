FROM python:latest
WORKDIR /queue
COPY requirements.txt /queue
RUN pip3 install -r requirements.txt --no-cache-dir
COPY queue.py /queue
COPY init_queue.py /queue
ENTRYPOINT ["python3"]
CMD ["queue.py"]
