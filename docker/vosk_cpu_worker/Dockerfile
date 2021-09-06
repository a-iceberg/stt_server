FROM python:latest
WORKDIR /worker
COPY requirements.txt /worker
RUN pip3 install -r requirements.txt --no-cache-dir
COPY vosk_cpu_worker.py /worker
COPY init_server.py /worker
RUN mkdir /worker/temp
ENTRYPOINT ["python3"]
CMD ["vosk_cpu_worker.py"]
