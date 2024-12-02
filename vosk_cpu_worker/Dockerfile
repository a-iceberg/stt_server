FROM python:3.11-slim
WORKDIR /worker

COPY requirements.txt /worker
RUN mkdir /worker/files

RUN apt-get update && apt-get install ffmpeg -y && rm -rf /var/lib/apt/lists/*
RUN pip3 install -r requirements.txt

ENV TZ=Europe/Moscow
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

COPY vosk_cpu_worker.py /worker
COPY init_server.py /worker

ENTRYPOINT ["python3"]
CMD ["vosk_cpu_worker.py"]
