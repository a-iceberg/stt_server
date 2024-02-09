FROM python:3.10.4

WORKDIR /worker

COPY requirements.txt /worker

RUN mkdir /worker/files
RUN bash -c 'apt-get install -y software-properties-common;add-apt-repository ppa:mc3man/trusty-media -y;apt-get update -y;apt-get install ffmpeg -y'
RUN pip3 install -r requirements.txt --no-cache-dir

# Set the timezone.
ENV TZ=Europe/Moscow
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

COPY vosk_cpu_worker.py /worker
COPY init_server.py /worker

ENTRYPOINT ["python3"]
CMD ["vosk_cpu_worker.py"]
