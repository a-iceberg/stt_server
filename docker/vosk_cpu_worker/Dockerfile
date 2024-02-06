FROM python:latest
WORKDIR /worker
COPY requirements.txt /worker
RUN pip3 install -r requirements.txt --no-cache-dir
RUN mkdir /worker/files
RUN bash -c 'apt-get install -y software-properties-common;add-apt-repository ppa:mc3man/trusty-media -y;apt-get update -y;apt-get install ffmpeg -y'
#RUN bash -c 'apt-get install software-properties-common;'
#RUN add-apt-repository ppa:mc3man/trusty-media -y
#RUN apt-get update
#RUN apt-get install ffmpeg -y
COPY vosk_cpu_worker.py /worker
COPY init_server.py /worker
ENTRYPOINT ["python3"]
CMD ["vosk_cpu_worker.py"]