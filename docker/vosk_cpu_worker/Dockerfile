FROM python:latest
WORKDIR /worker
COPY requirements.txt /worker
RUN pip3 install -r requirements.txt --no-cache-dir
COPY vosk_cpu_worker.py /worker
RUN mkdir /mnt/share
RUN mkdir /mnt/share/audio_master
RUN mount -t nfs 10.2.4.254:/opt/Call_TO_1C/ /mnt/share/audio_master/
ENTRYPOINT ["python3"]
CMD ["vosk_cpu_worker.py"]
