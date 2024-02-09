FROM python:3.10.4

WORKDIR /cleaner

COPY requirements.txt /cleaner

RUN pip3 install -r requirements.txt --no-cache-dir

COPY cleaner.py /cleaner

ENTRYPOINT ["python3"]
CMD ["cleaner.py"]
