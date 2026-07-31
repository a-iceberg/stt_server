FROM python:3.12-slim

WORKDIR /cleaner

COPY requirements.txt /cleaner

RUN pip3 install -r requirements.txt

COPY cleaner.py /cleaner

ENTRYPOINT ["python3"]
CMD ["cleaner.py"]
