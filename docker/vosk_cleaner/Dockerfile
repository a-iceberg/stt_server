FROM python:latest
WORKDIR /cleaner
COPY requirements.txt /cleaner
RUN pip3 install -r requirements.txt --no-cache-dir
COPY cleaner.py /cleaner
ENTRYPOINT ["python3"]
CMD ["cleaner.py"]
