FROM python:3.8-buster

COPY ./requirements.txt /home/requirements.txt
COPY ./tweets_producer.py /home/tweets_producer.py

RUN pip install -r /home/requirements.txt

ENTRYPOINT ["python", "/home/tweets_producer.py"]