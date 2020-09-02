FROM python:3.8-buster

COPY ./requirements.txt /home/requirements.txt
COPY ./binance_producer.py /home/binance_producer.py

RUN pip install -r /home/requirements.txt

ENTRYPOINT ["python", "/home/binance_producer.py"]