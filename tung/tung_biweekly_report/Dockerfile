FROM python:3.9.4-slim
#RUN apk add python3 py3-pip
WORKDIR /report

COPY requirements.txt /report

RUN pip3 install -r requirements.txt \
 && pip check

COPY ./* /report

CMD [ "python3",  "main.py"]
