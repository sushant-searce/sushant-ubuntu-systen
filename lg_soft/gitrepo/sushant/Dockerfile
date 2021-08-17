FROM python:3.7-slim-buster

RUN apt-get -y update
RUN apt-get -y upgrade

COPY ./service/requirements.txt .

RUN pip install -r ./requirements.txt

COPY ./service /workflowengineService

WORKDIR /workflowengineService

RUN pip install -r ./requirements.txt


RUN echo Starting python and starting the Flask service...

ENTRYPOINT ["python"]
CMD ["WorkflowEngine.py"]