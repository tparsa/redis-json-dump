FROM python:3.9.2-alpine

WORKDIR app

COPY requirements.txt .
RUN pip install -r requirements.txt

RUN pip install --no-cache-dir awscli

COPY . .

ENTRYPOINT ["python3", "src/s3_dumper.py"]
