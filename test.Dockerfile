FROM python:3.9.2-alpine

WORKDIR app

COPY test-requirements.txt .
RUN pip install -r test-requirements.txt

RUN pip install --no-cache-dir awscli

COPY . .

ENTRYPOINT ["python3"]
