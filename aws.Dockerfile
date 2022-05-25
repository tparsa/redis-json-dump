FROM python:3.9.2-alpine

WORKDIR app

COPY requirements.txt .
RUN pip install -r requirements.txt

ADD "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" "awscliv2.zip"
RUN unzip awscliv2.zip
RUN ./aws/install

COPY . .
