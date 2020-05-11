FROM python:3.6-slim

WORKDIR /root/

COPY . .

RUN pip install -r requirements.txt

ENTRYPOINT ["/bin/bash"]