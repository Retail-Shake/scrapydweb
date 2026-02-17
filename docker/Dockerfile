FROM python:3.9-slim-bullseye

WORKDIR /app

RUN apt-get update && apt-get install -y git gcc && \
  pip3 install scrapydweb

CMD ["scrapydweb"]
