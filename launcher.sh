#!/bin/sh

echo "Launching scapydweb"

uwsgi --py-executable /usr/local/bin/python3 \
      --http 0.0.0.0:5000 \
      --enable-threads \
      --chdir /app/scrapyd_server \
      --module "scrapydweb.run:main()" 

