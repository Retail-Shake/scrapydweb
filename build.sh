#!/bin/sh

# construit une image docker de l'app data_integration


docker build -t retail_shake/scrapydweb:dev -f docker/Dockerfile .