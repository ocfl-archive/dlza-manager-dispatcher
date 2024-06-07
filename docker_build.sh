#!/bin/bash
docker build --rm -t dlza-manager-dispatcher:latest .
docker tag dlza-manager-dispatcher:latest registry.localhost:5001/dlza-manager-dispatcher
docker push registry.localhost:5001/dlza-manager-dispatcher
