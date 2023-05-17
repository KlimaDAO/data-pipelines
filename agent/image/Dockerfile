FROM --platform=linux/amd64 python:3.10-slim AS base

# git is required to pull the flows repostitory
RUN apt-get update
RUN apt-get install -y git

# Prepare workspace
RUN mkdir -p /opt
WORKDIR /opt 
COPY ./requirements.txt .
COPY ./scripts/run_agent.sh .

# Install dependencies
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

