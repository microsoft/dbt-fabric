ARG PYTHON_VERSION="3.13"
FROM python:${PYTHON_VERSION}-bookworm AS base

# Install cURL
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl && \
    apt-get autoremove -yqq --purge && \
    apt-get clean &&  \
    rm -rf /var/lib/apt/lists/*

# install Azure CLI
RUN curl -sL https://aka.ms/InstallAzureCLIDeb | bash && \
    apt-get autoremove -yqq --purge && \
    apt-get clean &&  \
    rm -rf /var/lib/apt/lists/*

# Setup dependencies for mssql-python
ENV ACCEPT_EULA=Y
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      libltdl7 \
      libkrb5-3 \
      libgssapi-krb5-2 && \
    apt-get autoremove -yqq --purge && \
    apt-get clean &&  \
    rm -rf /var/lib/apt/lists/*
