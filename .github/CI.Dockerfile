ARG PYTHON_VERSION="3.13"
FROM python:${PYTHON_VERSION}-bookworm AS base

# Setup dependencies for pyodbc
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

# Download the package to configure the Microsoft repo
RUN curl -sSL -O https://packages.microsoft.com/config/debian/$(grep VERSION_ID /etc/os-release | cut -d '"' -f 2 | cut -d '.' -f 1)/packages-microsoft-prod.deb && \
    dpkg -i packages-microsoft-prod.deb && \
    rm packages-microsoft-prod.deb

FROM base AS msodbc17

# install ODBC driver 17
ENV ACCEPT_EULA=Y
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      msodbcsql17 \
      mssql-tools && \
    apt-get autoremove -yqq --purge && \
    apt-get clean &&  \
    rm -rf /var/lib/apt/lists/*

# add sqlcmd to the path
ENV PATH="$PATH:/opt/mssql-tools/bin"

FROM base AS msodbc18

# install ODBC driver 18
ENV ACCEPT_EULA=Y
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      msodbcsql18 \
      mssql-tools18 && \
    apt-get autoremove -yqq --purge && \
    apt-get clean &&  \
    rm -rf /var/lib/apt/lists/*

# add sqlcmd to the path
ENV PATH="$PATH:/opt/mssql-tools18/bin"
