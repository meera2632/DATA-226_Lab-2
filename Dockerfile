FROM apache/airflow:2.10.1-python3.12

COPY requirements.txt .

RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt --config-settings=resolver.max_rounds=500000# Use Airflow 2.10.1 with Python 3.12
FROM apache/airflow:2.10.1-python3.12

# Switch to root to install packages
USER root

# Copy requirements into the container
COPY requirements.txt /requirements.txt

# Upgrade pip
RUN pip install --upgrade pip

# Install required Python libraries
RUN pip install --no-cache-dir -r /requirements.txt --config-settings=resolver.max_rounds=500000

# Return to airflow user
USER airflow
