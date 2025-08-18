# Use official Airflow image as base
FROM apache/airflow:3.0.3-python3.12

# Switch to airflow user
USER airflow

# Copy your requirements.txt into the container
COPY requirements.txt .

# Install Python packages listed in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

