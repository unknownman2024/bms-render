# Base image
FROM python:3.10-slim

# Set workdir
WORKDIR /app

# Copy requirements
COPY requirements.txt .

# Install deps
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY . .

# Run script
CMD ["python", "main.py"]
