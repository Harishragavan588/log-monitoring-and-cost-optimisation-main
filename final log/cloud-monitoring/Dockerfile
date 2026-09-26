# Use a lightweight official Python image
FROM python:3.11-slim

# Set the working directory inside the container
WORKDIR /app

# Copy requirements and install dependencies first (optimizes caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

# Expose the port the app runs on
EXPOSE 5000

# Run the app using Gunicorn for production performance
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "app:app"]