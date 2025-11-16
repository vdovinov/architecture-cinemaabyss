FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Copy application code
COPY . /app

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Expose service port
EXPOSE 8000

# Configurable module and bind settings
ENV APP_MODULE=main:app
ENV HOST=0.0.0.0
ENV PORT=8000

# Start the server
CMD ["sh", "-c", "uvicorn ${APP_MODULE} --host ${HOST} --port ${PORT}"]