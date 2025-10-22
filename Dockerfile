# Use Python slim (arm64 compatible)
FROM python:3.11-slim-bullseye as builder

# Create a non-root user
RUN useradd --create-home --shell /bin/bash appuser
WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential gcc g++ curl gnupg2 apt-transport-https \
    && rm -rf /var/lib/apt/lists/*

# Install Microsoft ODBC Driver 18 for SQL Server (Debian 11)
RUN curl -sSL -O https://packages.microsoft.com/config/debian/11/packages-microsoft-prod.deb \
    && dpkg -i packages-microsoft-prod.deb \
    && rm packages-microsoft-prod.deb \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18 unixodbc-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Ensure logs directory exists and writable
RUN mkdir -p logs && chown -R appuser:appuser logs

# Switch to non-root user
USER appuser

# Run bot
CMD ["python", "bot.py"]










