# Use an official Apache Airflow base image
FROM apache/airflow:2.10.2

USER root

# Install necessary packages for running Chromium and ChromeDriver
RUN apt-get update && apt-get install -y \
    wget \
    unzip \
    libglib2.0-0 \
    libnss3 \
    libgconf-2-4 \
    libfontconfig1 \
    libxrender1 \
    libx11-xcb1 \
    libxcb1 \
    libxcomposite1 \
    libxcursor1 \
    libxdamage1 \
    libxi6 \
    libxtst6 \
    libatk-bridge2.0-0 \
    libxrandr2 \
    libasound2 \
    libpangocairo-1.0-0 \
    libpango-1.0-0 \
    libcups2 \
    libxss1 \
    libxext6 \
    libx11-6 \
    chromium

# Install ChromeDriver from Huawei mirror to match Chromium 130
RUN wget https://mirrors.huaweicloud.com/chromedriver/130.0.6723.58/chromedriver-linux64.zip && \
    unzip chromedriver-linux64.zip && \
    mv chromedriver-linux64/chromedriver /usr/local/bin/chromedriver && \
    chmod +x /usr/local/bin/chromedriver

# Clean up unnecessary files
RUN rm chromedriver-linux64.zip

# Set up environment variables for Chrome
ENV CHROME_BIN=/usr/bin/chromium-browser
ENV CHROMEDRIVER_PATH=/usr/local/bin/chromedriver

USER airflow