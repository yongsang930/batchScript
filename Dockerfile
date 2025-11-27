FROM python:3.10-slim

WORKDIR /app

RUN apt-get update && \
    apt-get install -y cron tzdata && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

ENV TZ=Asia/Seoul
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . /app

# run_batch.sh — bash 강제, source 사용 가능
RUN echo '#!/bin/bash\ncd /app\nsource /etc/environment\n/usr/local/bin/python3 /app/batch.py' \
    > /app/run_batch.sh && chmod +x /app/run_batch.sh

# cron 스케줄 — bash로 명시
RUN echo '0 * * * * root bash -c "source /etc/environment && /app/run_batch.sh" >> /var/log/batch.log 2>&1' \
    > /etc/cron.d/rss-cron && chmod 0644 /etc/cron.d/rss-cron

# 로그파일
RUN touch /var/log/batch.log

# cron을 foreground로 실행해야 컨테이너가 살고 스케줄이 돈다.
CMD ["bash", "-c", "printenv > /etc/environment && cron -f"]
