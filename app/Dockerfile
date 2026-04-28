# Используем базовый образ Python
FROM python:3.10-slim-bullseye

# Устанавливаем Java (нужна для Spark) и утилиты
RUN apt-get update && \
    apt-get install -y openjdk-11-jre-headless curl tar && \
    apt-get clean

# --- УСТАНОВКА SPARK ---
ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/usr/local/spark

# Скачиваем и распаковываем Spark
RUN curl -O https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar -zxvf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME} && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

# --- НАСТРОЙКА ОКРУЖЕНИЯ ---
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PYSPARK_PYTHON=python3

# Интеграция Spark в Python (чтобы не качать pyspark через pip)
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH
ENV PATH=$SPARK_HOME/bin:$PATH

# Устанавливаем зависимости
# Используем ./ для явного указания текущей директории (исправляет ошибку линтера)
COPY requirements.txt ./
RUN pip install --no-cache-dir --default-timeout=1000 -r requirements.txt
# Скачиваем лексикон VADER
RUN python -m nltk.downloader vader_lexicon

# Копируем код приложения
WORKDIR /app
COPY . ./