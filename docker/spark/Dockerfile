# FROM public.ecr.aws/bitnami/spark:3.5

# USER root

# # 建立 spark 使用者，解決 UnixLoginModule null name 問題
# RUN useradd -m -s /bin/bash spark && \
#     mkdir -p /opt/bitnami/spark/.ivy2 && \
#     chown -R spark:spark /opt/bitnami/spark/.ivy2

# USER spark
# ENV HOME=/opt/bitnami/spark
# ENV HADOOP_USER_NAME=spark