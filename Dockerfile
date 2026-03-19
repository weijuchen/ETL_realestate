

# second version version conflict(java17 vs spark 3.x)
# FROM quay.io/astronomer/astro-runtime:12.6.0

# USER root

# # 改為安裝 OpenJDK-11
# RUN apt-get update && \
#     apt-get install -y openjdk-11-jdk ant && \
#     apt-get clean;

# # 修改 JAVA_HOME 路徑 (注意路徑會變成 java-11)
# ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
# RUN export JAVA_HOME

# USER astro



FROM quay.io/astronomer/astro-runtime:12.6.0


USER root





# Install OpenJDK-17
RUN apt update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get install -y ant && \
    apt-get clean;

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64/
RUN export JAVA_HOME