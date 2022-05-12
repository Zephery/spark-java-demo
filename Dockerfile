FROM openjdk:11

COPY ./target/*.jar /app.jar

ENV TZ='Asia/Shanghai'
ENV JAVA_OPTS=" -Xms2048m -Xmx2048m "
ENV ACTIVE="test"

EXPOSE ${SERVER_PORT}

WORKDIR /data/logs/backend-checker

ENTRYPOINT [ "sh", "-c", "java $JAVA_OPTS -Dspring.profiles.active=$ACTIVE -jar /app.jar "  ]