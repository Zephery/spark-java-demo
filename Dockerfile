FROM gcr.io/spark-operator/spark:v3.1.1

COPY spark-es-demo/target/*.jar /opt/spark/examples/jars/app.jar

ENV TZ='Asia/Shanghai'

CMD ["java","-jar","/opt/spark/examples/jars/app.jar"]