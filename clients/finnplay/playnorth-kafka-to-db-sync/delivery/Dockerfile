FROM eclipse-temurin:17.0.1_12-jre

WORKDIR /app/
COPY target/playnorth-kafka-to-db-sync.jar /app/lib/playnorth-kafka-to-db-sync.jar
COPY delivery/start.sh /app/start.sh
RUN chmod +x /app/start.sh

EXPOSE 8080
EXPOSE 80

ENV JAVA_OPTS="-Xmx5076M -Dcom.sun.management.jmxremote.port=11942 -Dcom.sun.management.jmxremote.rmi.port=11942 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false"

ENTRYPOINT ["sh", "start.sh"]
