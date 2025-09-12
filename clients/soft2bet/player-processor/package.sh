#!/usr/bin/env bash

set -o errexit
set -o pipefail


mvn clean package -DskipTests -f pom.xml -Dquarkus.profile=missingdata 
mvn clean package -DskipTests -f pom.xml -Dquarkus.profile=fdl
mvn clean package -DskipTests -f pom.xml -Dquarkus.profile=funid
mvn clean package -DskipTests -f pom.xml -Dquarkus.profile=kpi
mvn clean package -DskipTests -f pom.xml -Dquarkus.profile=repartitioner
mvn clean package -DskipTests -f pom.xml -Dquarkus.profile=segmentation
mvn clean package -DskipTests -f pom.xml -Dquarkus.profile=sportpush
mvn clean package -DskipTests -f pom.xml -Dquarkus.profile=login
mvn clean package -DskipTests -f pom.xml
