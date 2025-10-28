@echo off
mvn clean compile package -DskipTests -Dcheckstyle.skip=true -Dmaven.test.skip=true