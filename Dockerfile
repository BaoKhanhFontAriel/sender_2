FROM maven AS build
WORKDIR /app
COPY . .
RUN mvn package

FROM tomcat:8.5-alpine
COPY --from=build /app/target/send6.war /usr/local/tomcat/webapps