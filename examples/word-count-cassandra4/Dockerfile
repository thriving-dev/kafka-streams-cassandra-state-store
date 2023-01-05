FROM amazoncorretto:17

WORKDIR /app

ADD build/libs/word-count-cassandra4.jar /app/app.jar

CMD java -noverify -jar /app/app.jar
