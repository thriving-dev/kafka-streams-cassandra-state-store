# kafka-streams-cassandra-state-store/examples
## word-count-scylladb

### run example

Note: Run from the directory of this README.md

1. Terminal 1: Start docker-compose stack
```bash
docker-compose up -d
```

2. Terminal 2: Produce some messages via kcat to the input topic
```bash
echo "Hello world" | kcat -P -b localhost:19092 -t streams-plaintext-input
echo "What a wonderful world" | kcat -P -b localhost:19092 -t streams-plaintext-input
echo "What a day to say hello" | kcat -P -b localhost:19092 -t streams-plaintext-input
```

3. Terminal 3: Start the example app
```bash
../../gradlew run
```

4. Terminal 4: Start a console-consumer on the output topic
```bash
kcat -C -q -b localhost:19092 -t streams-wordcount-output -K:: -s key=s -s value=q
```

#### (Cleanup)

Remove docker-compose stack (run from the directory of this README.md)
```bash
docker-compose down
```
