# kafka-streams-cassandra-state-store/examples 
## word-count-cassandra4

### run example

Note: Run from the directory of this README.md

1. Terminal 1: Start docker-compose stack
```bash
docker-compose up -d
```

2. Terminal 1: Start the example app
```bash
../../gradlew run
```

3. Terminal 2: Start a console-consumer
```bash
kcat -b localhost:19092 -t streams-wordcount-output -K:: -s key=s -s value=q
```

5. Terminal 3: Produce some messages via kcat
```bash
echo "Hello world" | kcat -b localhost:19092 -t streams-plaintext-input
echo "What a wonderful world" | kcat -b localhost:19092 -t streams-plaintext-input
echo "What a day to say hello" | kcat -b localhost:19092 -t streams-plaintext-input
```

#### (Cleanup)

Remove docker-compose stack (run from the directory of this README.md)
```bash
docker-compose down
```
