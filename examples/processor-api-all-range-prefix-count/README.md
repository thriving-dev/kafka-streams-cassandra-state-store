# kafka-streams-cassandra-state-store/examples 
## processor-api-all-range-prefix-count

Demonstrates, using the low-level Processor API, how to implement the WordCount program that computes a simple word occurrence histogram from an input text.

In addition, following `ReadOnlyKeyValueStore` methods are used (via _scheduled Punctuator_):
- `ReadOnlyKeyValueStore#all()`
- `ReadOnlyKeyValueStore#range(Object, Object)`
- `ReadOnlyKeyValueStore#prefixScan(Object, Serializer)`
- `ReadOnlyKeyValueStore#approximateNumEntries()`

In this example, the input stream reads from a topic named "streams-plaintext-input", where the values of messages represent lines of text; and the histogram output is written to topic "streams-wordcount-output" where each record is an updated count of a single word.

### run example

Note: Run from the directory of this README.md

1. Terminal 1: Start docker-compose stack
```bash
docker-compose up -d
```

2. Terminal 2: Produce some messages via kcat to the input topic
```bash
echo "Albania  Andorra  Armenia  Austria  Azerbaijan  Belarus  Belgium  BosniaAndHerzegovina  Bulgaria  Croatia  Cyprus  Czechia  Denmark  Estonia  Finland  France  Georgia  Germany  Greece  Hungary  Iceland  Ireland  Italy  Kazakhstan  Kosovo  Latvia  Liechtenstein  Lithuania  Luxembourg  Malta  Moldova  Monaco  Montenegro  Netherlands  NorthMacedonia  Norway  Poland  Portugal  Romania  Russia  SanMarino  Serbia  Slovakia  Slovenia  Spain  Sweden  Switzerland  Turkey  Ukraine  UnitedKingdom  VaticanCity" | kcat -P -b localhost:19092 -t streams-plaintext-input
```

3. Terminal 3: Start the example app
```bash
../../gradlew run
```
-> !!! check the application logs emitted periodically from scheduled Punctuators... !!!

4. Terminal 4: Start a console-consumer on the output topic
```bash
kcat -C -q -b localhost:19092 -t streams-wordcount-output -K:: -s key=s -s value=q
```

#### (Cleanup)

Remove docker-compose stack (run from the directory of this README.md)
```bash
docker-compose down
```
