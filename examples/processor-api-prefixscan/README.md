# kafka-streams-cassandra-state-store/examples 
## processor-api-prefixscan

### run example

Note: Run from the directory of this README.md

1. Terminal 1: Start docker-compose stack
```bash
docker-compose up -d
```

2. Terminal 2: Produce some messages via kcat to the input topic
```bash
echo "Albania  Andorra  Armenia  Austria  Azerbaijan  Belarus  Belgium  BosniaAndHerzegovina  Bulgaria  Croatia  Cyprus  Czechia  Denmark  Estonia  Finland  France  Georgia  Germany  Greece  Hungary  Iceland  Ireland  Italy  Kazakhstan  Kosovo  Latvia  Liechtenstein  Lithuania  Luxembourg  Malta  Moldova  Monaco  Montenegro  Netherlands  NorthMacedonia  Norway  Poland  Portugal  Romania  Russia  SanMarino  Serbia  Slovakia  Slovenia  Spain  Sweden  Switzerland  Turkey  Ukraine  UnitedKingdom  VaticanCity" | kcat -b localhost:19092 -t streams-plaintext-input
```

3. Terminal 3: Start the example app
```bash
../../gradlew run
```

4. Terminal 4: Start a console-consumer on the output topic
```bash
kcat -b localhost:19092 -t streams-wordcount-output -K:: -s key=s -s value=q
```

#### (Cleanup)

Remove docker-compose stack (run from the directory of this README.md)
```bash
docker-compose down
```
