# kafka-streams-cassandra-state-store/examples 
## global-store-restapi

### run example

Note: Run from the directory of this README.md

1. Terminal 1: Start docker-compose stack
```bash
docker-compose up -d
```

2. Terminal 2: Produce some messages via kcat to the input topic
```bash
echo "AT::Austria" | kcat -b localhost:19092 -t streams-plaintext-input -K::
echo "BE::Belgium" | kcat -b localhost:19092 -t streams-plaintext-input -K::
echo "BG::Bulgaria" | kcat -b localhost:19092 -t streams-plaintext-input -K::
echo "HR::Croatia" | kcat -b localhost:19092 -t streams-plaintext-input -K::
echo "CY::Cyprus" | kcat -b localhost:19092 -t streams-plaintext-input -K::
echo "CZ::Czech Republic" | kcat -b localhost:19092 -t streams-plaintext-input -K::
echo "DK::Denmark" | kcat -b localhost:19092 -t streams-plaintext-input -K::
echo "EE::Estonia" | kcat -b localhost:19092 -t streams-plaintext-input -K::
echo "FI::Finland" | kcat -b localhost:19092 -t streams-plaintext-input -K::
echo "FR::France" | kcat -b localhost:19092 -t streams-plaintext-input -K::
echo "DE::Germany" | kcat -b localhost:19092 -t streams-plaintext-input -K::
echo "GR::Greece" | kcat -b localhost:19092 -t streams-plaintext-input -K::
echo "HU::Hungary" | kcat -b localhost:19092 -t streams-plaintext-input -K::
echo "IE::Ireland" | kcat -b localhost:19092 -t streams-plaintext-input -K::
echo "IT::Italy" | kcat -b localhost:19092 -t streams-plaintext-input -K::
echo "LV::Latvia" | kcat -b localhost:19092 -t streams-plaintext-input -K::
echo "LT::Lithuania" | kcat -b localhost:19092 -t streams-plaintext-input -K::
echo "LU::Luxembourg" | kcat -b localhost:19092 -t streams-plaintext-input -K::
echo "MT::Malta" | kcat -b localhost:19092 -t streams-plaintext-input -K::
echo "NL::Netherlands" | kcat -b localhost:19092 -t streams-plaintext-input -K::
echo "PL::Poland" | kcat -b localhost:19092 -t streams-plaintext-input -K::
echo "PT::Portugal" | kcat -b localhost:19092 -t streams-plaintext-input -K::
echo "RO::Romania" | kcat -b localhost:19092 -t streams-plaintext-input -K::
echo "SK::Slovakia" | kcat -b localhost:19092 -t streams-plaintext-input -K::
echo "SI::Slovenia" | kcat -b localhost:19092 -t streams-plaintext-input -K::
echo "ES::Spain" | kcat -b localhost:19092 -t streams-plaintext-input -K::
echo "SE::Sweden" | kcat -b localhost:19092 -t streams-plaintext-input -K::
```

3. Terminal 3: Start the example app
```bash
../../gradlew run
```

4. Terminal 4: query REST API
```bash
curl localhost:8080/keyvalue/IT -vvv
http localhost:8080/keyvalue/IT
```

#### (Cleanup)

Remove docker-compose stack (run from the directory of this README.md)
```bash
docker-compose down
```
