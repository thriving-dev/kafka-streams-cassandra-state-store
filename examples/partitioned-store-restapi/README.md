# kafka-streams-cassandra-state-store/examples 
## partitioned-store-restapi

Read more details on the `CassandraPartitionedReadOnlyKeyValueStore` class, that _implements_ `ReadOnlyKeyValueStore<K, V>` in the blog post.
https://thriving.dev/blog/interactive-queries-with-kafka-streams-cassandra-state-store-part-2

The example also is featured in a YouTube demo. The link can be found in the blog post or on the https://www.youtube.com/@thriving_dev YouTube Channel. 

### run example

Note: Run from the directory of this README.md

1. Terminal 1: Start docker-compose stack
```bash
docker-compose up -d
```

2. Terminal 2: Produce some messages via kcat to the input topic
```bash
echo "AT::Austria" | kcat -P -b localhost:19092 -t streams-plaintext-input -K::
echo "BE::Belgium" | kcat -P -b localhost:19092 -t streams-plaintext-input -K::
echo "BG::Bulgaria" | kcat -P -b localhost:19092 -t streams-plaintext-input -K::
echo "HR::Croatia" | kcat -P -b localhost:19092 -t streams-plaintext-input -K::
echo "CY::Cyprus" | kcat -P -b localhost:19092 -t streams-plaintext-input -K::
echo "CZ::Czech Republic" | kcat -P -b localhost:19092 -t streams-plaintext-input -K::
echo "DK::Denmark" | kcat -P -b localhost:19092 -t streams-plaintext-input -K::
echo "EE::Estonia" | kcat -P -b localhost:19092 -t streams-plaintext-input -K::
echo "FI::Finland" | kcat -P -b localhost:19092 -t streams-plaintext-input -K::
echo "FR::France" | kcat -P -b localhost:19092 -t streams-plaintext-input -K::
echo "DE::Germany" | kcat -P -b localhost:19092 -t streams-plaintext-input -K::
echo "GR::Greece" | kcat -P -b localhost:19092 -t streams-plaintext-input -K::
echo "HU::Hungary" | kcat -P -b localhost:19092 -t streams-plaintext-input -K::
echo "IE::Ireland" | kcat -P -b localhost:19092 -t streams-plaintext-input -K::
echo "IT::Italy" | kcat -P -b localhost:19092 -t streams-plaintext-input -K::
echo "LV::Latvia" | kcat -P -b localhost:19092 -t streams-plaintext-input -K::
echo "LT::Lithuania" | kcat -P -b localhost:19092 -t streams-plaintext-input -K::
echo "LU::Luxembourg" | kcat -P -b localhost:19092 -t streams-plaintext-input -K::
echo "MT::Malta" | kcat -P -b localhost:19092 -t streams-plaintext-input -K::
echo "NL::Netherlands" | kcat -P -b localhost:19092 -t streams-plaintext-input -K::
echo "PL::Poland" | kcat -P -b localhost:19092 -t streams-plaintext-input -K::
echo "PT::Portugal" | kcat -P -b localhost:19092 -t streams-plaintext-input -K::
echo "RO::Romania" | kcat -P -b localhost:19092 -t streams-plaintext-input -K::
echo "SK::Slovakia" | kcat -P -b localhost:19092 -t streams-plaintext-input -K::
echo "SI::Slovenia" | kcat -P -b localhost:19092 -t streams-plaintext-input -K::
echo "ES::Spain" | kcat -P -b localhost:19092 -t streams-plaintext-input -K::
echo "SE::Sweden" | kcat -P -b localhost:19092 -t streams-plaintext-input -K::
```

3. Terminal 3: Start the example app
```bash
../../gradlew run
```

4. Terminal 4: query REST API using all endpoints
```bash
curl localhost:8080/keyvalue/IT -vvv
http localhost:8080/keyvalue/IT
http localhost:8080/all
http localhost:8080/reverseAll
http localhost:8080/range/R/Z
http localhost:8080/reverseRange/R/Z
http localhost:8080/prefixScan/L
http localhost:8080/approximateNumEntries
```

#### (Cleanup)

Remove docker-compose stack (run from the directory of this README.md)
```bash
docker-compose down
```
