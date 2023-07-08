plugins {
    java
    id("io.quarkus") version "3.2.0.Final"
}

repositories {
    mavenCentral()
}

val quarkusPlatformGroupId: String by project
val quarkusPlatformArtifactId: String by project
val quarkusPlatformVersion: String by project

dependencies {
    implementation(enforcedPlatform("$quarkusPlatformGroupId:quarkus-cassandra-bom:$quarkusPlatformVersion"))
    implementation(enforcedPlatform("$quarkusPlatformGroupId:$quarkusPlatformArtifactId:$quarkusPlatformVersion"))

    // Root lib
    implementation(project(":kafka-streams-cassandra-state-store"))

    implementation("io.quarkus:quarkus-kafka-streams")
    implementation("io.quarkus:quarkus-arc")
    implementation("io.quarkus:quarkus-resteasy-reactive-jackson")
    implementation("com.datastax.oss.quarkus:cassandra-quarkus-client")

    implementation("com.scylladb:java-driver-core:4.15.0.1") // ScyllaDB shard aware fork of the java driver

    testImplementation("io.quarkus:quarkus-junit5")
}

java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}

tasks.withType<Test> {
    systemProperty("java.util.logging.manager", "org.jboss.logmanager.LogManager")
}
tasks.withType<JavaCompile> {
    options.encoding = "UTF-8"
    options.compilerArgs.add("-parameters")
}
