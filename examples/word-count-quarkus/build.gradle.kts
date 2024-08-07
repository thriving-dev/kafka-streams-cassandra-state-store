plugins {
    java
    alias(libs.plugins.quarkus)
}

repositories {
    mavenCentral()
}

val quarkusPlatformGroupId: String by project
val quarkusPlatformArtifactId: String by project
val quarkusPlatformVersion: String by project

dependencies {
    implementation(enforcedPlatform(libs.quarkus.bom))
    implementation(enforcedPlatform(libs.quarkus.cassandra.bom))
    implementation("io.quarkus:quarkus-container-image-docker")

    // Root lib
    implementation(project(":kafka-streams-cassandra-state-store"))

    implementation("io.quarkus:quarkus-kafka-streams")
    implementation("io.quarkus:quarkus-arc")
    implementation("io.quarkus:quarkus-resteasy-reactive-jackson")
    implementation("com.datastax.oss.quarkus:cassandra-quarkus-client")

    // ScyllaDB shard aware fork of the java driver
    implementation(libs.scylladb.java.driver)

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
