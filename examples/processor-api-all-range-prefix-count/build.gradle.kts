/*
 * This file was generated by the Gradle 'init' task.
 *
 * This generated file contains a sample Java application project to get you started.
 * For more details take a look at the 'Building Java & JVM projects' chapter in the Gradle
 * User Manual available at https://docs.gradle.org/7.6/userguide/building_java_projects.html
 */

plugins {
    // Apply the application plugin to add support for building a CLI application in Java.
    application
}

repositories {
    // Use Maven Central for resolving dependencies.
    mavenCentral()
}

dependencies {
    // Use JUnit Jupiter for testing.
    testImplementation("org.junit.jupiter:junit-jupiter:5.9.1")

    // Root lib
    implementation(project(":kafka-streams-cassandra-state-store"))

    // This dependency is used by the application.
    implementation("org.slf4j:slf4j-simple:2.0.7")
    implementation("ch.qos.logback:logback-classic:1.4.8")
    implementation("org.apache.kafka:kafka-streams:3.5.0")
    implementation("com.scylladb:java-driver-core:4.15.0.1")
}

application {
    // Define the main class for the application.
    mainClass.set("dev.thriving.oss.kafka.streams.cassandra.state.store.example.processor.api.misc.ProcessorApiAllRangePrefixCountDemo")
}

tasks.named<Test>("test") {
    // Use JUnit Platform for unit tests.
    useJUnitPlatform()
}
