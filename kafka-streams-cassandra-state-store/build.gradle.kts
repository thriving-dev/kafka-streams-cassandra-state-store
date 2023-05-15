group = "dev.thriving.oss"

object Meta {
    const val desc = "Kafka Streams State Store implementation that persists data to Apache Cassandra"
    const val license = "Apache-2.0"
    const val githubRepo = "thriving-dev/kafka-streams-cassandra-state-store"
    const val release = "https://s01.oss.sonatype.org/service/local/"
    const val snapshot = "https://s01.oss.sonatype.org/content/repositories/snapshots/"
}

plugins {
    // Apply the java-library plugin for API and implementation separation.
    `java-library`
    `maven-publish`
    signing
}

repositories {
    mavenCentral()
}

sourceSets {
    create("intTest") {
        compileClasspath += sourceSets.main.get().output
        runtimeClasspath += sourceSets.main.get().output
    }
}

val intTestImplementation by configurations.getting {
    extendsFrom(configurations.implementation.get())
}

configurations["intTestRuntimeOnly"].extendsFrom(configurations.runtimeOnly.get())

dependencies {
    // This dependency is used internally, and not exposed to consumers on their own compile classpath.
    compileOnly("org.apache.kafka:kafka-streams:3.4.0")
    compileOnly("com.datastax.oss:java-driver-core:4.15.0")

    intTestImplementation("org.junit.jupiter:junit-jupiter:5.9.3")
    intTestImplementation("org.testcontainers:testcontainers:1.18.1")
    intTestImplementation("org.testcontainers:junit-jupiter:1.18.1")
    intTestImplementation("org.testcontainers:redpanda:1.18.1")
    intTestImplementation("org.testcontainers:cassandra:1.18.1")
    intTestImplementation("ch.qos.logback:logback-classic:1.4.7")
    intTestImplementation("org.apache.kafka:kafka-streams:3.4.0")
    intTestImplementation("com.datastax.oss:java-driver-core:4.15.0")
    intTestImplementation("org.assertj:assertj-core:3.24.2")
    intTestImplementation("com.google.guava:guava:31.1-jre")
}

val intTest = task<Test>("intTest") {
    description = "Runs integration tests."
    group = "verification"

    testClassesDirs = sourceSets["intTest"].output.classesDirs
    classpath = sourceSets["intTest"].runtimeClasspath
    shouldRunAfter("test")

    useJUnitPlatform()

    testLogging {
        events("passed")
    }
}

tasks.check { dependsOn(intTest) }

tasks.named<Test>("test") {
    // Use JUnit Platform for unit tests.
    useJUnitPlatform()
}

signing {
    val signingKey = providers.environmentVariable("GPG_SIGNING_KEY")
    val signingPassphrase = providers.environmentVariable("GPG_SIGNING_PASSPHRASE")
    if (signingKey.isPresent && signingPassphrase.isPresent) {
        useInMemoryPgpKeys(signingKey.get(), signingPassphrase.get())
        val extension = extensions.getByName("publishing") as PublishingExtension
        sign(extension.publications)
    }
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            groupId = project.group.toString()
            artifactId = project.name
            version = project.version.toString()
            from(components["java"])
            pom {
                name.set(project.name)
                description.set(Meta.desc)
                url.set("https://github.com/${Meta.githubRepo}")
                licenses {
                    license {
                        name.set(Meta.license)
                        url.set("https://opensource.org/licenses/Apache-2.0")
                    }
                }
                developers {
                    developer {
                        id.set("hartmut-co-uk")
                        name.set("Hartmut Armbruster")
                        organization.set("thriving.dev")
                        organizationUrl.set("https://www.thriving.dev/")
                    }
                }
                scm {
                    url.set("https://github.com/${Meta.githubRepo}.git")
                    connection.set("scm:git:git://github.com/${Meta.githubRepo}.git")
                    developerConnection.set("scm:git:git://github.com/${Meta.githubRepo}.git")
                }
                issueManagement {
                    url.set("https://github.com/${Meta.githubRepo}/issues")
                }
            }
        }
    }
}

tasks.jar {
    manifest {
        attributes(
            mapOf(
                "Implementation-Title" to project.name,
                "Implementation-Version" to project.version
            )
        )
    }
}

java {
    withSourcesJar()
    withJavadocJar()
}
