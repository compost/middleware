plugins {
    java
    application
    id("com.github.johnrengelman.shadow") version "8.1.1"
}

group = "io.symplify"
version = "0.1.0-SNAPSHOT"

val flinkVersion = "1.18.1"
val awsSdkVersion = "2.21.1"
val jacksonVersion = "2.15.2"
val log4jVersion = "2.17.1"

repositories {
    mavenCentral()
}

dependencies {
    // Flink
    implementation("org.apache.flink:flink-streaming-java:${flinkVersion}")
    implementation("org.apache.flink:flink-clients:${flinkVersion}")
    implementation("org.apache.flink:flink-connector-kafka:3.1.0-1.18")
    implementation("org.apache.flink:flink-connector-aws-kinesis-streams:4.3.0-1.18") 
    
    // AWS SDK v2 (for SQS if not using Flink connector, or for custom sinks)
    implementation("software.amazon.awssdk:sqs:${awsSdkVersion}")
    implementation("software.amazon.awssdk:url-connection-client:${awsSdkVersion}")
    implementation("software.amazon.awssdk:netty-nio-client:${awsSdkVersion}")

    // Jackson
    implementation("com.fasterxml.jackson.core:jackson-databind:${jacksonVersion}")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jdk8:${jacksonVersion}")

    // Logging
    implementation("org.apache.logging.log4j:log4j-slf4j-impl:${log4jVersion}")
    implementation("org.apache.logging.log4j:log4j-api:${log4jVersion}")
    implementation("org.apache.logging.log4j:log4j-core:${log4jVersion}")

    // Lombok
    compileOnly("org.projectlombok:lombok:1.18.30")
    annotationProcessor("org.projectlombok:lombok:1.18.30")

    // Test dependencies
    testImplementation("org.junit.jupiter:junit-jupiter:5.10.0")
    testImplementation("org.assertj:assertj-core:3.24.2")
    testImplementation("org.testcontainers:testcontainers:1.19.7")
    testImplementation("org.testcontainers:junit-jupiter:1.19.7")
    testImplementation("org.testcontainers:kafka:1.19.7")
    testImplementation("org.testcontainers:localstack:1.19.7")
    testImplementation("net.java.dev.jna:jna:5.14.0") // Helper for docker-java on macOS
    testImplementation("org.awaitility:awaitility:4.2.0")
    testImplementation("org.apache.flink:flink-test-utils:${flinkVersion}") {
        exclude(group = "junit", module = "junit")
    }
}

tasks.test {
    useJUnitPlatform()
    environment("DOCKER_HOST", System.getenv("DOCKER_HOST"))
    environment("DOCKER_CERT_PATH", System.getenv("DOCKER_CERT_PATH"))
    environment("DOCKER_TLS_VERIFY", System.getenv("DOCKER_TLS_VERIFY"))
    environment("TESTCONTAINERS_RYUK_DISABLED", "true") // Optional: disable Ryuk if it causes issues in Colima
}


java {
    sourceCompatibility = JavaVersion.VERSION_21
    targetCompatibility = JavaVersion.VERSION_21
}

application {
    mainClass.set("io.symplify.flink.FlinkJob")
}

tasks.withType<JavaCompile> {
    options.encoding = "UTF-8"
}

tasks.shadowJar {
    mergeServiceFiles()
}
