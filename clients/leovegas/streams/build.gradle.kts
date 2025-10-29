plugins {
    java
    id("io.quarkus")
}

repositories {
    mavenCentral()
    mavenLocal()
}

val quarkusPlatformGroupId: String by project
val quarkusPlatformArtifactId: String by project
val quarkusPlatformVersion: String by project

dependencies {
    implementation("io.quarkus:quarkus-rest-jackson")
    implementation("io.quarkus:quarkus-rest")
    implementation(enforcedPlatform("${quarkusPlatformGroupId}:${quarkusPlatformArtifactId}:${quarkusPlatformVersion}"))
    implementation(enforcedPlatform("${quarkusPlatformGroupId}:quarkus-amazon-services-bom:${quarkusPlatformVersion}"))
    implementation("io.quarkus:quarkus-smallrye-health")
    implementation("io.quarkiverse.loggingjson:quarkus-logging-json:3.1.0")
    implementation("io.quarkus:quarkus-kafka-streams")
    implementation("io.quarkiverse.amazonservices:quarkus-amazon-sqs")
    implementation("io.quarkus:quarkus-arc")
    implementation("io.quarkus:quarkus-container-image-docker")
    implementation("software.amazon.awssdk:aws-crt-client")
    implementation("software.amazon.awssdk:url-connection-client")
    testImplementation("io.quarkus:quarkus-junit5")
    testImplementation("io.quarkus:quarkus-junit5-component")
    testImplementation("org.apache.kafka:kafka-streams-test-utils")
}

group = "io.symplify"
version = "20251029"

java {
    sourceCompatibility = JavaVersion.VERSION_21
    targetCompatibility = JavaVersion.VERSION_21
}

tasks.withType<Test> {
    systemProperty("java.util.logging.manager", "org.jboss.logmanager.LogManager")
}
tasks.withType<JavaCompile> {
    options.encoding = "UTF-8"
    options.compilerArgs.add("-parameters")
}
