plugins {
    kotlin("jvm") version "1.9.22"
    kotlin("plugin.serialization") version "1.9.22"
    application
    id("com.github.johnrengelman.shadow") version "8.1.1"
}

group = "com.example"
version = "1.0.0"

repositories {
    mavenCentral()
}

val hadoopVersion = "3.3.6"
val kafkaVersion = "3.6.1"
val parquetVersion = "1.13.1"
val nettyVersion = "4.1.106.Final"
val coroutinesVersion = "1.7.3"

dependencies {
    // Kotlin
    implementation(kotlin("stdlib"))
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:$coroutinesVersion")
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.6.2")
    
    // Hadoop & Parquet
    implementation("org.apache.hadoop:hadoop-client:$hadoopVersion") {
        exclude(group = "org.slf4j")
        exclude(group = "log4j")
        exclude(group = "org.apache.logging.log4j")
    }
    implementation("org.apache.hadoop:hadoop-common:$hadoopVersion") {
        exclude(group = "org.slf4j")
        exclude(group = "log4j")
        exclude(group = "org.apache.logging.log4j")
    }
    implementation("org.apache.parquet:parquet-avro:$parquetVersion")
    implementation("org.apache.parquet:parquet-hadoop:$parquetVersion")
    
    // Avro (Parquet 읽기용)
    implementation("org.apache.avro:avro:1.11.3")
    
    // Kafka
    implementation("org.apache.kafka:kafka-clients:$kafkaVersion")
    
    // Timer (정밀 스케줄링)
    implementation("io.netty:netty-common:$nettyVersion")
    
    // CLI
    implementation("com.github.ajalt.clikt:clikt:4.2.2")
    
    // Configuration
    implementation("com.sksamuel.hoplite:hoplite-core:2.7.5")
    implementation("com.sksamuel.hoplite:hoplite-yaml:2.7.5")
    
    // Logging
    implementation("io.github.microutils:kotlin-logging-jvm:3.0.5")
    implementation("ch.qos.logback:logback-classic:1.4.14")
    
    // Metrics
    implementation("io.micrometer:micrometer-core:1.12.2")
    implementation("io.micrometer:micrometer-registry-prometheus:1.12.2")
    
    // HTTP Server (메트릭 노출용)
    implementation("io.ktor:ktor-server-core:2.3.7")
    implementation("io.ktor:ktor-server-netty:2.3.7")
    
    // Testing
    testImplementation(kotlin("test"))
    testImplementation("org.junit.jupiter:junit-jupiter:5.10.1")
    testImplementation("io.mockk:mockk:1.13.9")
    testImplementation("org.testcontainers:testcontainers:1.19.3")
    testImplementation("org.testcontainers:kafka:1.19.3")
}

application {
    mainClass.set("com.example.replayer.MainKt")
}

tasks.test {
    useJUnitPlatform()
}

tasks.shadowJar {
    archiveBaseName.set("kafka-replayer")
    archiveClassifier.set("all")
    archiveVersion.set(version.toString())
    
    mergeServiceFiles()
    
    manifest {
        attributes(
            "Main-Class" to "com.example.replayer.MainKt",
            "Implementation-Title" to "Kafka Time-Synced Replayer",
            "Implementation-Version" to version
        )
    }
    
    // 중복 파일 처리
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    
    // 불필요한 파일 제외
    exclude("META-INF/*.SF")
    exclude("META-INF/*.DSA")
    exclude("META-INF/*.RSA")
}

tasks.jar {
    manifest {
        attributes(
            "Main-Class" to "com.example.replayer.MainKt"
        )
    }
}

kotlin {
    jvmToolchain(17)
}
