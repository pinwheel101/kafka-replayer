package com.example.replayer

import com.example.replayer.config.*
import com.example.replayer.core.KafkaReplayer
import com.example.replayer.reader.ParquetChunkReader
import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.core.subcommands
import com.github.ajalt.clikt.parameters.options.*
import com.github.ajalt.clikt.parameters.types.*
import mu.KotlinLogging
import kotlin.system.exitProcess

private val logger = KotlinLogging.logger {}

/**
 * 메인 CLI 애플리케이션
 */
class ReplayerCli : CliktCommand(
    name = "kafka-replayer",
    help = "Kafka Time-Synced Replayer - Replay events from HDFS to Kafka with precise timing"
) {
    override fun run() = Unit
}

/**
 * 리플레이 명령어
 */
class ReplayCommand : CliktCommand(
    name = "replay",
    help = "Start replaying events from prepared Parquet chunks to Kafka"
) {
    // HDFS 설정
    private val hdfsUri by option("--hdfs-uri", help = "HDFS URI (e.g., hdfs://namenode:8020)")
        .default("hdfs://localhost:8020")

    private val inputPath by option("--input-path", "-i", help = "Input path with prepared Parquet chunks")
        .required()

    // Kafka 설정
    private val kafkaBootstrap by option("--kafka-bootstrap", "-k", help = "Kafka bootstrap servers")
        .default("localhost:9092")

    private val topic by option("--topic", "-t", help = "Target Kafka topic")
        .required()

    // 리플레이 모드
    private val mode by option("--mode", "-m", help = "Replay mode: time-synced or sequential")
        .choice("time-synced", "sequential")
        .default("time-synced")

    private val speedFactor by option("--speed", "-s", help = "Speed factor for time-synced mode (1.0 = realtime)")
        .double()
        .default(1.0)

    private val delayMs by option("--delay-ms", help = "Delay between events in sequential mode (ms)")
        .long()
        .default(0L)

    // 체크포인트
    private val resume by option("--resume", "-r", help = "Resume from last checkpoint")
        .flag(default = false)

    private val checkpointPath by option("--checkpoint-path", help = "Checkpoint file path")
        .default("/tmp/kafka-replayer-checkpoint.json")

    // 메트릭
    private val metricsPort by option("--metrics-port", help = "Prometheus metrics port")
        .int()
        .default(9090)

    private val metricsEnabled by option("--metrics-enabled", help = "Enable metrics server")
        .flag(default = true)

    // 설정 파일
    private val configFile by option("--config", "-c", help = "Configuration file path")

    override fun run() {
        echo("=" .repeat(60))
        echo("Kafka Time-Synced Replayer")
        echo("=" .repeat(60))

        val config = buildConfig()
        
        echo("Configuration:")
        echo("  HDFS URI: ${config.hdfs.uri}")
        echo("  Input Path: $inputPath")
        echo("  Kafka: ${config.kafka.bootstrapServers}")
        echo("  Topic: ${config.kafka.topic}")
        echo("  Mode: ${config.replay.mode}")
        echo("  Speed Factor: ${config.replay.speedFactor}")
        echo("  Resume: $resume")
        echo("=" .repeat(60))

        val replayer = KafkaReplayer(config, inputPath)

        // Shutdown hook 등록
        Runtime.getRuntime().addShutdownHook(Thread {
            echo("\nShutting down...")
            replayer.close()
        })

        try {
            replayer.replay(resume)
        } catch (e: Exception) {
            logger.error(e) { "Replay failed" }
            echo("Error: ${e.message}")
            exitProcess(1)
        } finally {
            replayer.close()
        }
    }

    private fun buildConfig(): ReplayerConfig {
        // 설정 파일이 있으면 로드
        val baseConfig = if (configFile != null) {
            ReplayerConfig.load(configFile)
        } else {
            ReplayerConfig()
        }

        // CLI 옵션으로 오버라이드
        return baseConfig.copy(
            hdfs = baseConfig.hdfs.copy(
                uri = hdfsUri,
                inputPath = inputPath
            ),
            kafka = baseConfig.kafka.copy(
                bootstrapServers = kafkaBootstrap,
                topic = topic
            ),
            replay = baseConfig.replay.copy(
                mode = if (mode == "time-synced") ReplayMode.TIME_SYNCED else ReplayMode.SEQUENTIAL,
                speedFactor = speedFactor,
                delayMs = delayMs
            ),
            metrics = baseConfig.metrics.copy(
                enabled = metricsEnabled,
                port = metricsPort
            ),
            checkpoint = baseConfig.checkpoint.copy(
                path = checkpointPath
            )
        )
    }
}

/**
 * 상태 확인 명령어
 */
class StatsCommand : CliktCommand(
    name = "stats",
    help = "Show statistics about prepared data chunks"
) {
    private val hdfsUri by option("--hdfs-uri", help = "HDFS URI")
        .default("hdfs://localhost:8020")

    private val inputPath by option("--input-path", "-i", help = "Input path with prepared Parquet chunks")
        .required()

    override fun run() {
        echo("Analyzing chunks at: $inputPath")
        echo("")

        val config = HdfsConfig(uri = hdfsUri)
        val reader = ParquetChunkReader(config, inputPath)

        try {
            val stats = reader.getStats()
            echo(stats.toString())
        } finally {
            reader.close()
        }
    }
}

/**
 * 검증 명령어
 */
class ValidateCommand : CliktCommand(
    name = "validate",
    help = "Validate prepared data and configuration"
) {
    private val hdfsUri by option("--hdfs-uri", help = "HDFS URI")
        .default("hdfs://localhost:8020")

    private val inputPath by option("--input-path", "-i", help = "Input path with prepared Parquet chunks")
        .required()

    private val kafkaBootstrap by option("--kafka-bootstrap", "-k", help = "Kafka bootstrap servers")
        .default("localhost:9092")

    private val topic by option("--topic", "-t", help = "Target Kafka topic")
        .required()

    override fun run() {
        echo("Validating configuration...")
        echo("")

        var hasErrors = false

        // HDFS 검증
        echo("1. Checking HDFS connection...")
        try {
            val config = HdfsConfig(uri = hdfsUri)
            val reader = ParquetChunkReader(config, inputPath)
            val hours = reader.getAvailableHours()
            
            if (hours.isEmpty()) {
                echo("   ❌ No chunks found at $inputPath")
                hasErrors = true
            } else {
                echo("   ✓ Found ${hours.size} chunks: $hours")
            }
            reader.close()
        } catch (e: Exception) {
            echo("   ❌ HDFS connection failed: ${e.message}")
            hasErrors = true
        }

        // Kafka 검증
        echo("2. Checking Kafka connection...")
        try {
            val props = mapOf(
                "bootstrap.servers" to kafkaBootstrap,
                "key.serializer" to "org.apache.kafka.common.serialization.StringSerializer",
                "value.serializer" to "org.apache.kafka.common.serialization.ByteArraySerializer",
                "max.block.ms" to "5000"
            )
            val producer = org.apache.kafka.clients.producer.KafkaProducer<String, ByteArray>(props)
            producer.partitionsFor(topic)
            producer.close()
            echo("   ✓ Kafka connection successful, topic '$topic' accessible")
        } catch (e: Exception) {
            echo("   ❌ Kafka connection failed: ${e.message}")
            hasErrors = true
        }

        echo("")
        if (hasErrors) {
            echo("Validation failed! Please fix the errors above.")
            exitProcess(1)
        } else {
            echo("✓ All validations passed!")
        }
    }
}

/**
 * 메인 진입점
 */
fun main(args: Array<String>) {
    ReplayerCli()
        .subcommands(ReplayCommand(), StatsCommand(), ValidateCommand())
        .main(args)
}
