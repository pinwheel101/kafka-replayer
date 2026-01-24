package com.example.replayer.config

import com.sksamuel.hoplite.ConfigLoaderBuilder
import com.sksamuel.hoplite.addResourceSource
import com.sksamuel.hoplite.addFileSource
import java.io.File

/**
 * 전체 애플리케이션 설정
 */
data class ReplayerConfig(
    val hdfs: HdfsConfig = HdfsConfig(),
    val kafka: KafkaConfig = KafkaConfig(),
    val replay: ReplayConfig = ReplayConfig(),
    val metrics: MetricsConfig = MetricsConfig(),
    val checkpoint: CheckpointConfig = CheckpointConfig()
) {
    companion object {
        /**
         * 설정 파일에서 로드
         */
        fun load(configFile: String? = null): ReplayerConfig {
            val builder = ConfigLoaderBuilder.default()
            
            // 외부 설정 파일이 지정된 경우
            if (configFile != null && File(configFile).exists()) {
                builder.addFileSource(configFile)
            }
            
            // 기본 리소스 설정
            builder.addResourceSource("/application.yaml", optional = true)
            
            return builder.build().loadConfigOrThrow()
        }
    }
}

/**
 * HDFS 설정
 */
data class HdfsConfig(
    val uri: String = "hdfs://localhost:8020",
    val user: String = "hadoop",
    val inputPath: String = "/replay/prepared",
    
    // Hadoop 추가 설정
    val replication: Int = 3,
    val blockSize: Long = 134217728,  // 128MB
    
    // Kerberos 설정 (선택)
    val kerberosEnabled: Boolean = false,
    val kerberosPrincipal: String = "",
    val kerberosKeytab: String = ""
)

/**
 * Kafka 설정
 */
data class KafkaConfig(
    val bootstrapServers: String = "localhost:9092",
    val topic: String = "events-replay",
    
    // Producer 설정
    val acks: String = "all",
    val batchSize: Int = 65536,           // 64KB
    val lingerMs: Int = 5,
    val bufferMemory: Long = 134217728,   // 128MB
    val maxInFlightRequests: Int = 5,
    val retries: Int = 3,
    val retryBackoffMs: Long = 100,
    val compressionType: String = "snappy",
    
    // 보안 설정 (선택)
    val securityProtocol: String = "PLAINTEXT",
    val saslMechanism: String = "",
    val saslJaasConfig: String = ""
) {
    fun toProducerProperties(): Map<String, Any> {
        val props = mutableMapOf<String, Any>(
            "bootstrap.servers" to bootstrapServers,
            "key.serializer" to "org.apache.kafka.common.serialization.StringSerializer",
            "value.serializer" to "org.apache.kafka.common.serialization.ByteArraySerializer",
            "acks" to acks,
            "batch.size" to batchSize,
            "linger.ms" to lingerMs,
            "buffer.memory" to bufferMemory,
            "max.in.flight.requests.per.connection" to maxInFlightRequests,
            "retries" to retries,
            "retry.backoff.ms" to retryBackoffMs,
            "compression.type" to compressionType
        )
        
        // 보안 설정
        if (securityProtocol != "PLAINTEXT") {
            props["security.protocol"] = securityProtocol
            if (saslMechanism.isNotBlank()) {
                props["sasl.mechanism"] = saslMechanism
            }
            if (saslJaasConfig.isNotBlank()) {
                props["sasl.jaas.config"] = saslJaasConfig
            }
        }
        
        return props
    }
}

/**
 * 리플레이 설정
 */
data class ReplayConfig(
    val mode: ReplayMode = ReplayMode.TIME_SYNCED,
    val speedFactor: Double = 1.0,
    val delayMs: Long = 0,
    val chunkHours: Int = 1,
    
    // 타이머 설정
    val timerTickMs: Long = 1,
    val timerWheelSize: Int = 65536,
    
    // 병렬 처리 (Sequential 모드용)
    val parallelism: Int = 4,
    
    // 청크 처리 설정
    val preloadNextChunk: Boolean = true,
    val gcAfterChunk: Boolean = true
)

enum class ReplayMode {
    TIME_SYNCED,    // 시간 동기화 모드
    SEQUENTIAL      // 순차 모드
}

/**
 * 메트릭 설정
 */
data class MetricsConfig(
    val enabled: Boolean = true,
    val port: Int = 9090,
    val path: String = "/metrics"
)

/**
 * 체크포인트 설정
 */
data class CheckpointConfig(
    val enabled: Boolean = true,
    val path: String = "/tmp/kafka-replayer-checkpoint.json",
    val intervalSeconds: Int = 60
)
