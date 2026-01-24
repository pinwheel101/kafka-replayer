package com.example.replayer.producer

import com.example.replayer.config.KafkaConfig
import com.example.replayer.model.Event
import mu.KotlinLogging
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import java.io.Closeable
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

private val logger = KotlinLogging.logger {}

/**
 * Kafka 이벤트 프로듀서
 *
 * 이벤트를 Kafka 토픽으로 전송합니다.
 */
class KafkaEventProducer(
    private val config: KafkaConfig
) : Closeable {

    private val producer: KafkaProducer<String, ByteArray> = createProducer()
    
    private val sentCount = AtomicLong(0)
    private val failedCount = AtomicLong(0)
    private val totalLatencyMs = AtomicLong(0)

    private fun createProducer(): KafkaProducer<String, ByteArray> {
        val props = config.toProducerProperties()
        logger.info { "Creating Kafka producer with config: $props" }
        return KafkaProducer(props)
    }

    /**
     * 이벤트 동기 전송 (순서 보장)
     */
    fun sendSync(event: Event): RecordMetadata {
        val startTime = System.currentTimeMillis()
        
        val record = ProducerRecord(
            config.topic,
            event.key,
            event.payload
        )

        return try {
            val metadata = producer.send(record).get(30, TimeUnit.SECONDS)
            
            val latency = System.currentTimeMillis() - startTime
            totalLatencyMs.addAndGet(latency)
            sentCount.incrementAndGet()
            
            logger.trace { 
                "Sent event ${event.key} to ${metadata.topic()}-${metadata.partition()} " +
                "offset=${metadata.offset()}, latency=${latency}ms" 
            }
            
            metadata
        } catch (e: Exception) {
            failedCount.incrementAndGet()
            logger.error(e) { "Failed to send event: ${event.key}" }
            throw e
        }
    }

    /**
     * 이벤트 비동기 전송 (처리량 우선)
     */
    fun sendAsync(event: Event, onComplete: ((RecordMetadata?, Exception?) -> Unit)? = null): Future<RecordMetadata> {
        val startTime = System.currentTimeMillis()
        
        val record = ProducerRecord(
            config.topic,
            event.key,
            event.payload
        )

        return producer.send(record) { metadata, exception ->
            val latency = System.currentTimeMillis() - startTime
            
            if (exception != null) {
                failedCount.incrementAndGet()
                logger.error(exception) { "Failed to send event: ${event.key}" }
            } else {
                totalLatencyMs.addAndGet(latency)
                sentCount.incrementAndGet()
                
                logger.trace { 
                    "Sent event ${event.key} to ${metadata.topic()}-${metadata.partition()} " +
                    "offset=${metadata.offset()}, latency=${latency}ms" 
                }
            }
            
            onComplete?.invoke(metadata, exception)
        }
    }

    /**
     * 배치 이벤트 전송 (비동기)
     */
    fun sendBatch(events: List<Event>, onComplete: ((Int, Int) -> Unit)? = null) {
        var successCount = 0
        var errorCount = 0
        val latch = java.util.concurrent.CountDownLatch(events.size)

        for (event in events) {
            sendAsync(event) { metadata, exception ->
                if (exception != null) {
                    errorCount++
                } else {
                    successCount++
                }
                latch.countDown()
            }
        }

        // 배치 완료 대기
        latch.await()
        onComplete?.invoke(successCount, errorCount)
    }

    /**
     * 버퍼 플러시
     */
    fun flush() {
        producer.flush()
    }

    /**
     * 프로듀서 통계
     */
    fun getStats(): ProducerStats {
        val sent = sentCount.get()
        val avgLatency = if (sent > 0) {
            totalLatencyMs.get().toDouble() / sent
        } else 0.0

        return ProducerStats(
            sent = sent,
            failed = failedCount.get(),
            avgLatencyMs = avgLatency
        )
    }

    /**
     * 통계 리셋
     */
    fun resetStats() {
        sentCount.set(0)
        failedCount.set(0)
        totalLatencyMs.set(0)
    }

    override fun close() {
        logger.info { "Closing Kafka producer. Final stats: ${getStats()}" }
        producer.flush()
        producer.close()
    }
}

/**
 * 프로듀서 통계
 */
data class ProducerStats(
    val sent: Long,
    val failed: Long,
    val avgLatencyMs: Double
) {
    val total: Long get() = sent + failed
    
    val successRate: Double 
        get() = if (total > 0) sent.toDouble() / total * 100 else 0.0

    override fun toString(): String {
        return "ProducerStats(sent=$sent, failed=$failed, avgLatency=${"%.2f".format(avgLatencyMs)}ms, " +
               "successRate=${"%.2f".format(successRate)}%)"
    }
}
