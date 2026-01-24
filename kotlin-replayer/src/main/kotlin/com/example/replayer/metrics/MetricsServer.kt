package com.example.replayer.metrics

import com.example.replayer.config.MetricsConfig
import io.ktor.server.application.*
import io.ktor.server.engine.*
import io.ktor.server.netty.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.Timer
import io.micrometer.prometheus.PrometheusConfig
import io.micrometer.prometheus.PrometheusMeterRegistry
import mu.KotlinLogging
import java.io.Closeable
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

private val logger = KotlinLogging.logger {}

/**
 * Prometheus 메트릭 서버
 */
class MetricsServer(
    private val config: MetricsConfig
) : Closeable {

    private val registry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
    private var server: NettyApplicationEngine? = null

    // 카운터
    private val eventsSentCounter: Counter = Counter.builder("replayer_events_sent_total")
        .description("Total number of events sent to Kafka")
        .register(registry)

    private val eventsFailedCounter: Counter = Counter.builder("replayer_events_failed_total")
        .description("Total number of failed events")
        .register(registry)

    // 게이지
    private val currentChunk = AtomicLong(0)
    private val totalChunks = AtomicLong(24)
    private val currentTps = AtomicLong(0)
    private val timingDriftMs = AtomicLong(0)

    init {
        Gauge.builder("replayer_current_chunk", currentChunk) { it.get().toDouble() }
            .description("Current chunk being processed")
            .register(registry)

        Gauge.builder("replayer_total_chunks", totalChunks) { it.get().toDouble() }
            .description("Total number of chunks")
            .register(registry)

        Gauge.builder("replayer_tps", currentTps) { it.get().toDouble() }
            .description("Current events per second")
            .register(registry)

        Gauge.builder("replayer_timing_drift_ms", timingDriftMs) { it.get().toDouble() }
            .description("Timing drift from expected send time in milliseconds")
            .register(registry)
    }

    // 타이머
    private val sendLatencyTimer: Timer = Timer.builder("replayer_send_latency")
        .description("Kafka send latency")
        .register(registry)

    /**
     * 메트릭 서버 시작
     */
    fun start() {
        if (!config.enabled) {
            logger.info { "Metrics server is disabled" }
            return
        }

        server = embeddedServer(Netty, port = config.port) {
            routing {
                get(config.path) {
                    call.respondText(registry.scrape())
                }
                get("/health") {
                    call.respondText("OK")
                }
            }
        }.start(wait = false)

        logger.info { "Metrics server started on port ${config.port}" }
    }

    /**
     * 이벤트 전송 성공 기록
     */
    fun recordSent() {
        eventsSentCounter.increment()
    }

    /**
     * 이벤트 전송 실패 기록
     */
    fun recordFailed() {
        eventsFailedCounter.increment()
    }

    /**
     * 전송 지연 시간 기록
     */
    fun recordLatency(latencyMs: Long) {
        sendLatencyTimer.record(latencyMs, TimeUnit.MILLISECONDS)
    }

    /**
     * 현재 청크 업데이트
     */
    fun updateCurrentChunk(chunk: Int) {
        currentChunk.set(chunk.toLong())
    }

    /**
     * 전체 청크 수 설정
     */
    fun setTotalChunks(total: Int) {
        totalChunks.set(total.toLong())
    }

    /**
     * TPS 업데이트
     */
    fun updateTps(tps: Long) {
        currentTps.set(tps)
    }

    /**
     * 타이밍 드리프트 업데이트
     */
    fun updateTimingDrift(driftMs: Long) {
        timingDriftMs.set(driftMs)
    }

    override fun close() {
        server?.stop(1000, 2000)
        logger.info { "Metrics server stopped" }
    }
}
