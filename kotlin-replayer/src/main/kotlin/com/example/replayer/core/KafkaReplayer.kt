package com.example.replayer.core

import com.example.replayer.checkpoint.CheckpointManager
import com.example.replayer.config.ReplayConfig
import com.example.replayer.config.ReplayMode
import com.example.replayer.config.ReplayerConfig
import com.example.replayer.metrics.MetricsServer
import com.example.replayer.model.Event
import com.example.replayer.model.ReplayStats
import com.example.replayer.producer.KafkaEventProducer
import com.example.replayer.reader.ParquetChunkReader
import com.example.replayer.scheduler.SequentialScheduler
import com.example.replayer.scheduler.TimeSyncedScheduler
import mu.KotlinLogging
import java.io.Closeable
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

private val logger = KotlinLogging.logger {}

/**
 * 메인 Kafka 리플레이어
 *
 * 하이브리드 아키텍처의 Phase 2를 담당합니다.
 * Spark에서 준비한 시간별 Parquet 청크를 읽어 Kafka로 리플레이합니다.
 */
class KafkaReplayer(
    private val config: ReplayerConfig,
    private val inputPath: String
) : Closeable {

    private val chunkReader = ParquetChunkReader(
        config = config.hdfs,
        basePath = inputPath
    )

    private val kafkaProducer = KafkaEventProducer(config.kafka)
    private val metricsServer = MetricsServer(config.metrics)
    private val checkpointManager = CheckpointManager(config.checkpoint)

    private val stats = ReplayStats()
    private val isRunning = AtomicBoolean(false)

    /**
     * 리플레이 시작
     *
     * @param resume 체크포인트에서 재개 여부
     */
    fun replay(resume: Boolean = false) {
        logger.info { "Starting replay..." }
        logger.info { "Input path: $inputPath" }
        logger.info { "Mode: ${config.replay.mode}" }
        logger.info { "Speed factor: ${config.replay.speedFactor}" }

        // 메트릭 서버 시작
        metricsServer.start()
        
        // 체크포인트 자동 저장 시작
        checkpointManager.startAutoSave()

        // 청크 정보 조회
        val chunkStats = chunkReader.getStats()
        logger.info { "\n$chunkStats" }

        stats.totalEvents = chunkStats.totalEvents
        stats.totalChunks = chunkStats.totalChunks
        metricsServer.setTotalChunks(chunkStats.totalChunks)

        // 체크포인트에서 재개
        val startChunk = if (resume) {
            val checkpoint = checkpointManager.load()
            if (checkpoint != null) {
                stats.sentEvents = checkpoint.sentCount
                stats.failedEvents = checkpoint.failedCount
                logger.info { "Resuming from chunk ${checkpoint.lastProcessedChunk + 1}" }
                checkpoint.lastProcessedChunk + 1
            } else {
                0
            }
        } else {
            checkpointManager.clear()
            0
        }

        isRunning.set(true)

        when (config.replay.mode) {
            ReplayMode.TIME_SYNCED -> replayTimeSynced(startChunk)
            ReplayMode.SEQUENTIAL -> replaySequential(startChunk)
        }

        logger.info { "Replay completed!" }
        logger.info { stats.summary() }
    }

    /**
     * 시간 동기화 리플레이
     */
    private fun replayTimeSynced(startChunk: Int) {
        logger.info { "Starting time-synced replay mode" }

        val scheduler = TimeSyncedScheduler(
            config = config.replay,
            onSend = { event -> sendEvent(event) },
            onError = { event, error -> handleError(event, error) }
        )

        try {
            val hours = chunkReader.getAvailableHours().filter { it >= startChunk }
            var isFirstChunk = (startChunk == 0)

            for (hour in hours) {
                if (!isRunning.get()) {
                    logger.info { "Replay interrupted" }
                    break
                }

                logger.info { "Processing chunk for hour $hour..." }
                stats.currentChunk = hour
                metricsServer.updateCurrentChunk(hour)

                // 청크 로드
                val events = chunkReader.readChunk(hour)
                if (events.isEmpty()) {
                    logger.info { "No events in chunk $hour, skipping" }
                    continue
                }

                logger.info { "Loaded ${events.size} events for hour $hour" }

                // 이벤트 스케줄링
                val latch = scheduler.scheduleChunk(events, isFirstChunk)
                isFirstChunk = false

                // 청크 완료 대기
                latch.await()
                kafkaProducer.flush()

                // 체크포인트 업데이트
                checkpointManager.update(
                    lastProcessedChunk = hour,
                    lastEventTimeMs = events.last().eventTimeMs,
                    sentCount = stats.sentEvents,
                    failedCount = stats.failedEvents
                )

                // GC
                if (config.replay.gcAfterChunk) {
                    System.gc()
                }

                logger.info { "Chunk $hour completed. ${scheduler.getStats()}" }
            }
        } finally {
            scheduler.close()
        }
    }

    /**
     * 순차 리플레이 (최대 속도)
     */
    private fun replaySequential(startChunk: Int) {
        logger.info { "Starting sequential replay mode" }

        val scheduler = SequentialScheduler(
            config = config.replay,
            onSend = { event -> sendEvent(event) },
            onError = { event, error -> handleError(event, error) }
        )

        val hours = chunkReader.getAvailableHours().filter { it >= startChunk }

        for (hour in hours) {
            if (!isRunning.get()) {
                logger.info { "Replay interrupted" }
                break
            }

            logger.info { "Processing chunk for hour $hour..." }
            stats.currentChunk = hour
            metricsServer.updateCurrentChunk(hour)

            // 청크 로드
            val events = chunkReader.readChunk(hour)
            if (events.isEmpty()) {
                logger.info { "No events in chunk $hour, skipping" }
                continue
            }

            logger.info { "Loaded ${events.size} events for hour $hour" }

            // 순차 처리
            scheduler.processEvents(events)
            kafkaProducer.flush()

            // 체크포인트 업데이트
            checkpointManager.update(
                lastProcessedChunk = hour,
                lastEventTimeMs = events.last().eventTimeMs,
                sentCount = stats.sentEvents,
                failedCount = stats.failedEvents
            )

            // GC
            if (config.replay.gcAfterChunk) {
                System.gc()
            }

            logger.info { "Chunk $hour completed. ${scheduler.getStats()}" }
        }
    }

    private fun sendEvent(event: Event) {
        kafkaProducer.sendAsync(event) { metadata, exception ->
            if (exception != null) {
                stats.failedEvents++
                metricsServer.recordFailed()
                logger.error(exception) { "Failed to send event: ${event.key}" }
            } else {
                stats.sentEvents++
                stats.lastEventTimeMs = event.eventTimeMs
                metricsServer.recordSent()
            }
        }
    }

    private fun handleError(event: Event, error: Throwable) {
        stats.failedEvents++
        metricsServer.recordFailed()
        logger.error(error) { "Failed to send event: ${event.key}" }
    }

    /**
     * 리플레이 중지
     */
    fun stop() {
        logger.info { "Stopping replay..." }
        isRunning.set(false)
    }

    /**
     * 현재 통계 조회
     */
    fun getStats(): ReplayStats = stats.copy()

    override fun close() {
        stop()
        checkpointManager.stop()
        metricsServer.close()
        kafkaProducer.close()
        chunkReader.close()
        logger.info { "Replayer closed" }
    }
}
