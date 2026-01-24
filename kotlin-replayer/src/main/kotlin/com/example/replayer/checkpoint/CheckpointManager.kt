package com.example.replayer.checkpoint

import com.example.replayer.config.CheckpointConfig
import com.example.replayer.model.Checkpoint
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import mu.KotlinLogging
import java.io.File
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

private val logger = KotlinLogging.logger {}

/**
 * 체크포인트 관리자
 *
 * 장애 복구를 위해 진행 상황을 주기적으로 저장합니다.
 */
class CheckpointManager(
    private val config: CheckpointConfig
) {
    private val json = Json { 
        prettyPrint = true 
        ignoreUnknownKeys = true
    }
    
    private val currentCheckpoint = AtomicReference<Checkpoint?>(null)
    private var scheduler: ScheduledExecutorService? = null

    /**
     * 자동 저장 시작
     */
    fun startAutoSave() {
        if (!config.enabled) {
            logger.info { "Checkpoint auto-save is disabled" }
            return
        }

        scheduler = Executors.newSingleThreadScheduledExecutor { r ->
            Thread(r, "checkpoint-saver").apply { isDaemon = true }
        }

        scheduler?.scheduleAtFixedRate(
            { saveCurrentCheckpoint() },
            config.intervalSeconds.toLong(),
            config.intervalSeconds.toLong(),
            TimeUnit.SECONDS
        )

        logger.info { "Checkpoint auto-save started (interval: ${config.intervalSeconds}s)" }
    }

    /**
     * 체크포인트 업데이트
     */
    fun update(
        lastProcessedChunk: Int,
        lastEventTimeMs: Long,
        sentCount: Long,
        failedCount: Long
    ) {
        currentCheckpoint.set(
            Checkpoint(
                lastProcessedChunk = lastProcessedChunk,
                lastEventTimeMs = lastEventTimeMs,
                sentCount = sentCount,
                failedCount = failedCount
            )
        )
    }

    /**
     * 현재 체크포인트 저장
     */
    fun saveCurrentCheckpoint() {
        val checkpoint = currentCheckpoint.get() ?: return
        save(checkpoint)
    }

    /**
     * 체크포인트 저장
     */
    fun save(checkpoint: Checkpoint) {
        if (!config.enabled) return

        try {
            val file = File(config.path)
            file.parentFile?.mkdirs()
            
            val jsonStr = json.encodeToString(
                CheckpointData(
                    lastProcessedChunk = checkpoint.lastProcessedChunk,
                    lastEventTimeMs = checkpoint.lastEventTimeMs,
                    sentCount = checkpoint.sentCount,
                    failedCount = checkpoint.failedCount,
                    savedAtMs = checkpoint.savedAtMs
                )
            )
            file.writeText(jsonStr)
            
            logger.debug { "Checkpoint saved: chunk=${checkpoint.lastProcessedChunk}" }
        } catch (e: Exception) {
            logger.error(e) { "Failed to save checkpoint" }
        }
    }

    /**
     * 체크포인트 로드
     */
    fun load(): Checkpoint? {
        if (!config.enabled) return null

        val file = File(config.path)
        if (!file.exists()) {
            logger.info { "No checkpoint file found at ${config.path}" }
            return null
        }

        return try {
            val data = json.decodeFromString<CheckpointData>(file.readText())
            val checkpoint = Checkpoint(
                lastProcessedChunk = data.lastProcessedChunk,
                lastEventTimeMs = data.lastEventTimeMs,
                sentCount = data.sentCount,
                failedCount = data.failedCount,
                savedAtMs = data.savedAtMs
            )
            logger.info { "Checkpoint loaded: chunk=${checkpoint.lastProcessedChunk}" }
            checkpoint
        } catch (e: Exception) {
            logger.error(e) { "Failed to load checkpoint" }
            null
        }
    }

    /**
     * 체크포인트 삭제
     */
    fun clear() {
        val file = File(config.path)
        if (file.exists()) {
            file.delete()
            logger.info { "Checkpoint cleared" }
        }
        currentCheckpoint.set(null)
    }

    /**
     * 자동 저장 중지
     */
    fun stop() {
        // 마지막 체크포인트 저장
        saveCurrentCheckpoint()
        
        scheduler?.shutdown()
        scheduler?.awaitTermination(5, TimeUnit.SECONDS)
        logger.info { "Checkpoint manager stopped" }
    }
}

/**
 * 체크포인트 직렬화용 데이터 클래스
 */
@kotlinx.serialization.Serializable
private data class CheckpointData(
    val lastProcessedChunk: Int,
    val lastEventTimeMs: Long,
    val sentCount: Long,
    val failedCount: Long,
    val savedAtMs: Long
)
