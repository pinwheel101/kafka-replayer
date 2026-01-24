package com.example.replayer.model

/**
 * 리플레이 대상 이벤트
 *
 * @property key Kafka 메시지 키
 * @property payload Kafka 메시지 페이로드
 * @property eventTimeMs 원본 이벤트 발생 시간 (epoch milliseconds)
 * @property eventHour 이벤트 발생 시간대 (0-23)
 */
data class Event(
    val key: String,
    val payload: ByteArray,
    val eventTimeMs: Long,
    val eventHour: Int = ((eventTimeMs / 1000 / 3600) % 24).toInt()
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as Event

        if (key != other.key) return false
        if (!payload.contentEquals(other.payload)) return false
        if (eventTimeMs != other.eventTimeMs) return false

        return true
    }

    override fun hashCode(): Int {
        var result = key.hashCode()
        result = 31 * result + payload.contentHashCode()
        result = 31 * result + eventTimeMs.hashCode()
        return result
    }

    override fun toString(): String {
        return "Event(key='$key', eventTimeMs=$eventTimeMs, eventHour=$eventHour, payloadSize=${payload.size})"
    }
}

/**
 * 리플레이 통계
 */
data class ReplayStats(
    var totalEvents: Long = 0,
    var sentEvents: Long = 0,
    var failedEvents: Long = 0,
    var currentChunk: Int = 0,
    var totalChunks: Int = 24,
    var startTimeMs: Long = System.currentTimeMillis(),
    var lastEventTimeMs: Long = 0
) {
    val successRate: Double
        get() = if (sentEvents + failedEvents > 0) {
            sentEvents.toDouble() / (sentEvents + failedEvents) * 100
        } else 0.0

    val elapsedSeconds: Long
        get() = (System.currentTimeMillis() - startTimeMs) / 1000

    val eventsPerSecond: Double
        get() = if (elapsedSeconds > 0) {
            sentEvents.toDouble() / elapsedSeconds
        } else 0.0

    fun summary(): String {
        return """
            |ReplayStats:
            |  Total Events: $totalEvents
            |  Sent: $sentEvents
            |  Failed: $failedEvents
            |  Success Rate: ${"%.2f".format(successRate)}%
            |  Current Chunk: $currentChunk / $totalChunks
            |  Elapsed: ${elapsedSeconds}s
            |  TPS: ${"%.2f".format(eventsPerSecond)}
        """.trimMargin()
    }
}

/**
 * 체크포인트 정보 (장애 복구용)
 */
data class Checkpoint(
    val lastProcessedChunk: Int,
    val lastEventTimeMs: Long,
    val sentCount: Long,
    val failedCount: Long,
    val savedAtMs: Long = System.currentTimeMillis()
)
