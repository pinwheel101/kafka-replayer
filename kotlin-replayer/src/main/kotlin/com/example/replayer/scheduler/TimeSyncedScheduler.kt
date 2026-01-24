package com.example.replayer.scheduler

import com.example.replayer.config.ReplayConfig
import com.example.replayer.model.Event
import io.netty.util.HashedWheelTimer
import io.netty.util.Timeout
import mu.KotlinLogging
import java.io.Closeable
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

private val logger = KotlinLogging.logger {}

/**
 * 시간 동기화 스케줄러
 *
 * HashedWheelTimer를 사용하여 밀리초 단위 정밀도로 이벤트를 스케줄링합니다.
 */
class TimeSyncedScheduler(
    private val config: ReplayConfig,
    private val onSend: (Event) -> Unit,
    private val onError: (Event, Throwable) -> Unit
) : Closeable {

    private val timer: HashedWheelTimer = HashedWheelTimer(
        { runnable -> Thread(runnable, "timer-wheel-${System.currentTimeMillis()}") },
        config.timerTickMs,
        TimeUnit.MILLISECONDS,
        config.timerWheelSize
    )

    private val scheduledCount = AtomicLong(0)
    private val completedCount = AtomicLong(0)
    private val failedCount = AtomicLong(0)

    // 글로벌 기준 시간 (청크 간 연속성 유지)
    @Volatile
    private var globalBaseEventTime: Long? = null
    
    @Volatile
    private var globalBaseWallTime: Long? = null

    /**
     * 청크 이벤트들을 시간 동기화하여 스케줄링
     *
     * @param events 스케줄링할 이벤트 목록 (시간순 정렬 필요)
     * @param isFirstChunk 첫 번째 청크 여부 (기준 시간 설정)
     * @return 모든 이벤트 처리 완료를 기다리는 CountDownLatch
     */
    fun scheduleChunk(events: List<Event>, isFirstChunk: Boolean = false): CountDownLatch {
        if (events.isEmpty()) {
            return CountDownLatch(0)
        }

        // 첫 번째 청크에서 기준 시간 설정
        if (isFirstChunk || globalBaseEventTime == null) {
            globalBaseEventTime = events.first().eventTimeMs
            globalBaseWallTime = System.currentTimeMillis()
            logger.info { 
                "Setting base time: eventTime=${globalBaseEventTime}, wallTime=${globalBaseWallTime}" 
            }
        }

        val latch = CountDownLatch(events.size)
        val baseEventTime = globalBaseEventTime!!
        val baseWallTime = globalBaseWallTime!!
        val speedFactor = config.speedFactor

        logger.debug { 
            "Scheduling ${events.size} events, speedFactor=$speedFactor" 
        }

        for (event in events) {
            scheduleEvent(event, baseEventTime, baseWallTime, speedFactor, latch)
        }

        scheduledCount.addAndGet(events.size.toLong())
        
        return latch
    }

    private fun scheduleEvent(
        event: Event,
        baseEventTime: Long,
        baseWallTime: Long,
        speedFactor: Double,
        latch: CountDownLatch
    ) {
        // 상대적 시간 오프셋 계산
        val eventOffset = event.eventTimeMs - baseEventTime
        val adjustedOffset = (eventOffset / speedFactor).toLong()
        val targetSendTime = baseWallTime + adjustedOffset
        val delay = maxOf(0L, targetSendTime - System.currentTimeMillis())

        // 정밀한 타이밍으로 이벤트 스케줄링
        // onSend()는 sendAsync()를 호출하므로 비동기 전송됩니다
        timer.newTimeout({ _: Timeout ->
            try {
                onSend(event)  // sendAsync() - 타이밍만 제어, 실제 전송은 비동기
                completedCount.incrementAndGet()
            } catch (e: Exception) {
                failedCount.incrementAndGet()
                onError(event, e)
            } finally {
                latch.countDown()
            }
        }, delay, TimeUnit.MILLISECONDS)
    }

    /**
     * 스케줄러 통계
     */
    fun getStats(): SchedulerStats {
        return SchedulerStats(
            scheduled = scheduledCount.get(),
            completed = completedCount.get(),
            failed = failedCount.get(),
            pending = scheduledCount.get() - completedCount.get() - failedCount.get()
        )
    }

    /**
     * 기준 시간 리셋 (새로운 리플레이 세션 시작 시)
     */
    fun resetBaseTime() {
        globalBaseEventTime = null
        globalBaseWallTime = null
        logger.info { "Base time reset" }
    }

    override fun close() {
        timer.stop()
        logger.info { "Timer stopped. Final stats: ${getStats()}" }
    }
}

/**
 * 스케줄러 통계
 */
data class SchedulerStats(
    val scheduled: Long,
    val completed: Long,
    val failed: Long,
    val pending: Long
) {
    override fun toString(): String {
        return "SchedulerStats(scheduled=$scheduled, completed=$completed, failed=$failed, pending=$pending)"
    }
}

/**
 * 순차 스케줄러 (시간 동기화 없이 최대 속도로 전송)
 *
 * 비동기 전송을 활용하여 높은 처리량을 제공합니다.
 */
class SequentialScheduler(
    private val config: ReplayConfig,
    private val onSend: (Event) -> Unit,
    private val onError: (Event, Throwable) -> Unit
) {
    private val processedCount = AtomicLong(0)

    /**
     * 이벤트 비동기 전송 (최대 성능)
     *
     * Kafka Producer의 내부 배치 처리를 활용하여
     * 네트워크 왕복 횟수를 최소화합니다.
     *
     * sendAsync()를 연속 호출하여 Producer 버퍼에 쌓고,
     * Producer 내부 스레드가 자동으로 배치 처리하여 전송합니다.
     *
     * 실제 전송 완료는 호출자(KafkaReplayer)가 flush()로 처리합니다.
     */
    fun processEvents(events: List<Event>) {
        logger.info { "Processing ${events.size} events in async mode" }

        // 비동기로 모든 이벤트를 Producer 버퍼에 추가
        for (event in events) {
            try {
                onSend(event)  // sendAsync() 호출 - 즉시 리턴
                processedCount.incrementAndGet()

                // 지연 설정이 있으면 대기 (디버깅/테스트용)
                if (config.delayMs > 0) {
                    Thread.sleep(config.delayMs)
                }
            } catch (e: Exception) {
                onError(event, e)
            }
        }

        logger.debug { "All ${events.size} events submitted to Kafka Producer buffer" }
    }

    /**
     * 통계 조회
     *
     * 참고: 실제 성공/실패 카운트는 KafkaReplayer가 관리합니다.
     * 여기서는 처리 요청한 이벤트 수만 추적합니다.
     */
    fun getStats(): SequentialStats {
        return SequentialStats(
            processed = processedCount.get()
        )
    }
}

data class SequentialStats(
    val processed: Long
) {
    override fun toString(): String {
        return "SequentialStats(processed=$processed events)"
    }
}
