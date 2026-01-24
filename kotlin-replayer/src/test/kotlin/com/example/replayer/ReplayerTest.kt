package com.example.replayer

import com.example.replayer.config.ReplayConfig
import com.example.replayer.config.ReplayMode
import com.example.replayer.model.Event
import com.example.replayer.scheduler.TimeSyncedScheduler
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.math.abs

class TimeSyncedSchedulerTest {

    @Test
    fun `test events are sent in order`() {
        val sentEvents = CopyOnWriteArrayList<Event>()
        val config = ReplayConfig(
            mode = ReplayMode.TIME_SYNCED,
            speedFactor = 100.0,  // 100배속으로 빠르게 테스트
            timerTickMs = 1,
            timerWheelSize = 512
        )

        val scheduler = TimeSyncedScheduler(
            config = config,
            onSend = { event -> sentEvents.add(event) },
            onError = { _, _ -> }
        )

        // 테스트 이벤트 생성 (100ms 간격)
        val baseTime = System.currentTimeMillis()
        val events = listOf(
            Event("key1", ByteArray(0), baseTime),
            Event("key2", ByteArray(0), baseTime + 100),
            Event("key3", ByteArray(0), baseTime + 200),
            Event("key4", ByteArray(0), baseTime + 300),
            Event("key5", ByteArray(0), baseTime + 400)
        )

        val latch = scheduler.scheduleChunk(events, isFirstChunk = true)
        
        // 완료 대기
        assertTrue(latch.await(10, TimeUnit.SECONDS), "Timeout waiting for events")

        // 순서 검증
        assertEquals(5, sentEvents.size)
        assertEquals("key1", sentEvents[0].key)
        assertEquals("key2", sentEvents[1].key)
        assertEquals("key3", sentEvents[2].key)
        assertEquals("key4", sentEvents[3].key)
        assertEquals("key5", sentEvents[4].key)

        scheduler.close()
    }

    @Test
    fun `test timing precision`() {
        val sendTimes = CopyOnWriteArrayList<Long>()
        val config = ReplayConfig(
            mode = ReplayMode.TIME_SYNCED,
            speedFactor = 1.0,
            timerTickMs = 1,
            timerWheelSize = 512
        )

        val scheduler = TimeSyncedScheduler(
            config = config,
            onSend = { _ -> sendTimes.add(System.currentTimeMillis()) },
            onError = { _, _ -> }
        )

        // 50ms 간격 이벤트
        val baseTime = System.currentTimeMillis()
        val events = listOf(
            Event("key1", ByteArray(0), baseTime),
            Event("key2", ByteArray(0), baseTime + 50),
            Event("key3", ByteArray(0), baseTime + 100)
        )

        val latch = scheduler.scheduleChunk(events, isFirstChunk = true)
        assertTrue(latch.await(5, TimeUnit.SECONDS))

        // 타이밍 검증 (10ms 오차 허용)
        assertEquals(3, sendTimes.size)
        
        val interval1 = sendTimes[1] - sendTimes[0]
        val interval2 = sendTimes[2] - sendTimes[1]
        
        assertTrue(abs(interval1 - 50) <= 10, "First interval: $interval1 (expected ~50ms)")
        assertTrue(abs(interval2 - 50) <= 10, "Second interval: $interval2 (expected ~50ms)")

        scheduler.close()
    }

    @Test
    fun `test speed factor`() {
        val sendTimes = CopyOnWriteArrayList<Long>()
        val config = ReplayConfig(
            mode = ReplayMode.TIME_SYNCED,
            speedFactor = 2.0,  // 2배속
            timerTickMs = 1,
            timerWheelSize = 512
        )

        val scheduler = TimeSyncedScheduler(
            config = config,
            onSend = { _ -> sendTimes.add(System.currentTimeMillis()) },
            onError = { _, _ -> }
        )

        // 100ms 간격 이벤트 (2배속이면 50ms 간격으로 전송)
        val baseTime = System.currentTimeMillis()
        val events = listOf(
            Event("key1", ByteArray(0), baseTime),
            Event("key2", ByteArray(0), baseTime + 100)
        )

        val latch = scheduler.scheduleChunk(events, isFirstChunk = true)
        assertTrue(latch.await(5, TimeUnit.SECONDS))

        val interval = sendTimes[1] - sendTimes[0]
        
        // 2배속이므로 ~50ms 예상 (10ms 오차 허용)
        assertTrue(abs(interval - 50) <= 15, "Interval: $interval (expected ~50ms at 2x speed)")

        scheduler.close()
    }

    @Test
    fun `test empty events`() {
        val config = ReplayConfig()
        val scheduler = TimeSyncedScheduler(
            config = config,
            onSend = { },
            onError = { _, _ -> }
        )

        val latch = scheduler.scheduleChunk(emptyList(), isFirstChunk = true)
        assertEquals(0, latch.count)

        scheduler.close()
    }
}

class EventTest {

    @Test
    fun `test event creation`() {
        val event = Event(
            key = "test-key",
            payload = "test-payload".toByteArray(),
            eventTimeMs = 1609459200000  // 2021-01-01 00:00:00 UTC
        )

        assertEquals("test-key", event.key)
        assertEquals("test-payload", String(event.payload))
        assertEquals(1609459200000, event.eventTimeMs)
    }

    @Test
    fun `test event hour calculation`() {
        // 2021-01-01 13:30:00 UTC
        val event = Event(
            key = "test",
            payload = ByteArray(0),
            eventTimeMs = 1609507800000
        )

        assertEquals(13, event.eventHour)
    }

    @Test
    fun `test event equality`() {
        val event1 = Event("key", "payload".toByteArray(), 1000)
        val event2 = Event("key", "payload".toByteArray(), 1000)
        val event3 = Event("key", "different".toByteArray(), 1000)

        assertEquals(event1, event2)
        assertNotEquals(event1, event3)
    }
}
