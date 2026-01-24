package com.example.replayer.reader

import com.example.replayer.config.HdfsConfig
import com.example.replayer.model.Event
import mu.KotlinLogging
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetReader
import org.apache.parquet.hadoop.ParquetReader
import java.io.Closeable
import java.nio.ByteBuffer

private val logger = KotlinLogging.logger {}

/**
 * Parquet 청크 파일 리더
 *
 * Spark에서 준비한 시간별 파티션된 Parquet 파일을 읽습니다.
 * 
 * 디렉토리 구조:
 * /replay/prepared/2021-01-02/
 * ├── event_hour=0/
 * │   ├── part-00000.parquet
 * │   └── part-00001.parquet
 * ├── event_hour=1/
 * │   └── ...
 * └── event_hour=23/
 */
class ParquetChunkReader(
    private val config: HdfsConfig,
    private val basePath: String,
    private val eventKeyColumn: String = "event_key",
    private val eventTimeColumn: String = "event_time",
    private val payloadColumn: String = "payload"
) : Closeable {

    private val hadoopConf: Configuration = createHadoopConfig()
    private val fs: FileSystem = FileSystem.get(hadoopConf)

    private fun createHadoopConfig(): Configuration {
        return Configuration().apply {
            set("fs.defaultFS", config.uri)
            
            // Kerberos 설정
            if (config.kerberosEnabled) {
                set("hadoop.security.authentication", "kerberos")
                set("hadoop.security.authorization", "true")
            }
            
            // 성능 최적화
            set("dfs.client.read.shortcircuit", "false")
            set("io.file.buffer.size", "131072")  // 128KB
        }
    }

    /**
     * 사용 가능한 시간대(hour) 목록 조회
     */
    fun getAvailableHours(): List<Int> {
        val path = Path(basePath)
        
        if (!fs.exists(path)) {
            logger.warn { "Base path does not exist: $basePath" }
            return emptyList()
        }

        return fs.listStatus(path)
            .filter { it.isDirectory }
            .mapNotNull { status ->
                // event_hour=0 형식에서 숫자 추출
                val dirName = status.path.name
                if (dirName.startsWith("event_hour=")) {
                    dirName.substringAfter("event_hour=").toIntOrNull()
                } else {
                    null
                }
            }
            .sorted()
    }

    /**
     * 특정 시간대 청크의 이벤트 수 조회 (미리보기용)
     */
    fun getChunkEventCount(hour: Int): Long {
        val chunkPath = Path("$basePath/event_hour=$hour")
        
        if (!fs.exists(chunkPath)) {
            return 0
        }

        var count = 0L
        getParquetFiles(chunkPath).forEach { file ->
            count += countEventsInFile(file)
        }
        return count
    }

    /**
     * 특정 시간대의 이벤트 로드
     */
    fun readChunk(hour: Int): List<Event> {
        val chunkPath = Path("$basePath/event_hour=$hour")
        
        if (!fs.exists(chunkPath)) {
            logger.warn { "Chunk path does not exist: $chunkPath" }
            return emptyList()
        }

        logger.info { "Reading chunk for hour $hour from $chunkPath" }
        
        val events = mutableListOf<Event>()
        val parquetFiles = getParquetFiles(chunkPath)
        
        logger.debug { "Found ${parquetFiles.size} parquet files in chunk" }

        for (file in parquetFiles) {
            val fileEvents = readParquetFile(file, hour)
            events.addAll(fileEvents)
            logger.debug { "Read ${fileEvents.size} events from ${file.name}" }
        }

        // 시간순 정렬 (이미 정렬되어 있지만 확인 차원)
        events.sortBy { it.eventTimeMs }
        
        logger.info { "Loaded ${events.size} events for hour $hour" }
        return events
    }

    /**
     * 스트리밍 방식으로 청크 읽기 (메모리 효율적)
     */
    fun streamChunk(hour: Int, onEvent: (Event) -> Unit) {
        val chunkPath = Path("$basePath/event_hour=$hour")
        
        if (!fs.exists(chunkPath)) {
            logger.warn { "Chunk path does not exist: $chunkPath" }
            return
        }

        val parquetFiles = getParquetFiles(chunkPath)
        
        for (file in parquetFiles) {
            streamParquetFile(file, hour, onEvent)
        }
    }

    /**
     * 모든 청크를 순차적으로 처리
     */
    fun processAllChunks(onChunk: (hour: Int, events: List<Event>) -> Unit) {
        val hours = getAvailableHours()
        logger.info { "Found ${hours.size} chunks: $hours" }

        for (hour in hours) {
            val events = readChunk(hour)
            if (events.isNotEmpty()) {
                onChunk(hour, events)
            }
        }
    }

    private fun getParquetFiles(dirPath: Path): List<Path> {
        return fs.listStatus(dirPath)
            .filter { !it.isDirectory && it.path.name.endsWith(".parquet") }
            .map { it.path }
            .sortedBy { it.name }
    }

    private fun readParquetFile(filePath: Path, hour: Int): List<Event> {
        val events = mutableListOf<Event>()
        
        val reader: ParquetReader<GenericRecord> = AvroParquetReader
            .builder<GenericRecord>(filePath)
            .withConf(hadoopConf)
            .build()

        try {
            var record: GenericRecord? = reader.read()
            while (record != null) {
                val event = parseRecord(record, hour)
                if (event != null) {
                    events.add(event)
                }
                record = reader.read()
            }
        } finally {
            reader.close()
        }

        return events
    }

    private fun streamParquetFile(filePath: Path, hour: Int, onEvent: (Event) -> Unit) {
        val reader: ParquetReader<GenericRecord> = AvroParquetReader
            .builder<GenericRecord>(filePath)
            .withConf(hadoopConf)
            .build()

        try {
            var record: GenericRecord? = reader.read()
            while (record != null) {
                val event = parseRecord(record, hour)
                if (event != null) {
                    onEvent(event)
                }
                record = reader.read()
            }
        } finally {
            reader.close()
        }
    }

    private fun parseRecord(record: GenericRecord, hour: Int): Event? {
        return try {
            val key = record.get(eventKeyColumn)?.toString() ?: return null
            val eventTime = when (val time = record.get(eventTimeColumn)) {
                is Long -> time
                is Int -> time.toLong()
                else -> return null
            }
            
            val payload = when (val p = record.get(payloadColumn)) {
                is ByteBuffer -> {
                    val bytes = ByteArray(p.remaining())
                    p.get(bytes)
                    bytes
                }
                is ByteArray -> p
                is String -> p.toByteArray()
                null -> ByteArray(0)
                else -> p.toString().toByteArray()
            }

            Event(
                key = key,
                payload = payload,
                eventTimeMs = eventTime,
                eventHour = hour
            )
        } catch (e: Exception) {
            logger.warn(e) { "Failed to parse record: $record" }
            null
        }
    }

    private fun countEventsInFile(filePath: Path): Long {
        var count = 0L
        val reader: ParquetReader<GenericRecord> = AvroParquetReader
            .builder<GenericRecord>(filePath)
            .withConf(hadoopConf)
            .build()

        try {
            while (reader.read() != null) {
                count++
            }
        } finally {
            reader.close()
        }
        return count
    }

    /**
     * 전체 통계 조회
     */
    fun getStats(): ChunkStats {
        val hours = getAvailableHours()
        var totalEvents = 0L
        val hourlyStats = mutableMapOf<Int, Long>()

        for (hour in hours) {
            val count = getChunkEventCount(hour)
            hourlyStats[hour] = count
            totalEvents += count
        }

        return ChunkStats(
            basePath = basePath,
            totalChunks = hours.size,
            totalEvents = totalEvents,
            hourlyEventCounts = hourlyStats
        )
    }

    override fun close() {
        fs.close()
    }
}

/**
 * 청크 통계 정보
 */
data class ChunkStats(
    val basePath: String,
    val totalChunks: Int,
    val totalEvents: Long,
    val hourlyEventCounts: Map<Int, Long>
) {
    override fun toString(): String {
        val hourlyStr = hourlyEventCounts.entries
            .sortedBy { it.key }
            .joinToString("\n") { (hour, count) ->
                "    Hour %02d: %,d events".format(hour, count)
            }
        
        return """
            |ChunkStats:
            |  Base Path: $basePath
            |  Total Chunks: $totalChunks
            |  Total Events: %,d
            |  Hourly Distribution:
            |$hourlyStr
        """.trimMargin().format(totalEvents)
    }
}
