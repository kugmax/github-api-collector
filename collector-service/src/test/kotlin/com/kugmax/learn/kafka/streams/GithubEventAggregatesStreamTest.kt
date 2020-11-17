package com.kugmax.learn.kafka.streams

import com.kugmax.learn.kafka.model.GitHubEvent
import com.kugmax.learn.kafka.model.GithubEventAggregates
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.Serdes.StringSerde
import org.apache.kafka.streams.*
import org.apache.kafka.streams.processor.WallclockTimestampExtractor
import org.apache.kafka.streams.state.KeyValueStore
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.time.Duration
import java.time.Instant
import java.util.*

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class GithubEventAggregatesStreamTest {

    val testGitHubEventTopic : String = "test-GitHubEvent"
    val testGitHubEventPushTopic : String = "test-GithubEventAggregates"

    lateinit var testDriver: TopologyTestDriver
    lateinit var inputTopic: TestInputTopic<String, GitHubEvent>
    lateinit var outputTopic: TestOutputTopic<String, GithubEventAggregates>
    lateinit var store: KeyValueStore<String, GitHubEvent>

    private val stringSerde: Serde<String> = StringSerde()
    private val eventSerde: Serde<GitHubEvent> = Serdes.serdeFrom(JsonPOJOSerializer(), JsonPOJODeserializer(GitHubEvent::class.java))
    private val aggregateSerde: Serde<GithubEventAggregates> = Serdes.serdeFrom(JsonPOJOSerializer(), JsonPOJODeserializer(GithubEventAggregates::class.java))

    @BeforeAll
    fun before() {

        val props = Properties()
        props[StreamsConfig.APPLICATION_ID_CONFIG] = "test-filter-github-event-stream-aggregate"
        props[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
        props[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String()::class.java;
        props[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = Serdes.String()::class.java;
        props[StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG] = WallclockTimestampExtractor::class.java

        val stream = GithubEventAggregatesStream(testGitHubEventTopic, testGitHubEventPushTopic, Duration.ofSeconds(5), props)
        testDriver = TopologyTestDriver(stream.buildTopology(), props)

        inputTopic = testDriver.createInputTopic(testGitHubEventTopic, stringSerde.serializer(), eventSerde.serializer())
        outputTopic = testDriver.createOutputTopic(testGitHubEventPushTopic, stringSerde.deserializer(), aggregateSerde.deserializer())
    }

    @AfterAll
    fun after() {
        testDriver.close()
    }

    @Test
    fun testFilter() {
        val window1 = Instant.now().toString()
        val window2 = Instant.now().plus(Duration.ofMinutes(2)).toString()

//        inputTopic.pipeInput(window1, GitHubEvent("1", "PushEvent", "a1", "url1", "r1", "url2", "time1"))
//        Thread.sleep(10_000)

//        inputTopic.pipeInput(window1, GitHubEvent("2", "PushEvent", "a2", "url2", "r2", "url2", "time2"))
//        inputTopic.pipeInput(window1, GitHubEvent("3", "PushEvent", "a3", "url3", "r3", "url3", "time3"))

        inputTopic.pipeInput("k1", GitHubEvent("1", "PushEvent", "a1", "url1", "r1", "url2", "time1"))
        inputTopic.pipeInput("k2", GitHubEvent("2", "PushEvent", "a2", "url2", "r2", "url2", "time2"))
        inputTopic.pipeInput("k3", GitHubEvent("3", "PushEvent", "a3", "url3", "r3", "url3", "time3"))

                Thread.sleep(10_000)

//        inputTopic.advanceTime(Duration.ofMinutes(2))
//        Thread.sleep(10_000)

//        inputTopic.pipeInput(window2, GitHubEvent("4", "PushEvent", "a4", "url4", "r4", "url4", "time4"))

        val values = outputTopic.readKeyValuesToMap().values
        values.forEach { println(it) }

        assertEquals(3, outputTopic.readValue().count)
        assertTrue(outputTopic.isEmpty)

//        inputTopic.advanceTime(Duration.ofMinutes(1))
//        inputTopic.pipeInput("k4", GitHubEvent("4", "PushEvent", "a4", "url4", "r4", "url4", "time4"))
//
//        assertEquals(1, outputTopic.readValue().count)
//        assertTrue(outputTopic.isEmpty)

//        val values = outputTopic.readKeyValuesToMap().values
//        values.forEach { println(it) }
//
//        assertEquals(3, outputTopic.readValue().count)
//        assertEquals(1, outputTopic.readValue().count)

//                .contains(
//                KeyValue("3", GithubEventAggregates("PushEvent", "start", "end", 4, "now")),
//                KeyValue("4", GitHubEvent("4", "PushEvent", "a4", "url4", "r4", "url4", "time4"))
//        )
    }
}