package com.kugmax.learn.kafka.streams

import com.kugmax.learn.kafka.model.GitHubEvent
import com.kugmax.learn.kafka.model.GithubEventAggregates
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.Serdes.StringSerde
import org.apache.kafka.streams.*
import org.apache.kafka.streams.state.KeyValueStore
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
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

    val windowSize = Duration.ofSeconds(5)

    @BeforeAll
    fun before() {

        val props = Properties()
        props[StreamsConfig.APPLICATION_ID_CONFIG] = "test-filter-github-event-stream-aggregate"
        props[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
        props[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String()::class.java;
        props[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = Serdes.String()::class.java;
//        TODO: test advanceTime or advanceWallClockTime or both doesn't work with it :))))
//        props[StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG] = WallclockTimestampExtractor::class.java

        val stream = GithubEventAggregatesStream(testGitHubEventTopic, testGitHubEventPushTopic, windowSize, props)
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
        val window1 = Instant.now()
        val window2 = window1.plus(windowSize).plusSeconds(1)
        val window3 = window2.plus(windowSize).plusSeconds(1)
        val window4 = window3.plus(windowSize).plusSeconds(1)

        inputTopic.pipeInput("PushEvent", GitHubEvent("1", "PushEvent", "a1", "url1", "r1", "url2", "time1"), window1)
        inputTopic.pipeInput("PushEvent", GitHubEvent("2", "PushEvent", "a2", "url2", "r2", "url2", "time2"), window1)
        inputTopic.pipeInput("PushEvent", GitHubEvent("3", "PushEvent", "a3", "url3", "r3", "url3", "time3"), window1)

        testDriver.advanceWallClockTime(windowSize)
        inputTopic.advanceTime(windowSize)

        inputTopic.pipeInput("PushEvent", GitHubEvent("4", "PushEvent", "a4", "url4", "r4", "url4", "time4"), window2)
        inputTopic.pipeInput("PushEvent", GitHubEvent("4", "PushEvent", "a4", "url4", "r4", "url4", "time4"), window2)

        testDriver.advanceWallClockTime(windowSize)
        inputTopic.advanceTime(windowSize)

        inputTopic.pipeInput("PushEvent", GitHubEvent("5", "PushEvent", "a5", "url5", "r5", "url5", "time5"), window3)

//        TODO: to show previous window needs to push one more event :)))))
        inputTopic.pipeInput("PushEvent", GitHubEvent("6", "PushEvent", "a6", "url6", "r6", "url6", "time6"), window4)

        val values = outputTopic.readKeyValuesToMap().values
        println("Output topic: ")
        values.forEach { println(it) }

        assertEquals(3, values.size)
        assertThat(values.map {it.count }).contains(3, 2, 1)
    }
}