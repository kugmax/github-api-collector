package com.kugmax.learn.kafka.streams

import com.kugmax.learn.kafka.model.GitHubEvent
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.Serdes.StringSerde
import org.apache.kafka.streams.*
import org.apache.kafka.streams.state.KeyValueStore
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.*

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class GitHubEventsFilterStreamTest {

    val testGitHubEventTopic : String = "test-GitHubEvent"
    val testGitHubEventPushTopic : String = "test-GitHubEventPush"

    lateinit var testDriver: TopologyTestDriver
    lateinit var inputTopic: TestInputTopic<String, GitHubEvent>
    lateinit var outputTopic: TestOutputTopic<String, GitHubEvent>
    lateinit var store: KeyValueStore<String, GitHubEvent>

    private val stringSerde: Serde<String> = StringSerde()
    private val longSerde: Serde<GitHubEvent> = Serdes.serdeFrom(JsonPOJOSerializer(), JsonPOJODeserializer(GitHubEvent::class.java))

    @BeforeAll
    fun before() {

        val props = Properties()
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "test-filter-github-event-stream")
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")

        val stream = GitHubEventsFilterStream(testGitHubEventTopic, testGitHubEventPushTopic, "PushEvent", props)
        testDriver = TopologyTestDriver(stream.buildTopology(), props)

        inputTopic = testDriver.createInputTopic(testGitHubEventTopic, stringSerde.serializer(), longSerde.serializer())
        outputTopic = testDriver.createOutputTopic(testGitHubEventPushTopic, stringSerde.deserializer(), longSerde.deserializer())
    }

    @AfterAll
    fun after() {
        testDriver.close()
    }

    @Test
    fun testFilter() {
        inputTopic.pipeInput("1", GitHubEvent("1", "event-1", "a1", "url1", "r1", "url2", "time1"))
        inputTopic.pipeInput("2", GitHubEvent("2", "event-2", "a2", "url2", "r2", "url2", "time2"))

        inputTopic.pipeInput("3", GitHubEvent("3", "PushEvent", "a3", "url3", "r3", "url3", "time3"))
        inputTopic.pipeInput("4", GitHubEvent("4", "PushEvent", "a4", "url4", "r4", "url4", "time4"))

        assertThat(outputTopic.readKeyValuesToList()).contains(
                KeyValue("PushEvent", GitHubEvent("3", "PushEvent", "a3", "url3", "r3", "url3", "time3")),
                KeyValue("PushEvent", GitHubEvent("4", "PushEvent", "a4", "url4", "r4", "url4", "time4"))
        )
    }
}