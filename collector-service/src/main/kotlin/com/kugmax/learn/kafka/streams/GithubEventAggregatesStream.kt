package com.kugmax.learn.kafka.streams

import com.kugmax.learn.kafka.model.GitHubEvent
import com.kugmax.learn.kafka.model.GithubEventAggregates
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.*
import org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.*

class GithubEventAggregatesStream(val fromTopic: String,
                                  val toTopic: String,
                                  val windowSize: Duration,
                                  val props: Properties) {
    fun start() {
        val streams = KafkaStreams(buildTopology(), props)

        println("Starting aggregate streams...")
        streams.start()
        println("Streams aggregate started!")

        Runtime.getRuntime().addShutdownHook(Thread { streams.close() })
    }

    fun buildTopology() : Topology {
        val builder = StreamsBuilder();

        val messages: KStream<String, GitHubEvent> = builder.stream(fromTopic,
                Consumed.with(
                        Serdes.String(),
                        Serdes.serdeFrom(JsonPOJOSerializer<GitHubEvent>(), JsonPOJODeserializer(GitHubEvent::class.java))
                )
        )

        messages
//                .map{ key, value -> KeyValue<String, String>(Instant.parse(key).truncatedTo(ChronoUnit.MINUTES).toString(), value.id) }
                .groupByKey()
                .windowedBy(TimeWindows.of(windowSize).advanceBy(windowSize).grace(windowSize))
                .count()
                .suppress(Suppressed.untilWindowCloses(unbounded()))
                .toStream()
                .map{ key: Windowed<String>, count: Long ->
                    KeyValue(Instant.now().toString(),
                            GithubEventAggregates("PushEvent",
                                    key.window().startTime().toString(),
                                    key.window().endTime().toString(),
                                    count,
                                    Instant.now().toString())
                    )
                }
                .to(toTopic, Produced.with(
                        Serdes.String(),
                        Serdes.serdeFrom(JsonPOJOSerializer<GithubEventAggregates>(), JsonPOJODeserializer(GithubEventAggregates::class.java))
                ) )

        val topology = builder.build()

        println("Filter topology: ${topology.describe()}")

        return topology
    }
}