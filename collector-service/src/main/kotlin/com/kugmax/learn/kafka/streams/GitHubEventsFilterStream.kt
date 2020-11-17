package com.kugmax.learn.kafka.streams

import com.kugmax.learn.kafka.model.GitHubEvent
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.KStream
import java.util.*


class GitHubEventsFilterStream(
        val fromTopic: String,
        val toTopic: String,
        val filterType: String,
        val props: Properties) {

    fun start() {
        val builder = StreamsBuilder();

        val messages: KStream<String, GitHubEvent> = builder.stream(fromTopic,
                Consumed.with(
                        Serdes.String(),
                        Serdes.serdeFrom(JsonPOJOSerializer(), JsonPOJODeserializer(GitHubEvent::class.java))
                )
        )

        messages
                .filter{ _, event -> filterType == event.type}
                .to(toTopic)

        val topology = builder.build()

        println("Filter topology: ${topology.describe()}")

        val streams = KafkaStreams(topology, props)

        println("Starting streams...")
        streams.start()
        println("Streams started!")

        Runtime.getRuntime().addShutdownHook(Thread { streams.close() })
    }
}