package com.kugmax.learn.kafka.producer

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.kugmax.learn.kafka.model.GitHubEvent
import io.micronaut.context.annotation.Value
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import java.time.Instant
import javax.inject.Inject
import javax.inject.Singleton

@Singleton
class GitHubEventProducerImpl : GitHubEventProducer {
    @Inject
    lateinit var producer: Producer<String, String>

    @Value("\${kafka.topic.github.events}")
    lateinit var topic: String

    override fun push(events: List<GitHubEvent>) {
        val mapper = jacksonObjectMapper()

        events.forEach{
            producer.send(ProducerRecord(topic, Instant.now().toString(), mapper.writeValueAsString(it)))
        }
    }
}