package com.kugmax.learn.kafka.producer

import org.apache.kafka.clients.producer.Producer
import javax.inject.Inject
import javax.inject.Singleton

@Singleton
class GitHubEventProducer {
    @Inject
    lateinit var producer: Producer<String, String>
}