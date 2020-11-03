package com.kugmax.learn.kafka.service

import com.kugmax.learn.kafka.clients.GitHubClient
import com.kugmax.learn.kafka.producer.GitHubEventProducer
import com.kugmax.learn.kafka.producer.GitHubEventProducerImpl
import javax.inject.Inject
import javax.inject.Singleton

@Singleton
class GitHubEventsCollectorServiceImpl : GitHubEventsCollector {

    @Inject
    lateinit var producer: GitHubEventProducer

    @Inject
    lateinit var client: GitHubClient

    override fun collectEvents() {
        val events = client.getEvents()
//        println(events)
        producer.push(events)
    }
}