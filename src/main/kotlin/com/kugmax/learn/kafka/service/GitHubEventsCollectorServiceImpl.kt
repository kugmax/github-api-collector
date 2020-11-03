package com.kugmax.learn.kafka.service

import com.kugmax.learn.kafka.clients.GitHubClient
import com.kugmax.learn.kafka.model.GitHubEvent
import com.kugmax.learn.kafka.producer.GitHubEventProducer
import javax.inject.Inject
import javax.inject.Singleton

@Singleton
class GitHubEventsCollectorServiceImpl : GitHubEventsCollector {

    @Inject
    lateinit var producer: GitHubEventProducer

    @Inject
    lateinit var client: GitHubClient

    override fun collectEvents() {
        val eventsResource = client.getEvents()
        println(eventsResource)

        val eventsModel = eventsResource.map { GitHubEvent(it.id, it.type, it.actor.id, it.actor.url, it.repo.id, it.repo.url, it.createdAt) }

        producer.push(eventsModel)
    }
}