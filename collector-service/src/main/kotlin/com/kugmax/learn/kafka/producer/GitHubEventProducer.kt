package com.kugmax.learn.kafka.producer

import com.kugmax.learn.kafka.model.GitHubEvent

interface GitHubEventProducer {
    fun push(events: List<GitHubEvent>)
}