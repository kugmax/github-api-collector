package com.kugmax.learn.kafka.clients

import com.kugmax.learn.kafka.model.GitHubEvent

interface GitHubClient {
    fun getEvents() : List<GitHubEvent>
}