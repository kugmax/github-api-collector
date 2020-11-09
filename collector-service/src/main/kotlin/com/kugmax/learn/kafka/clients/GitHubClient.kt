package com.kugmax.learn.kafka.clients

import com.kugmax.learn.kafka.resource.GitHubEventResource

interface GitHubClient {
    fun getEvents() : List<GitHubEventResource>
}