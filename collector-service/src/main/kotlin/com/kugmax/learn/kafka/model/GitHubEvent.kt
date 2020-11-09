package com.kugmax.learn.kafka.model

import com.fasterxml.jackson.annotation.JsonIgnoreProperties

@JsonIgnoreProperties(ignoreUnknown = true)
data class GitHubEvent (
        val id: String,
        val type: String,
        val actorId: String,
        val actorUrl: String,
        val repoId: String,
        val repoUrl: String,
        val createdAt: String,
)