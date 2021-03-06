package com.kugmax.learn.kafka.resource

import com.fasterxml.jackson.annotation.JsonIgnoreProperties

@JsonIgnoreProperties(ignoreUnknown = true)
data class GitHubActorResource(
        val id: String,
        val url: String
)