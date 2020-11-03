package com.kugmax.learn.kafka.resource

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonProperty

@JsonIgnoreProperties(ignoreUnknown = true)
data class GitHubEventResource(
        val id: String,
        val type: String,
        val actor: GitHubActorResource,
        val repo: GitHubRepoResource,
        @JsonProperty("created_at")  val createdAt: String,
)
