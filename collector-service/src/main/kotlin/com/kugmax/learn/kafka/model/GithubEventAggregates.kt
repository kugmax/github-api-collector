package com.kugmax.learn.kafka.model

import com.fasterxml.jackson.annotation.JsonIgnoreProperties

@JsonIgnoreProperties(ignoreUnknown = true)
data class GithubEventAggregates (
        val eventType: String,
        val startTimestamp: String,
        val endTimestamp: String,
        val count: Long,
        val createdAt: String

)