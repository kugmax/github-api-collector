package com.kugmax.learn.kafka.clients

import io.micronaut.context.annotation.Value
import okhttp3.OkHttpClient
import okhttp3.Request
import java.io.IOException
import javax.inject.Singleton
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.kugmax.learn.kafka.resource.GitHubEventResource


@Singleton
class GitHubClientImpl : GitHubClient {
    @Value("\${github.api.events.url}")
    lateinit var url: String

    @Throws(IOException::class)
    override fun getEvents() : List<GitHubEventResource> {
        val client = OkHttpClient()

        val request = Request.Builder()
                .url(url)
                .build();

        val response = client.newCall(request).execute()
        val body = response.body()?.string()

        val mapper = jacksonObjectMapper()
        val events: List<GitHubEventResource>? = body?.let { mapper.readValue(it) }

        return events!!
    }
}