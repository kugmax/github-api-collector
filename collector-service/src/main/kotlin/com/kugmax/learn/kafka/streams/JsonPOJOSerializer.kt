package com.kugmax.learn.kafka.streams

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kugmax.learn.kafka.model.GitHubEvent
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

class JsonPOJOSerializer : Serializer<GitHubEvent> {
    private val objectMapper = ObjectMapper()
    override fun configure(props: Map<String?, *>?, isKey: Boolean) {}

    override fun serialize(topic: String?, data: GitHubEvent?): ByteArray? {
        return if (data == null) null else try {
            objectMapper.writeValueAsBytes(data)
        } catch (e: Exception) {
            throw SerializationException("Error serializing JSON message", e)
        }
    }

    override fun close() {}
}