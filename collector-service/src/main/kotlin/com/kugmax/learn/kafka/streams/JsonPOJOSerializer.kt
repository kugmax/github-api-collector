package com.kugmax.learn.kafka.streams

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kugmax.learn.kafka.model.GitHubEvent
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

class JsonPOJOSerializer<T> : Serializer<T> {
    private val objectMapper = ObjectMapper()
    override fun configure(props: Map<String?, *>?, isKey: Boolean) {}

    override fun serialize(topic: String?, data: T?): ByteArray? {
        return if (data == null) null else try {
            objectMapper.writeValueAsBytes(data)
        } catch (e: Exception) {
            throw SerializationException("Error serializing JSON message", e)
        }
    }

    override fun close() {}
}