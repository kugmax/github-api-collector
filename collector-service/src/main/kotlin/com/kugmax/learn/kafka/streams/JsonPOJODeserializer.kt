package com.kugmax.learn.kafka.streams

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.kugmax.learn.kafka.model.GitHubEvent
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization.Deserializer


class JsonPOJODeserializer<T>(private var tClass: Class<T>) : Deserializer<T>{
    private val objectMapper = jacksonObjectMapper()

    override fun configure(props: Map<String?, *>, isKey: Boolean) {}

    override fun deserialize(topic: String, bytes: ByteArray): T {
        val data: T
        data = try {
            objectMapper.readValue(bytes, tClass)
        } catch (e: Exception) {
            throw SerializationException(e)
        }
        return data
    }

    override fun close() {}
}
