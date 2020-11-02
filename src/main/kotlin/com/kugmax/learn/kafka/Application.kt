package com.kugmax.learn.kafka

import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Value
import io.micronaut.runtime.Micronaut.build
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerConfig
import java.util.*
import javax.inject.Singleton

fun main(args: Array<String>) {
	build()
	    .args(*args)
		.packages("com.kugmax.learn.kafka")
		.start()
}

@Factory
internal class KafkaFactory {
	@Value("\${kafka.bootstrap.servers}")
	var kafkaBootstrap: String = "localhost:8080"

	@Singleton
	fun kafkaProducer() : Producer<String, String> {
		val props = Properties()
		props[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = kafkaBootstrap
		props[ProducerConfig.ACKS_CONFIG] = "all"
		props[ProducerConfig.RETRIES_CONFIG] = 0
		props[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = "org.apache.kafka.common.serialization.StringSerializer"
		props[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = "org.apache.kafka.common.serialization.StringSerializer"

		return KafkaProducer(props)
	}
}

