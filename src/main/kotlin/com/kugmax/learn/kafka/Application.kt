package com.kugmax.learn.kafka

import com.kugmax.learn.kafka.consumer.FileGitHubEventsConsumer
import com.kugmax.learn.kafka.consumer.FilterGitHubEventsConsumer
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Value
import io.micronaut.runtime.Micronaut.build
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerConfig
import java.util.*
import javax.inject.Singleton

fun main(args: Array<String>) {
	build()
			.args(*args)
			.eagerInitSingletons(true)
			.packages("com.kugmax.learn.kafka")
			.start()
}

@Factory
internal class KafkaFactory {
	@Value("\${kafka.bootstrap.servers}")
	var kafkaBootstrap: String = "localhost:8080"

	@Value("\${kafka.consumer.group.github.events.getall}")
	lateinit var getAllConsumerGroup: String

	@Value("\${kafka.consumer.group.github.events.filter}")
	lateinit var filterConsumerGroup: String

	@Value("\${kafka.topic.github.events}")
	lateinit var topic: String

	@Value("\${github.events.file}")
	lateinit var fileName: String

	@Singleton
	fun kafkaProducer() : Producer<String, String> {
		val props = getBaseProps()
		props[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = "org.apache.kafka.common.serialization.StringSerializer"
		props[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = "org.apache.kafka.common.serialization.StringSerializer"

		return KafkaProducer(props)
	}

	@Singleton
	fun getAllConsumer() : FileGitHubEventsConsumer {
		val props = getBaseProps()
		props[ConsumerConfig.GROUP_ID_CONFIG] = getAllConsumerGroup
		props[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = "org.apache.kafka.common.serialization.StringDeserializer"
		props[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = "org.apache.kafka.common.serialization.StringDeserializer"

		val consumer = FileGitHubEventsConsumer(topic, KafkaConsumer(props), fileName)
		consumer.start()

		return consumer
	}

	@Singleton
	fun filterConsumer() : FilterGitHubEventsConsumer {
		val props = getBaseProps()
		props[ConsumerConfig.GROUP_ID_CONFIG] = filterConsumerGroup
		props[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = "org.apache.kafka.common.serialization.StringDeserializer"
		props[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = "org.apache.kafka.common.serialization.StringDeserializer"

		val consumer = FilterGitHubEventsConsumer(topic, KafkaConsumer(props))
		consumer.start()

		return consumer
	}

	private fun getBaseProps() : Properties {
		val props = Properties()
		props[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = kafkaBootstrap
		props[ProducerConfig.ACKS_CONFIG] = "all"
		props[ProducerConfig.RETRIES_CONFIG] = 0

		return props
	}
}

