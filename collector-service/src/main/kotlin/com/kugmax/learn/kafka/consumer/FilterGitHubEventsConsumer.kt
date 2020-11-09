package com.kugmax.learn.kafka.consumer

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.kugmax.learn.kafka.model.GitHubEvent
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import java.time.Duration
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

class FilterGitHubEventsConsumer(
        val topic: String,
        val consumer: Consumer<String, String>) {

    lateinit var executor: ExecutorService
    private val mapper = jacksonObjectMapper()
    private val filterType = "PushEvent"

    fun start() {
        println("FilterGitHubEventsConsumer start")
        executor = Executors.newSingleThreadExecutor()
        executor.submit { this.run() }
    }

    private fun process(record: ConsumerRecord<String, String>) {

        val event: GitHubEvent = mapper.readValue(record.value())

        if (filterType == event.type) {
            println(record.value())
        }
    }

    private fun run() {
        try {
            consumer.subscribe(listOf(topic))
            while (true) {
                val records: ConsumerRecords<String, String>? = consumer.poll(Duration.ofSeconds(3))
                println("FilterGitHubEventsConsumer poll " + records?.count())
                records?.forEach { process(it) }
            }
        } catch (e: Exception) {
            e.printStackTrace()
        } finally {
            consumer.close()
        }
    }
}