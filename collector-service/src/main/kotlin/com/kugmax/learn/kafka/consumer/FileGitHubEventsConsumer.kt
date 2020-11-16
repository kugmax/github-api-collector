package com.kugmax.learn.kafka.consumer

import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import java.io.File
import java.time.Duration
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

class FileGitHubEventsConsumer(
        val topic: String,
        val getAllConsumer: Consumer<String, String>,
        val fileName: String) {

    lateinit var executor: ExecutorService
    lateinit var file: File

    fun start() {
        println("FileGitHubEventsConsumer start")
        file = File(fileName)

        executor = Executors.newSingleThreadExecutor()
        executor.submit { this.run() }
    }

    private fun process(record: ConsumerRecord<String, String>) {
        file.appendText(record.value() + "\n")
    }

    private fun run() {
        try {
            getAllConsumer.subscribe(listOf(topic))
            while (true) {
                val records: ConsumerRecords<String, String>? = getAllConsumer.poll(Duration.ofSeconds(3))
//                println("FileGitHubEventsConsumer poll " + records?.count())
                records?.forEach { process(it) }
            }
        } catch (e: Exception) {
            e.printStackTrace()
        } finally {
            getAllConsumer.close()
        }
    }
}