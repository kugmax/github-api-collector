package com.kugmax.learn.github.events.connect.sink

import com.kugmax.learn.github.events.connect.sink.FileSinkConfig.Companion.FILE_NAME
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.sink.SinkTask
import java.io.File

class FileSinkTask: SinkTask() {
    lateinit var fileName: String
    lateinit var file: File

    override fun version() = FileSinkConnector().version()

    override fun start(props: MutableMap<String, String>?) {
        fileName = props?.get(FILE_NAME)!!
        file = File(fileName)

        print("File: " + file.absolutePath)
    }

    override fun stop() {

    }

    override fun put(records: MutableCollection<SinkRecord>) {
        records.forEach {  file.appendText(it.value() as String + "\n") }
    }
}