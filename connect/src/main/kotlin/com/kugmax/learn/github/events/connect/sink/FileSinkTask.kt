package com.kugmax.learn.github.events.connect.sink

import com.kugmax.learn.github.events.connect.sink.FileSinkConfig.Companion.FILE_NAME
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.sink.SinkTask
import java.io.File
import org.apache.kafka.connect.data.Struct

class FileSinkTask: SinkTask() {
    lateinit var fileName: String
    lateinit var file: File

    override fun version() = FileSinkConnector().version()

    override fun start(props: MutableMap<String, String>?) {
        fileName = props?.get(FILE_NAME)!!
        file = File(fileName)
    }

    override fun stop() {
        TODO("Not yet implemented")
    }

    override fun put(records: MutableCollection<SinkRecord>) {
        for (record in records) {
            println("record.value() " + record.value())

            val struct = record.value() as Struct

            println("struct $struct")
        }

//        records.forEach {  file.appendText(it.value() as String + "\n") }


    }
}