package com.kugmax.learn.github.events.connect.sink

import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.Importance
import org.apache.kafka.common.config.ConfigDef.Type

class FileSinkConfig (props: Map<String, String>) : AbstractConfig(config, props) {
    companion object {

        const val FILE_NAME = "github.events.file.name"
        private const val FILE_NAME_DOC = "File name to store events from kafka topic"

        var config = ConfigDef()
            .define(
                FILE_NAME,
                Type.STRING,
                Importance.HIGH,
                FILE_NAME_DOC
            )!!
    }
}