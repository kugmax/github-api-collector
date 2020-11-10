package com.kugmax.learn.github.events.connect.sink
import com.kugmax.learn.github.events.connect.sink.FileSinkConfig.Companion.FILE_NAME
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.sink.SinkConnector
import org.apache.kafka.common.utils.AppInfoParser

class FileSinkConnector : SinkConnector() {
    private var fileName = ""

    override fun version(): String = AppInfoParser.getVersion()
    override fun taskClass(): Class<out Task> = FileSinkTask::class.java
    override fun config(): ConfigDef = FileSinkConfig.config

    override fun taskConfigs(maxTasks: Int): MutableList<MutableMap<String, String>> {
        val configs = mutableListOf<MutableMap<String, String>>()

        for (i in 0 until maxTasks) {
            val config = mutableMapOf<String, String>()
            config[FILE_NAME] = fileName
            configs.add(config)
        }

        return configs
    }

    override fun start(props: Map<String, String>) {
        fileName = props[FILE_NAME] ?: error("")
    }

    override fun stop() {
    }
}