package com.kugmax.learn.kafka.controller

import com.kugmax.learn.kafka.service.GitHubEventsCollector
import io.micronaut.http.MediaType
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Get
import io.micronaut.http.annotation.Post
import javax.inject.Inject

@Controller("/api")
class GitHubCollectorController {
    @Inject
    lateinit var collector : GitHubEventsCollector

//    @Post
    @Get //TODO for test
    fun collectEvents() {
        println("In the collectEvents")
        collector.collectEvents()
    }
}