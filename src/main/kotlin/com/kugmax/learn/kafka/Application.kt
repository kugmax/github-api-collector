package com.kugmax.learn.kafka

import io.micronaut.runtime.Micronaut.*
fun main(args: Array<String>) {
	build()
	    .args(*args)
		.packages("com.kugmax.learn.kafka")
		.start()
}

