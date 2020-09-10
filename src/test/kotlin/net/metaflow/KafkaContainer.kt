package net.metaflow

import org.testcontainers.containers.KafkaContainer

class KafkaContainer {
    val kafkaContainer = KafkaContainer()
    fun init() {
        kafkaContainer.start()
    }
}