package net.metaflow

import Application
import TopologyLogic
import TopologyLogic.Companion.INPUT_TOPIC
import TopologyLogic.Companion.OUTPUT_TOPIC
import com.github.daniel.shuy.kafka.protobuf.serde.KafkaProtobufDeserializer
import com.github.daniel.shuy.kafka.protobuf.serde.KafkaProtobufSerializer
import net.metaflow.models.Protos.City
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.common.serialization.LongSerializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.awaitility.Awaitility
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.testcontainers.containers.KafkaContainer
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class TopologyTester {

    companion object {
        val kafkaContainer = KafkaContainer().apply { start() }

        val consumer = KafkaConsumer(Application().getStreamsConfig(kafkaContainer.bootstrapServers).apply {
            put(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "earliest"
            )
        }, StringDeserializer(), KafkaProtobufDeserializer(City.parser()))

        val producer:Producer<Long, City> =
            KafkaProducer(
                Application().getStreamsConfig(kafkaContainer.bootstrapServers),
                LongSerializer(), KafkaProtobufSerializer())

        @BeforeAll
        @JvmStatic
        fun init() {
            TopologyLogic().topologyInit(kafkaContainer.bootstrapServers)
        }

    }

    @Test
    fun streamJoins() {
        val recordCount = DataGenerator().generateAdderssData(producer, INPUT_TOPIC)

        producer.flush()
        consumer.subscribe(listOf(OUTPUT_TOPIC))

        val consumerRecords = mutableListOf<ConsumerRecord<String, City>>()
        val newSingleThreadExecutor = Executors.newSingleThreadExecutor()
        val consumingTask = newSingleThreadExecutor
            .submit {
                while(!Thread.currentThread().isInterrupted) {
                    val received = consumer.poll(Duration.ofMillis(100))
                    received.forEach{consumerRecords.add(it)}
                }
            }
        try {
            Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until {
                    val records = consumerRecords.map { it.value().name }
                    records.size == recordCount && records[recordCount-1] == "Ennadai"
                }
        } finally {
            consumingTask.cancel(true)
            newSingleThreadExecutor.awaitTermination(100, TimeUnit.MILLISECONDS)
        }
    }
}