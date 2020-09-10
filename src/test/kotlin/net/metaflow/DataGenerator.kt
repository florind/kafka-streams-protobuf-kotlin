package net.metaflow

import net.metaflow.models.Protos
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import java.io.File
import java.util.stream.Stream

class DataGenerator {

    fun generateAdderssData(producer: Producer<Long, Protos.City>, topic: String): Int {
        var count = 0
        readCitiesDb()
            .map<ProducerRecord<Long, Protos.City>> { ProducerRecord(topic, it.id, it) }
            .forEach { producer.send(it); count++; }
        return count
    }

    private fun readCitiesDb(): Stream<Protos.City> {
        return File("src/test/resources/worldcities.csv").bufferedReader().lines().skip(1)
            .map {
                val addressParts = it.split("\",").map { it.replace("\"", "") }
                Protos.City.newBuilder()
                    .setName(addressParts[0])
                    .setCoordinates(Protos.Coordinates
                        .newBuilder().setLat(addressParts[2].toDouble())
                        .setLon(addressParts[3].toDouble()).build())
                    .setCountryIso2(addressParts[5])
                    .setCapital(addressParts[8])
                    .setPopulation(addressParts[9].let { itt ->
                        when(itt) {
                            "" -> -1
                            else -> itt.toDouble().toInt()
                        }
                    })
                    .setAdminName(addressParts[7])
                    .setId(addressParts[10].toLong())
                    .build()
            }
    }
}