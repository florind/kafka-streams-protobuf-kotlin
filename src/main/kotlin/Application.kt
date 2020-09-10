
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.streams.StreamsConfig
import java.util.*


class Application {
    fun getStreamsConfig(bootstrapServers: String): Properties {
        val props = Properties()
        props[StreamsConfig.APPLICATION_ID_CONFIG] = "APP_ID_1"
        props[StreamsConfig.NUM_STREAM_THREADS_CONFIG] = 4
        props[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
        //props[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String().javaClass.name
        //props[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = KafkaProtobufSerde().javaClass.name
        props[ConsumerConfig.GROUP_ID_CONFIG] = "group1"
        return props
    }
}

fun main() {
    print("aa")
}
