import com.github.daniel.shuy.kafka.protobuf.serde.KafkaProtobufSerde
import net.metaflow.models.Protos.City
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.Consumed

class TopologyLogic {
    companion object {
        val INPUT_TOPIC = "input-topic"
        val OUTPUT_TOPIC = "output-topic"
    }
    fun topologyInit(bootstrapServers: String) {
        val builder = StreamsBuilder()
        builder.stream(INPUT_TOPIC,
            Consumed.with(Serdes.String(), KafkaProtobufSerde(City.parser())))
            .filter { k, v -> true }
            .to(OUTPUT_TOPIC)

        val topology = builder.build()
        val kprops = Application().getStreamsConfig(bootstrapServers)
        val kafkaStreams = KafkaStreams(topology, kprops)
        print(topology.describe())
        kafkaStreams.start()
    }
}