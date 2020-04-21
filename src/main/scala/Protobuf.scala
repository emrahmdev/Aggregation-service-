import DecodedSample.VehicleSignals
import org.apache.kafka.common.serialization.{Deserializer, Serializer}

class ProtobufSerializer extends Serializer[VehicleSignals] {
  override def serialize(topic: String, data: VehicleSignals): Array[Byte] = data.toByteArray
}

class ProtobufDeserializer extends Deserializer[VehicleSignals] {
  override def deserialize(topic: String, data: Array[Byte]): VehicleSignals = VehicleSignals.parseFrom(data)
}