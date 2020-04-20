import DecodedSample.VehicleSignals
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.adapter._
import akka.kafka._
import akka.kafka.scaladsl.Producer
import akka.stream.scaladsl.Source
import akka.Done
import com.google.protobuf.timestamp.Timestamp
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization._
import org.slf4j.LoggerFactory

import scala.collection.immutable
import scala.concurrent.duration._
import scala.concurrent.Future

import scala.concurrent.ExecutionContext

object KafkaMessagesProducer extends App {

  final val log = LoggerFactory.getLogger(getClass)

  private val kakfaServer = "192.168.12.100:9092"
  private val topic = "vehicle-signals"
  private val groupId = "0"

  implicit val actorSystem: ActorSystem[_] = ActorSystem(Behaviors.ignore, "KafkaMessagesProducer")
  implicit val executionContext: ExecutionContext = actorSystem.executionContext

  // #kafka-setup
  val kafkaConsumerSettings = ConsumerSettings(actorSystem.toClassic, new StringDeserializer, new ProtobuffDeserializer)
    .withBootstrapServers(kakfaServer)
    .withGroupId(groupId)
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    .withStopTimeout(0.seconds)

  val kafkaProducerSettings = ProducerSettings(actorSystem.toClassic, new StringSerializer, new ProtobuffSerializer)
    .withBootstrapServers(kakfaServer)
  // #kafka-setup

  // #writing to kafka
  val signalsList = immutable.Seq(
    VehicleSignals(vehicleId = "1", recordedAt = Some(Timestamp(seconds = 1587218849)), signalValues = Map("currentSpeed" -> 66, "odometer" -> 61, "uptime" -> 3660000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "1", recordedAt = Some(Timestamp(seconds = 1587218909)), signalValues = Map("currentSpeed" -> 61, "odometer" -> 62, "uptime" -> 3720000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "1", recordedAt = Some(Timestamp(seconds = 1587218969)), signalValues = Map("currentSpeed" -> 73, "odometer" -> 63, "uptime" -> 3780000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "1", recordedAt = Some(Timestamp(seconds = 1587219029)), signalValues = Map("currentSpeed" -> 58, "odometer" -> 64, "uptime" -> 3840000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "1", recordedAt = Some(Timestamp(seconds = 1587219089)), signalValues = Map("currentSpeed" -> 57, "odometer" -> 65, "uptime" -> 3900000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "1", recordedAt = Some(Timestamp(seconds = 1587219149)), signalValues = Map("currentSpeed" -> 60, "odometer" -> 66, "uptime" -> 3960000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "1", recordedAt = Some(Timestamp(seconds = 1587219209)), signalValues = Map("currentSpeed" -> 65, "odometer" -> 67, "uptime" -> 4020000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "1", recordedAt = Some(Timestamp(seconds = 1587219269)), signalValues = Map("currentSpeed" -> 50, "odometer" -> 68, "uptime" -> 4080000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "1", recordedAt = Some(Timestamp(seconds = 1587219329)), signalValues = Map("currentSpeed" -> 54, "odometer" -> 69, "uptime" -> 4140000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "1", recordedAt = Some(Timestamp(seconds = 1587219389)), signalValues = Map("currentSpeed" -> 57, "odometer" -> 70, "uptime" -> 4200000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "1", recordedAt = Some(Timestamp(seconds = 1587219449)), signalValues = Map("currentSpeed" -> 54, "odometer" -> 71, "uptime" -> 4260000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "1", recordedAt = Some(Timestamp(seconds = 1587219509)), signalValues = Map("currentSpeed" -> 67, "odometer" -> 72, "uptime" -> 4320000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "1", recordedAt = Some(Timestamp(seconds = 1587219569)), signalValues = Map("currentSpeed" -> 67, "odometer" -> 73, "uptime" -> 4380000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "1", recordedAt = Some(Timestamp(seconds = 1587219629)), signalValues = Map("currentSpeed" -> 59, "odometer" -> 74, "uptime" -> 4440000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "1", recordedAt = Some(Timestamp(seconds = 1587219689)), signalValues = Map("currentSpeed" -> 59, "odometer" -> 75, "uptime" -> 4500000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "1", recordedAt = Some(Timestamp(seconds = 1587219749)), signalValues = Map("currentSpeed" -> 45, "odometer" -> 76, "uptime" -> 4560000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "1", recordedAt = Some(Timestamp(seconds = 1587219809)), signalValues = Map("currentSpeed" -> 57, "odometer" -> 77, "uptime" -> 4620000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "1", recordedAt = Some(Timestamp(seconds = 1587219869)), signalValues = Map("currentSpeed" -> 66, "odometer" -> 78, "uptime" -> 4680000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "1", recordedAt = Some(Timestamp(seconds = 1587219929)), signalValues = Map("currentSpeed" -> 71, "odometer" -> 79, "uptime" -> 4740000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "1", recordedAt = Some(Timestamp(seconds = 1587219989)), signalValues = Map("currentSpeed" -> 63, "odometer" -> 80, "uptime" -> 4800000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "1", recordedAt = Some(Timestamp(seconds = 1587220049)), signalValues = Map("currentSpeed" -> 77, "odometer" -> 81, "uptime" -> 4860000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "1", recordedAt = Some(Timestamp(seconds = 1587220109)), signalValues = Map("currentSpeed" -> 65, "odometer" -> 82, "uptime" -> 4920000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "1", recordedAt = Some(Timestamp(seconds = 1587220169)), signalValues = Map("currentSpeed" -> 65, "odometer" -> 83, "uptime" -> 4980000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "1", recordedAt = Some(Timestamp(seconds = 1587220229)), signalValues = Map("currentSpeed" -> 71, "odometer" -> 84, "uptime" -> 5040000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "1", recordedAt = Some(Timestamp(seconds = 1587220289)), signalValues = Map("currentSpeed" -> 79, "odometer" -> 85, "uptime" -> 5100000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "1", recordedAt = Some(Timestamp(seconds = 1587220349)), signalValues = Map("currentSpeed" -> 68, "odometer" -> 86, "uptime" -> 5160000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "1", recordedAt = Some(Timestamp(seconds = 1587220409)), signalValues = Map("currentSpeed" -> 53, "odometer" -> 87, "uptime" -> 5220000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "1", recordedAt = Some(Timestamp(seconds = 1587220469)), signalValues = Map("currentSpeed" -> 63, "odometer" -> 88, "uptime" -> 5280000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "1", recordedAt = Some(Timestamp(seconds = 1587220529)), signalValues = Map("currentSpeed" -> 49, "odometer" -> 89, "uptime" -> 5340000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "1", recordedAt = Some(Timestamp(seconds = 1587220589)), signalValues = Map("currentSpeed" -> 48, "odometer" -> 90, "uptime" -> 5400000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "1", recordedAt = Some(Timestamp(seconds = 1587220649)), signalValues = Map("currentSpeed" -> 0, "odometer" -> 90, "uptime" -> 5460000, "isCharging" -> 1)),
    VehicleSignals(vehicleId = "1", recordedAt = Some(Timestamp(seconds = 1587220709)), signalValues = Map("currentSpeed" -> 0, "odometer" -> 90, "uptime" -> 5540000, "isCharging" -> 1)),
  )

  val producing: Future[Done] = Source(signalsList)
    .map { vehicleSignal =>
      log.info("Producing signal {}", vehicleSignal)
      new ProducerRecord(topic, vehicleSignal.vehicleId, vehicleSignal)
    }
    .runWith(Producer.plainSink(kafkaProducerSettings))

  producing.foreach(_ => log.info("Producing finished"))(actorSystem.executionContext)

  actorSystem.terminate()
}
