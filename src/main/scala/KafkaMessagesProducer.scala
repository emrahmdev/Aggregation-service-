import DecodedSample.VehicleSignals
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.adapter._
import akka.kafka._
import akka.kafka.scaladsl.Producer
import akka.stream.scaladsl.Source
import akka.Done
import com.google.protobuf.timestamp.Timestamp
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization._
import org.slf4j.LoggerFactory

import scala.collection.immutable
import scala.concurrent.Future

import scala.concurrent.ExecutionContext

object KafkaMessagesProducer extends App {

  final val log = LoggerFactory.getLogger(getClass)

  private val kafkaServer = "127.0.0.1:9092"
  private val topic = "vehicle-signals"

  implicit val actorSystem: ActorSystem[_] = ActorSystem(Behaviors.ignore, "KafkaMessagesProducer")
  implicit val executionContext: ExecutionContext = actorSystem.executionContext

  // #kafka-setup
  val kafkaProducerSettings = ProducerSettings(actorSystem.toClassic, new StringSerializer, new ProtobufSerializer)
    .withBootstrapServers(kafkaServer)
  // #kafka-setup

  // #writing to kafka
  val signalsList = immutable.Seq(
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587467671)), signalValues = Map("currentSpeed" -> 60, "odometer" -> 61, "uptime" -> 3660000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587467731)), signalValues = Map("currentSpeed" -> 59, "odometer" -> 62, "uptime" -> 3720000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587467791)), signalValues = Map("currentSpeed" -> 59, "odometer" -> 63, "uptime" -> 3780000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587467851)), signalValues = Map("currentSpeed" -> 58, "odometer" -> 64, "uptime" -> 3840000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587467911)), signalValues = Map("currentSpeed" -> 61, "odometer" -> 65, "uptime" -> 3900000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587467971)), signalValues = Map("currentSpeed" -> 58, "odometer" -> 66, "uptime" -> 3960000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587468031)), signalValues = Map("currentSpeed" -> 62, "odometer" -> 67, "uptime" -> 4020000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587468091)), signalValues = Map("currentSpeed" -> 59, "odometer" -> 68, "uptime" -> 4080000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587468151)), signalValues = Map("currentSpeed" -> 58, "odometer" -> 69, "uptime" -> 4140000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587468211)), signalValues = Map("currentSpeed" -> 58, "odometer" -> 70, "uptime" -> 4200000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587468271)), signalValues = Map("currentSpeed" -> 54, "odometer" -> 71, "uptime" -> 4260000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587468331)), signalValues = Map("currentSpeed" -> 57, "odometer" -> 72, "uptime" -> 4320000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587468391)), signalValues = Map("currentSpeed" -> 52, "odometer" -> 73, "uptime" -> 4380000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587468451)), signalValues = Map("currentSpeed" -> 48, "odometer" -> 74, "uptime" -> 4440000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587468511)), signalValues = Map("currentSpeed" -> 48, "odometer" -> 75, "uptime" -> 4500000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587468571)), signalValues = Map("currentSpeed" -> 48, "odometer" -> 76, "uptime" -> 4560000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587468631)), signalValues = Map("currentSpeed" -> 44, "odometer" -> 77, "uptime" -> 4620000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587468691)), signalValues = Map("currentSpeed" -> 44, "odometer" -> 78, "uptime" -> 4680000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587468751)), signalValues = Map("currentSpeed" -> 48, "odometer" -> 79, "uptime" -> 4740000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587468811)), signalValues = Map("currentSpeed" -> 48, "odometer" -> 80, "uptime" -> 4800000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587468871)), signalValues = Map("currentSpeed" -> 49, "odometer" -> 81, "uptime" -> 4860000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587468931)), signalValues = Map("currentSpeed" -> 49, "odometer" -> 82, "uptime" -> 4920000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587468991)), signalValues = Map("currentSpeed" -> 49, "odometer" -> 83, "uptime" -> 4980000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587469051)), signalValues = Map("currentSpeed" -> 46, "odometer" -> 84, "uptime" -> 5040000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587469111)), signalValues = Map("currentSpeed" -> 48, "odometer" -> 85, "uptime" -> 5100000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587469171)), signalValues = Map("currentSpeed" -> 48, "odometer" -> 86, "uptime" -> 5160000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587469231)), signalValues = Map("currentSpeed" -> 45, "odometer" -> 87, "uptime" -> 5220000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587469291)), signalValues = Map("currentSpeed" -> 45, "odometer" -> 88, "uptime" -> 5280000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587469351)), signalValues = Map("currentSpeed" -> 45, "odometer" -> 89, "uptime" -> 5340000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587469411)), signalValues = Map("currentSpeed" -> 45, "odometer" -> 90, "uptime" -> 5400000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587469471)), signalValues = Map("currentSpeed" -> 48, "odometer" -> 91, "uptime" -> 5460000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587469531)), signalValues = Map("currentSpeed" -> 45, "odometer" -> 92, "uptime" -> 5520000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587469591)), signalValues = Map("currentSpeed" -> 45, "odometer" -> 93, "uptime" -> 5580000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587469651)), signalValues = Map("currentSpeed" -> 45, "odometer" -> 94, "uptime" -> 5640000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587469711)), signalValues = Map("currentSpeed" -> 46, "odometer" -> 95, "uptime" -> 5700000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587469771)), signalValues = Map("currentSpeed" -> 43, "odometer" -> 96, "uptime" -> 5760000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587469831)), signalValues = Map("currentSpeed" -> 41, "odometer" -> 97, "uptime" -> 5820000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587469891)), signalValues = Map("currentSpeed" -> 41, "odometer" -> 98, "uptime" -> 5880000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587469951)), signalValues = Map("currentSpeed" -> 40, "odometer" -> 99, "uptime" -> 5940000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587470011)), signalValues = Map("currentSpeed" -> 42, "odometer" -> 100, "uptime" -> 6000000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587470071)), signalValues = Map("currentSpeed" -> 38, "odometer" -> 101, "uptime" -> 6060000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587470131)), signalValues = Map("currentSpeed" -> 35, "odometer" -> 102, "uptime" -> 6120000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587470191)), signalValues = Map("currentSpeed" -> 35, "odometer" -> 103, "uptime" -> 6180000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587470251)), signalValues = Map("currentSpeed" -> 35, "odometer" -> 104, "uptime" -> 6240000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587470311)), signalValues = Map("currentSpeed" -> 35, "odometer" -> 105, "uptime" -> 6300000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587470371)), signalValues = Map("currentSpeed" -> 35, "odometer" -> 106, "uptime" -> 6360000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587470431)), signalValues = Map("currentSpeed" -> 35, "odometer" -> 107, "uptime" -> 6420000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587470491)), signalValues = Map("currentSpeed" -> 38, "odometer" -> 108, "uptime" -> 6480000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587470551)), signalValues = Map("currentSpeed" -> 38, "odometer" -> 109, "uptime" -> 6540000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587470611)), signalValues = Map("currentSpeed" -> 42, "odometer" -> 110, "uptime" -> 6600000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587470671)), signalValues = Map("currentSpeed" -> 0, "odometer" -> 110, "uptime" -> 6660000, "isCharging" -> 1)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587470731)), signalValues = Map("currentSpeed" -> 0, "odometer" -> 110, "uptime" -> 6720000, "isCharging" -> 1)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587470791)), signalValues = Map("currentSpeed" -> 0, "odometer" -> 110, "uptime" -> 6780000, "isCharging" -> 1)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587470851)), signalValues = Map("currentSpeed" -> 0, "odometer" -> 110, "uptime" -> 6840000, "isCharging" -> 1)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587470911)), signalValues = Map("currentSpeed" -> 0, "odometer" -> 110, "uptime" -> 6900000, "isCharging" -> 1)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587470971)), signalValues = Map("currentSpeed" -> 0, "odometer" -> 110, "uptime" -> 6960000, "isCharging" -> 1)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587471031)), signalValues = Map("currentSpeed" -> 44, "odometer" -> 111, "uptime" -> 7020000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587471091)), signalValues = Map("currentSpeed" -> 44, "odometer" -> 112, "uptime" -> 7080000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587471151)), signalValues = Map("currentSpeed" -> 44, "odometer" -> 113, "uptime" -> 7140000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587471211)), signalValues = Map("currentSpeed" -> 44, "odometer" -> 114, "uptime" -> 7200000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587471271)), signalValues = Map("currentSpeed" -> 44, "odometer" -> 115, "uptime" -> 7260000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587471331)), signalValues = Map("currentSpeed" -> 42, "odometer" -> 116, "uptime" -> 7320000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587471391)), signalValues = Map("currentSpeed" -> 44, "odometer" -> 117, "uptime" -> 7380000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587471451)), signalValues = Map("currentSpeed" -> 44, "odometer" -> 118, "uptime" -> 7440000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587471511)), signalValues = Map("currentSpeed" -> 46, "odometer" -> 119, "uptime" -> 7500000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587471571)), signalValues = Map("currentSpeed" -> 44, "odometer" -> 120, "uptime" -> 7560000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587471631)), signalValues = Map("currentSpeed" -> 44, "odometer" -> 121, "uptime" -> 7620000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587471691)), signalValues = Map("currentSpeed" -> 44, "odometer" -> 122, "uptime" -> 7680000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587471751)), signalValues = Map("currentSpeed" -> 39, "odometer" -> 123, "uptime" -> 7740000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587471811)), signalValues = Map("currentSpeed" -> 39, "odometer" -> 124, "uptime" -> 7800000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587471871)), signalValues = Map("currentSpeed" -> 39, "odometer" -> 125, "uptime" -> 7860000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587471931)), signalValues = Map("currentSpeed" -> 37, "odometer" -> 126, "uptime" -> 7920000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587471991)), signalValues = Map("currentSpeed" -> 35, "odometer" -> 127, "uptime" -> 7980000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587472051)), signalValues = Map("currentSpeed" -> 35, "odometer" -> 128, "uptime" -> 8040000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587472111)), signalValues = Map("currentSpeed" -> 38, "odometer" -> 129, "uptime" -> 8100000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587472171)), signalValues = Map("currentSpeed" -> 36, "odometer" -> 130, "uptime" -> 8160000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587472231)), signalValues = Map("currentSpeed" -> 36, "odometer" -> 131, "uptime" -> 8220000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587472291)), signalValues = Map("currentSpeed" -> 36, "odometer" -> 132, "uptime" -> 8280000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587472351)), signalValues = Map("currentSpeed" -> 36, "odometer" -> 133, "uptime" -> 8340000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587472411)), signalValues = Map("currentSpeed" -> 36, "odometer" -> 134, "uptime" -> 8400000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587472471)), signalValues = Map("currentSpeed" -> 36, "odometer" -> 135, "uptime" -> 8460000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587472531)), signalValues = Map("currentSpeed" -> 39, "odometer" -> 136, "uptime" -> 8520000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587472591)), signalValues = Map("currentSpeed" -> 39, "odometer" -> 137, "uptime" -> 8580000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587472651)), signalValues = Map("currentSpeed" -> 39, "odometer" -> 138, "uptime" -> 8640000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587472711)), signalValues = Map("currentSpeed" -> 39, "odometer" -> 139, "uptime" -> 8700000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587472771)), signalValues = Map("currentSpeed" -> 40, "odometer" -> 140, "uptime" -> 8760000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587472831)), signalValues = Map("currentSpeed" -> 42, "odometer" -> 141, "uptime" -> 8820000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587472891)), signalValues = Map("currentSpeed" -> 42, "odometer" -> 142, "uptime" -> 8880000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587472951)), signalValues = Map("currentSpeed" -> 42, "odometer" -> 143, "uptime" -> 8940000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587473011)), signalValues = Map("currentSpeed" -> 42, "odometer" -> 144, "uptime" -> 9000000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587473071)), signalValues = Map("currentSpeed" -> 39, "odometer" -> 145, "uptime" -> 9060000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587473131)), signalValues = Map("currentSpeed" -> 39, "odometer" -> 146, "uptime" -> 9120000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587473191)), signalValues = Map("currentSpeed" -> 34, "odometer" -> 147, "uptime" -> 9180000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587473251)), signalValues = Map("currentSpeed" -> 30, "odometer" -> 147, "uptime" -> 9240000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587473311)), signalValues = Map("currentSpeed" -> 30, "odometer" -> 147, "uptime" -> 9300000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587473371)), signalValues = Map("currentSpeed" -> 30, "odometer" -> 147, "uptime" -> 9360000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587473431)), signalValues = Map("currentSpeed" -> 34, "odometer" -> 148, "uptime" -> 9420000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587473491)), signalValues = Map("currentSpeed" -> 30, "odometer" -> 148, "uptime" -> 9480000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587473551)), signalValues = Map("currentSpeed" -> 30, "odometer" -> 148, "uptime" -> 9540000, "isCharging" -> 0)),
    VehicleSignals(vehicleId = "3bfaca64-598e-4006-aeaa-0e9902069232", recordedAt = Some(Timestamp(seconds = 1587473611)), signalValues = Map("currentSpeed" -> 27, "odometer" -> 148, "uptime" -> 9600000, "isCharging" -> 0)),
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
