import DecodedSample.VehicleSignals
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.adapter._
import akka.kafka._
import akka.kafka.scaladsl.Consumer
import akka.stream.alpakka.cassandra.{CassandraSessionSettings, CassandraWriteSettings}
import akka.stream.alpakka.cassandra.scaladsl.{CassandraFlow, CassandraSession, CassandraSessionRegistry}
import akka.stream.scaladsl.Sink
import com.datastax.oss.driver.api.core.cql.{BoundStatement, PreparedStatement}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization._
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
// #imports

import scala.concurrent.ExecutionContext

object Main extends App {
  final val log = LoggerFactory.getLogger(getClass)

  private val kakfaServer = "192.168.12.100:9092"
  private val topic = "vehicle-signals"
  private val groupId = "0"

  implicit val actorSystem: ActorSystem[Nothing] = ActorSystem(Behaviors.ignore, "AggregationService")
  implicit val executionContext: ExecutionContext = actorSystem.executionContext

  // #kafka-setup
  val kafkaConsumerSettings = ConsumerSettings(actorSystem.toClassic, new StringDeserializer, new ProtobuffDeserializer)
    .withBootstrapServers(kakfaServer)
    .withGroupId(groupId)
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    .withStopTimeout(0.seconds)
  // #kafka-setup

  // #cassandra-setup
  val sessionSettings = CassandraSessionSettings()
  implicit val cassandraSession: CassandraSession = CassandraSessionRegistry.get(actorSystem).sessionFor(sessionSettings)
  // #cassandra-setup

  CassandraBootstrap.create()

  object AggregateData {
    var maxSpeed: Double = 0
    var numberOfCharges = 0
    var lastMessageTimestamp: Long = 0
    var isCharging = false

    def proccess(signal: VehicleSignals):Option[AggregationResult] = {

      if(lastMessageTimestamp > signal.recordedAt.get.seconds) return None

      val averageSpeed = signal.signalValues("odometer") / (signal.signalValues("uptime") / 1000 / 60 / 60)
      if(signal.signalValues("currentSpeed") > maxSpeed) {
        maxSpeed = signal.signalValues("currentSpeed")
      }

      if(signal.signalValues("isCharging") == 1 && !isCharging) {
        numberOfCharges += 1
      }

      isCharging = signal.signalValues("isCharging") == 1

      lastMessageTimestamp = signal.recordedAt.get.seconds

      Some(AggregationResult(signal.vehicleId, lastMessageTimestamp, averageSpeed, maxSpeed, numberOfCharges))
    }
  }


  val cassandraFlow = {
    val statementBinder: (AggregationResult, PreparedStatement) => BoundStatement =
      (aggregationResult, preparedStatement) => preparedStatement.bind(
        aggregationResult.vehicleId,
        aggregationResult.averageSpeed,
        aggregationResult.maxSpeed,
        aggregationResult.charges,
        aggregationResult.lastMessageTimestamp
      )
    CassandraFlow.create(CassandraWriteSettings.defaults,
      s"INSERT INTO aggregationApp.results(vehicle_id, average_speed, max_speed, number_of_charges, last_messaget_timestamp) VALUES (?, ?, ?, ?, ?)",
      statementBinder)
  }

  Consumer.plainSource(kafkaConsumerSettings, Subscriptions.topics(topic))
    .map { vehicleSignalsRecord => vehicleSignalsRecord.value()}
    .map(AggregateData.proccess)
    .collect {
      case Some(x) => x
    }
    .via(cassandraFlow)
    .runWith(Sink.foreach(r => {
      log.info("vehicleId: {}", r.vehicleId)
      log.info("lastMessageTimestamp: {}", r.lastMessageTimestamp)
      log.info("averageSpeed: {}", r.averageSpeed)
      log.info("maxSpeed: {}", r.maxSpeed)
      log.info("charges: {}", r.charges)
      log.info("------------------------")
    }))

  Thread.sleep(60.seconds.toMillis)

  // #read from kafka

  actorSystem.terminate()
}
