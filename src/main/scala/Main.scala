import DecodedSample.VehicleSignals
import akka.Done
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

import scala.concurrent.{Await, Future}
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
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    .withStopTimeout(0.seconds)
  // #kafka-setup

  // #cassandra-setup
  val sessionSettings = CassandraSessionSettings()
  implicit val cassandraSession: CassandraSession = CassandraSessionRegistry.get(actorSystem).sessionFor(sessionSettings)
  // #cassandra-setup

  private def AggregateData(signals: Seq[VehicleSignals]): AggregationResult = {
    val time = (signals.last.signalValues("uptime") - signals.head.signalValues("uptime")) / 1000 / 60 / 60
    val distance = signals.last.signalValues("odometer") - signals.head.signalValues("odometer")
    val averageSpeed = distance / time
    var numberOfCharges = 0

    for( w <- signals.indices)
    {
      if(signals(w).signalValues("isCharging") == 1 && (w > 0 && signals(w-1).signalValues("isCharging") == 0)) {
        numberOfCharges += 1
      }
    }

    val maxSpeed = signals.maxBy(signal => signal.signalValues("currentSpeed")).signalValues("currentSpeed")

    AggregationResult(signals.head.vehicleId, signals.last.recordedAt.get.seconds, averageSpeed, maxSpeed, numberOfCharges)
  }

  CassandraBootstrap.create()

  // #read from kafka
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

  val consumer: Future[Done] = Consumer
    .plainSource(kafkaConsumerSettings, Subscriptions.topics(topic))
    .map { vehicleSignalsRecord => vehicleSignalsRecord.value()}
    .groupBy(Integer.MAX_VALUE, p => p.vehicleId)
    .groupedWithin(Integer.MAX_VALUE, 10000.milliseconds)
    .map(s => s.sortWith((vs1, vs2) => vs1.recordedAt.get.seconds < vs2.recordedAt.get.seconds))
    .map(AggregateData)
    .mergeSubstreams
    .via(cassandraFlow)
    .runWith(Sink.foreach(r => {
      log.info("vehicleId: {}", r.vehicleId)
      log.info("lastMessageTimestamp: {}", r.lastMessageTimestamp)
      log.info("averageSpeed: {}", r.averageSpeed)
      log.info("maxSpeed: {}", r.maxSpeed)
      log.info("charges: {}", r.charges)
      log.info("------------------------")
    }))
  
  Thread.sleep(20.seconds.toMillis)

  actorSystem.terminate()
}
