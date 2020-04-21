import akka.Done
import akka.stream.alpakka.cassandra.scaladsl.CassandraSession
import scala.concurrent.duration._

import scala.concurrent.{Await, Future}

object CassandraBootstrap {
  def create()(implicit session: CassandraSession): Unit = {
    val keyspace: Future[Done] = session.executeDDL(
      """
        |CREATE KEYSPACE IF NOT EXISTS aggregationApp
        |WITH REPLICATION = {
        |  'class' : 'SimpleStrategy',
        |  'replication_factor': 1
        |};
        |""".stripMargin)

    Await.result(keyspace, 10.seconds)

    val table: Future[Done] = session.executeDDL(
      """
        |CREATE TABLE IF NOT EXISTS aggregationApp.results (
        |  vehicle_id TEXT,
        |  average_speed DOUBLE,
        |  max_speed DOUBLE,
        |  number_of_charges INT,
        |  last_message_timestamp BIGINT,
        |
        |  PRIMARY KEY(vehicle_id)
        |);
        |""".stripMargin)

    Await.result(table, 10.seconds)
  }
}