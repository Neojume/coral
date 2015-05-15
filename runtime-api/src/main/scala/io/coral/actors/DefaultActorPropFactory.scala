package io.coral.actors

import akka.actor.Props
import io.coral.actors.database.CassandraActor
import io.coral.actors.transform._
import org.json4s._

class DefaultActorPropFactory extends ActorPropFactory {
  def getProps(actorType: String, params: JValue): Option[Props] = {
    actorType match {
      case "cassandra" => CassandraActor(params)
      case "fsm" => FsmActor(params)
      case "generator" => GeneratorActor(params)
      case "httpbroadcast" => HttpBroadcastActor(params)
      case "httpclient" => HttpClientActor(params)
      case "kde" => KDEActor(params)
      case "lookup" => LookupActor(params)
      case "memory" => MemoryActor(params)
      case "memuser" => MemoryUserActor(params)
      case "sample" => SampleActor(params)
      case "stats" => StatsActor(params)
      case "threshold" => ThresholdActor(params)
      case "window" => WindowActor(params)
      case "zscore" => ZscoreActor(params)
      case _ => None
    }
  }
}