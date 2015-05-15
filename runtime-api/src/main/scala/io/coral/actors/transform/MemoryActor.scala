package io.coral.actors.transform

import akka.actor.{ActorLogging, Props}
import io.coral.actors.CoralActor
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.concurrent.Future
import scalaz.OptionT

/**
 * The MemoryActor stores the object provided by trigger in its state, until a new trigger presents itself
 */
object MemoryActor {
  implicit val formats = org.json4s.DefaultFormats

  def apply(json: JValue): Option[Props] = {
    Some(Props(classOf[MemoryActor], json))
  }
}

class MemoryActor(json: JObject) extends CoralActor with ActorLogging {
  var memory: JValue = JNull

  def jsonDef = json
  def emit    = emitNothing
  def timer   = noTimer
  def state   = Map(("data", render(memory)))

  def trigger = {
    json: JObject => {
      memory = json
      OptionT.some(Future.successful({}))
    }
  }
}