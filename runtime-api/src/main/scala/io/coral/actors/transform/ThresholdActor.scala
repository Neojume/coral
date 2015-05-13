package io.coral.actors.transform

// akka
import akka.actor.{ActorLogging, Props}
import org.json4s.JsonAST.JValue

// json goodness
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.render

// coral
import io.coral.actors.CoralActor

object ThresholdActor {
  implicit val formats = org.json4s.DefaultFormats

  def getParams(json: JValue) = {
    for {
      key <- (json \ "params" \ "key").extractOpt[String]
      threshold <- getThreshold(json)
      triggerWhen <- getTriggerWhen(json)
    } yield(key, threshold, triggerWhen)
  }

  def getThreshold(json: JValue) = {
    val threshold = json \ "params" \ "threshold"
    val value: Double = threshold match {
      case JDouble(d) => d.toDouble
      case JInt(i) => i.toDouble
      case _ => throw new IllegalArgumentException("threshold")
    }

    Some(value)
  }

  def getTriggerWhen(json: JValue) = {
    val triggerWhen = json \ "params" \ "triggerWhen"
    val value: String = triggerWhen match {
      case JString(s) => s
      case _ => "higher"
    }

    Some(value)
  }

  def apply(json: JValue): Option[Props] = {
    getParams(json).map(_ => Props(classOf[ThresholdActor], json))
    // TODO: take better care of exceptions and error handling
  }
}

class ThresholdActor(json: JObject) extends CoralActor with ActorLogging {
  val (key, threshold, triggerWhen) = ThresholdActor.getParams(json).get

  var thresholdReached = false

  def jsonDef = json

  def state = Map.empty

  def timer = noTimer

  def trigger = {
    json =>
      for {
        value <- getTriggerInputField[Double](json \ key)
      } yield {
        triggerWhen match {
          case "lower" => thresholdReached = value <= threshold
          case _ => thresholdReached = value >= threshold
        }
      }
  }
  def emit = {
    json =>
      thresholdReached match {
        case true => {
          val result = "thresholdReached" -> key
          json merge render(result)
        }
        case false => JNothing
      }
  }

}