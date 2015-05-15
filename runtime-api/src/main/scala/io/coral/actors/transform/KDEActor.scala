package io.coral.actors.transform

import akka.actor.Props
import io.coral.actors.CoralActor
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
;

/**
 * Created by kj12xh on 5/13/15.
 */
object KDEActor {
  implicit val formats = org.json4s.DefaultFormats

  def getParams(json: JValue) = {
    for {
    // from json actor definition
    // possible parameters server/client, url, etc
      by <- (json \ "params" \ "by").extractOpt[String]
      field <- (json \ "params" \ "field").extractOpt[String]
      kernel <- getKernel(json)
      bandwidth <- getBandwidth(json)
    } yield {
      (by, field, kernel, bandwidth)
    }
  }

  def getKernel(json: JValue) = {
    val kernel = json \ "params" \ "kernel"
    val value = kernel match {
      case JString(s) => s match {
        case ("gaussian" | "epanechnikov" | "uniform" | "triangular") => s
        case _ => throw new IllegalArgumentException("kernel")
      }
      case JNothing => "gaussian"
      case _ => throw new IllegalArgumentException("kernel")
    }
    Some(value)
  }

  def getBandwidth(json: JValue) = {
    val bandwidth = json \ "params" \ "bandwidth"
    val value = bandwidth match {
      case JDouble(d) => d
      case JInt(i) => i
      case JString(s) => s match {
        case ("silverman" | "scott" | "sheather-jones" ) => s
        case _ => throw new IllegalArgumentException("bandwidth")
      }
      case JNothing => "silverman"
      case _ => throw new IllegalArgumentException("bandwidth")
    }
    Some(value)
  }

  def apply(json: JValue): Option[Props] = {
    getParams(json).map(_ => Props(classOf[KDEActor], json))
    // todo: take better care of exceptions and error handling
  }
}

class KDEActor(json: JObject) extends CoralActor {

  def jsonDef = json

  val (by, field, kernel, bandwidth) = KDEActor.getParams(jsonDef).get
  var probability: Double = 0.0

  def state = Map.empty
  def timer = noTimer

  def applyKernel(kernel: String, value: Double): Double = kernel match {
    case "gaussian" => 1.0 / Math.sqrt(2 * Math.PI) * Math.pow(Math.E, (- 0.5 * value * value))
    case "triangular" => if (Math.abs(value) <= 1) (1.0 - Math.abs(value)) else 0.0
    case "uniform" => if (Math.abs(value) <= 1) 0.5 else 0.0
    case "epanechnikov" => if (Math.abs(value) <= 1) 0.75 * (1.0 - value * value) else 0.0
  }

  def applyBandwidth(x: Double, value: Double, h: Double): Double = Math.abs((x - value) / h) / h

  def computeBandwidth(func: Any, values: Array[Double]): Double = bandwidth match {
    case "silverman" => {
      val mean = values.sum / values.length
      val devs = values.map(value => (value - mean) * (value - mean))
      Math.sqrt(devs.sum / values.length)
    }
    case "scott" => 0.0
    case "sheather-jones" => 0.0
    case _ => bandwidth.asInstanceOf[Double]
  }

  def trigger = {
    json: JObject =>
      for {
        // From trigger data
        subpath <- getTriggerInputField[String](json \ by)
        value <- getTriggerInputField[Double](json \ field)
//        memory <- getCollectInputField[Array[JObject]]("memory", subpath, "data")
        memory <- getCollectInputField[Double]("memory", subpath, "data")
      } yield {
        // compute (local variables & update state)
//        val values = memory.map(_ \ field).map(_.extract[Double])
//        val h = computeBandwidth(bandwidth, values);
//        val computed = values.map(applyBandwidth(_, value, h)).map(applyKernel(kernel, _)).reduce(_ + _)
        probability = memory
      }
  }

  def emit = {
    json: JObject => {
      val result = ("probability" -> probability)
      render(result) merge json
    }
  }
}