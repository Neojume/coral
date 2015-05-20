package io.coral.actors.transform

// akka
import akka.actor.{ActorLogging, Props}

//json goodness
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.{render, compact}

// coral
import io.coral.actors.CoralActor


object ZscoreActor {
  implicit val formats = org.json4s.DefaultFormats

  def getParams(json: JValue) = {
    for {
    // from json actor definition
    // possible parameters server/client, url, etc
      by <- (json \ "params" \ "by").extractOpt[String]
      field <- (json \ "params" \ "field").extractOpt[String]
      score <- (json \ "params" \ "score").extractOpt[Double]
    } yield {
      (by, field, score)
    }
  }

  def apply(json: JValue): Option[Props] = {
    getParams(json).map(_ => Props(classOf[ZscoreActor], json))
    // todo: take better care of exceptions and error handling
  }

}

// metrics actor example
class ZscoreActor(json: JObject) extends CoralActor {

  def jsonDef = json

  val (by, field, score) = ZscoreActor.getParams(jsonDef).get
  var outlier: Boolean = false

  def state = Map.empty

  def timer = noTimer

  def trigger = {
    json: JObject =>
      for {
      // from trigger data
        value <- getTriggerInputField[Double](json \ field)

        // from other actors
        count <- getCollectInputField[Long]("stats", by, "count")
        avg   <- getCollectInputField[Double]("stats", by, "avg")
        std   <- getCollectInputField[Double]("stats", by, "sd")

      //alternative syntax from other actors multiple fields
      //(avg,std) <- getActorField[Double](s"/user/events/histogram/$city", List("avg", "sd"))
      } yield {
        // compute (local variables & update state)
        val th = avg + score * std
        outlier = (value > th) & (count > 20)
      }
  }

  def emit = {
    json: JObject =>
      outlier match {
        case true =>
          // produce emit my results (dataflow)
          // need to define some json schema, maybe that would help
          val result = ("outlier" -> outlier)

          // what about merging with input data?
          val js = render(result) merge json

          //emit resulting json
          js

        case _ => JNothing
      }
  }
}