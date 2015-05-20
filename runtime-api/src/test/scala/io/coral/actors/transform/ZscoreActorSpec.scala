package io.coral.actors.transform

import akka.actor.{Actor, ActorSystem, Props}
import akka.testkit.{TestProbe, ImplicitSender, TestActorRef, TestKit}
import akka.util.Timeout
import io.coral.actors.CoralActorFactory
import io.coral.actors.Messages.GetFieldBy
import io.coral.api.DefaultModule
import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.duration._

class ZscoreActorSpec(_system: ActorSystem)
  extends TestKit(_system)
  with ImplicitSender
  with WordSpecLike
  with Matchers
  with BeforeAndAfterAll {

  implicit val injector = new DefaultModule(system.settings.config)

  def this() = this(ActorSystem("coral"))

  override def afterAll() {
    TestKit.shutdownActorSystem(system)
  }

  implicit val timeout = Timeout(100.millis)
  implicit val formats = org.json4s.DefaultFormats

  class MockStatsActor(var count: Long, var avg: Double, var sd: Double) extends Actor {
    def receive = {
      case GetFieldBy("count", _) => sender ! render(count)
      case GetFieldBy("avg", _) => sender ! render(avg)
      case GetFieldBy("sd", _) => sender ! render(sd)
      case GetFieldBy(other, _) => throw new UnsupportedOperationException(other.toString)
    }
  }

  def createMockStatsRef(count: Long, avg: Double, sd: Double): TestActorRef[MockStatsActor] = {
    TestActorRef[MockStatsActor](Props(new MockStatsActor(count, avg, sd)))
  }

  def createZscoreActorRef(by: String, field: String, score: Double): TestActorRef[ZscoreActor] = {
    val createJson = parse(
      s"""{ "type": "zscore",
         |"params": { "by": "${by}",
         |"field": "${field}",
         |"score": ${score} } }""".stripMargin)
      .asInstanceOf[JObject]
    val props = CoralActorFactory.getProps(createJson).get
    TestActorRef[ZscoreActor](props)
  }

  "ZscoreActor" should {

    "obtain correct values from create json" in {
      val actor = createZscoreActorRef("field1", "field2", 6.1).underlyingActor
      actor.by should be("field1")
      actor.field should be("field2")
      actor.score should be(6.1)
    }

    "have no state" in {
      val actor = createZscoreActorRef("field1", "field2", 6.1).underlyingActor
      actor.state should be(Map.empty)
    }

    "have no timer action" in {
      val actor = createZscoreActorRef("field1", "field2", 6.1).underlyingActor
      actor.timer should be(actor.noTimer)
    }

    "not emit when count is too low" in {
      val zscore = createZscoreActorRef(by = "", field = "val", score = 6.1)
      val statsRef = createMockStatsRef(count = 20L, avg = 3.0, sd = 2.0)
      val probe = TestProbe()

      zscore.underlyingActor.collectSources = Map("stats" -> statsRef.path.toString)
      zscore.underlyingActor.emitTargets += probe.ref

      zscore ! parse( s"""{ "dummy": "", "val": 50.0 }""").asInstanceOf[JObject]
      probe.expectNoMsg()

      zscore ! parse( s"""{ "dummy": "", "val": 4.0 }""").asInstanceOf[JObject]
      probe.expectNoMsg()
    }

    "emit correctly when count is high" in {
      val zscore = createZscoreActorRef(by = "", field = "val", score = 6.1)
      val statsRef = createMockStatsRef(count = 21L, avg = 3.0, sd = 2.0)
      val probe = TestProbe()

      zscore.underlyingActor.collectSources = Map("stats" -> statsRef.path.toString)
      zscore.underlyingActor.emitTargets += probe.ref

      zscore ! parse( s"""{ "dummy": "", "val": 50.0 }""").asInstanceOf[JObject]
      probe.expectMsg(parse(s"""{ "dummy": "", "val": 50.0, "outlier": true }""").asInstanceOf[JObject])

      zscore ! parse( s"""{ "dummy": "", "val": 4.0 }""").asInstanceOf[JObject]
      probe.expectNoMsg()
    }
  }
}
