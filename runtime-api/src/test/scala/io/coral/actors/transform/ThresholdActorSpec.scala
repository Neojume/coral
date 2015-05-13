package io.coral.actors.transform

import io.coral.api.DefaultModule
import scala.concurrent.duration._
// akka
import akka.actor.ActorSystem
import akka.testkit._
import akka.util.Timeout

// json
import org.json4s._
import org.json4s.jackson.JsonMethods._

// scalatest
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

class ThresholdActorSpec(_system: ActorSystem) extends TestKit(_system)
with ImplicitSender
with WordSpecLike
with Matchers
with BeforeAndAfterAll {

  implicit val timeout = Timeout(100.millis)
  def this() = this(ActorSystem("ThresholdActorSpec"))

  override def afterAll() {
    TestKit.shutdownActorSystem(system)
  }

  "A ThresholdActor" must {
    implicit val injector = new DefaultModule(system.settings.config)

    "Default to trigger when higher" in {
      val constructor = parse(
        """ {"type": "threshold", "params":
            { "key": "key1", "threshold": 10.5 } } """.stripMargin).asInstanceOf[JObject]
      val props = ThresholdActor(constructor).get
      val threshold = TestActorRef[ThresholdActor](props)

      // Subscribe the testprobe for emitting
      val probe = TestProbe()
      threshold.underlyingActor.emitTargets += probe.ref

      // Emit when higher than the threshold
      val json = parse( """{"key1": 10.7}""").asInstanceOf[JObject]
      threshold ! json
      probe.expectMsg(parse( """{"key1": 10.7, "thresholdReached": "key1"}"""))

    }

    "Not create a new actor with an improper definition" in {
      val constructor = parse(
        """{ "type": "threshold", "params" : { "key":
          |"key1", "threshold": "someString" }}""".stripMargin).asInstanceOf[JObject]

      intercept[IllegalArgumentException] {
        val thresholdActor = ThresholdActor(constructor)
        assert(thresholdActor == None)
      }
    }

    "Properly trigger when set to 'higher'" in {
      val constructor = parse(
        """ {"type": "threshold", "params":
            { "key": "key1", "threshold": 10.5, "triggerWhen": "higher" } } """.stripMargin).asInstanceOf[JObject]
      val props = ThresholdActor(constructor).get
      val threshold = TestActorRef[ThresholdActor](props)

      // Subscribe the testprobe for emitting
      val probe = TestProbe()
      threshold.underlyingActor.emitTargets += probe.ref

      // Emit when equal to the threshold
      var json = parse( """{"key1": 10.5}""").asInstanceOf[JObject]
      threshold ! json
      probe.expectMsg(parse( """{"key1": 10.5, "thresholdReached": "key1"}"""))

      // Emit when higher than the threshold
      json = parse( """{"key1": 10.7}""").asInstanceOf[JObject]
      threshold ! json
      probe.expectMsg(parse( """{"key1": 10.7, "thresholdReached": "key1"}"""))

      // Not emit when lower than the threshold
      json = parse( """{"key1": 10.4}""").asInstanceOf[JObject]
      threshold ! json
      probe.expectNoMsg()

      // Not emit when key is not present in triggering json
      json = parse( """{"key2": 10.7}""").asInstanceOf[JObject]
      threshold ! json
      probe.expectNoMsg()
    }

    "Properly trigger when set to 'lower'" in {
      val constructor = parse(
        """ {"type": "threshold", "params":
            { "key": "key1", "threshold": 10.5, "triggerWhen": "lower" } } """.stripMargin).asInstanceOf[JObject]
      val props = ThresholdActor(constructor).get
      val threshold = TestActorRef[ThresholdActor](props)

      // Subscribe the testprobe for emitting
      val probe = TestProbe()
      threshold.underlyingActor.emitTargets += probe.ref

      // Emit when equal to the threshold
      var json = parse( """{"key1": 10.5}""").asInstanceOf[JObject]
      threshold ! json
      probe.expectMsg(parse( """{"key1": 10.5, "thresholdReached": "key1"}"""))

      // Emit when lower than the threshold
      json = parse( """{"key1": 10.4}""").asInstanceOf[JObject]
      threshold ! json
      probe.expectMsg(parse( """{"key1": 10.4, "thresholdReached": "key1"}"""))

      // Not emit when higher than the threshold
      json = parse( """{"key1": 10.7}""").asInstanceOf[JObject]
      threshold ! json
      probe.expectNoMsg()

      // Not emit when key is not present in triggering json
      json = parse( """{"key2": 10.4}""").asInstanceOf[JObject]
      threshold ! json
      probe.expectNoMsg()
    }
  }
}