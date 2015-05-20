package io.coral.actors.transform

import akka.actor.{Actor, ActorSystem, Props}
import akka.testkit.{TestProbe, ImplicitSender, TestActorRef, TestKit}
import akka.util.Timeout
import io.coral.actors.CoralActorFactory
import io.coral.actors.Messages.{GetFieldBy, GetField}
import io.coral.api.DefaultModule
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.duration._

class KDEActorSpec(_system: ActorSystem)
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

  class MockMemoryActor(var data: JObject) extends Actor {
    def receive = {
      case GetFieldBy("data", _) => sender ! render(data)
      case GetField(other) => throw new UnsupportedOperationException(other.toString)
    }
  }

  def createMockMemoryRef(data: JObject): TestActorRef[MockMemoryActor] = {
    TestActorRef[MockMemoryActor](Props(new MockMemoryActor(data)))
  }

  val tinyData = parse("""{ "data": [ {"val": 1}, {"val": 2.0} ] }""").asInstanceOf[JObject]
  val smallData = parse("""{ "data": [ {"val": 1}, {"val": 2.0}, {"val": 1.4}, {"val": 1.2}, {"val": 1.8} ] }""").asInstanceOf[JObject]

  def createKDEActorRef(by: String, field: String, kernel: String, bandwidth: String): TestActorRef[KDEActor] = {
    val createJson = parse(s"""{ "type": "kde", "params": { "by": "${by}", "field": "${field}", "kernel": "${kernel}", "bandwidth": "${bandwidth}"} }""".stripMargin).asInstanceOf[JObject]
    val props = CoralActorFactory.getProps(createJson).get
    TestActorRef[KDEActor](props)
  }

  "KDEActor" should {

    "obtain correct values from create json" in {
      val actor = createKDEActorRef("by", "val", "gaussian", "silverman").underlyingActor
      actor.by should be("by")
      actor.field should be("val")
      actor.kernel should be("gaussian")
      actor.bandwidth should be("silverman")
    }

    "have no state" in {
      val actor = createKDEActorRef("by", "val", "gaussian", "silverman").underlyingActor
      actor.state should be(Map.empty)
    }

    "have no timer action" in {
      val actor = createKDEActorRef("by", "val", "gaussian", "silverman").underlyingActor
      actor.timer should be(actor.noTimer)
    }

    "create no actor with invalid kernel" in {
      intercept[IllegalArgumentException] {
        val kdeActor = createKDEActorRef("by", "val", "not a kernel", "silverman")
        kdeActor should be(None)
      }
    }

    "create no actor with invalid bandwidth" in {
      // Invalid string
      intercept[IllegalArgumentException] {
        val kdeActor = createKDEActorRef("by", "val", "gaussian", "invalid bandwidth")
        kdeActor should be(None)
      }

      // Invalid type
      intercept[IllegalArgumentException] {
        val createJson = parse("""{ "type": "kde", "params": { "field": "val", "bandwidth": {"bandwidth": "still invalid"}} }""".stripMargin).asInstanceOf[JObject]
        val props = CoralActorFactory.getProps(createJson).get
        val kdeActor = TestActorRef[KDEActor](props).underlyingActor
        kdeActor should be(None)
      }
    }

    "use defaults when no params are provided" in {
      val createJson = parse("""{ "type": "kde", "params": { "field": "val"} }""".stripMargin).asInstanceOf[JObject]
      val props = CoralActorFactory.getProps(createJson).get
      val actor = TestActorRef[KDEActor](props).underlyingActor
      actor.by should be("")
      actor.kernel should be("gaussian")
      actor.bandwidth should be("silverman")
    }

    "compute a probability based on the memory actor" in {
      val kdeRef = createKDEActorRef("", "val", "gaussian", "silverman")
      val mockMemory = createMockMemoryRef(tinyData)
      val probe = TestProbe()

      kdeRef.underlyingActor.collectSources = Map("memory" -> mockMemory.path.toString)
      kdeRef.underlyingActor.emitTargets += probe.ref

      kdeRef ! parse(s"""{"val": 1.0 }""").asInstanceOf[JObject]
      probe.expectMsg(parse(s"""{"val": 1.0, "probability": 0.5287675721190984}""").asInstanceOf[JObject])
    }

    "be able to use scotts rule of thumb" in {
      val kdeRef = createKDEActorRef("", "val", "gaussian", "scott")
      val mockMemory = createMockMemoryRef(tinyData)
      val probe = TestProbe()

      kdeRef.underlyingActor.collectSources = Map("memory" -> mockMemory.path.toString)
      kdeRef.underlyingActor.emitTargets += probe.ref

      kdeRef ! parse(s"""{"val": 1.0 }""").asInstanceOf[JObject]
      probe.expectMsg(parse(s"""{"val": 1.0, "probability": 0.473872584145238}""").asInstanceOf[JObject])
    }
  }
}
