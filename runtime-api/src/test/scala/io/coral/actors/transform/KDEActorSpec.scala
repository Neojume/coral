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

  val testData = parse("""{"memory": { "data": [ {"val": 1}, {"val": 2.0}, {"val": 1.0}, {"val": 1.2}, {"val": 1.8} ] }}""").asInstanceOf[JObject]

  def createKDEActorRef: TestActorRef[KDEActor] = {
    val createJson = parse("""{ "type": "kde", "params": { "by": "", "field": "val", "kernel": "gaussian", "bandwidth": "silverman"} }""".stripMargin).asInstanceOf[JObject]
    val props = CoralActorFactory.getProps(createJson).get
    TestActorRef[KDEActor](props)
  }

  "KDEActor" should {

    "obtain correct values from create json" in {
      val actor = createKDEActorRef.underlyingActor
      actor.by should be("")
      actor.field should be("val")
      actor.kernel should be("gaussian")
      actor.bandwidth should be("silverman")
    }

    "have no state" in {
      val actor = createKDEActorRef.underlyingActor
      actor.state should be(Map.empty)
    }

    "have no timer action" in {
      val actor = createKDEActorRef.underlyingActor
      actor.timer should be(actor.noTimer)
    }

    // this should be better separated, even if only from a unit testing point of view
    "process trigger and collect data" in {
      val kdeRef = createKDEActorRef
      val mockMemory = createMockMemoryRef(testData)
      val probe = TestProbe()

      kdeRef.underlyingActor.collectSources = Map("memory" -> mockMemory.path.toString)
      kdeRef.underlyingActor.emitTargets += probe.ref

      kdeRef ! parse(s"""{"val": 1.6 }""").asInstanceOf[JObject]
      probe.expectMsg(parse(s"""{"val": 1.6, "probability": 0.2718602889819728}""").asInstanceOf[JObject])
    }
  }
}
