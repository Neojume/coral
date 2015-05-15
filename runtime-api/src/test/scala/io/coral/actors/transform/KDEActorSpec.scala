package io.coral.actors.transform

import akka.actor.{Actor, ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestActorRef, TestKit}
import akka.util.Timeout
import io.coral.actors.CoralActorFactory
import io.coral.actors.Messages.GetField
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
      case GetField("memory") => sender ! render(data)
      case GetField("data") => sender ! render(JInt(5))
      case GetField(other) => throw new UnsupportedOperationException(other.toString)
    }
  }

  def createMockMemoryActor(name: String, data: JObject): MockMemoryActor = {
    val ref = TestActorRef[MockMemoryActor](Props(new MockMemoryActor(data)), name)
    ref.underlyingActor
  }

  val testData = parse("""{"memory": { "data": [ {"val": 1}, {"val": 2}, {"val": 1}, {"val": 1.2}, {"val": 1.8} ] }}""").asInstanceOf[JObject]

  def createKDEActor: KDEActor = {
    val createJson = parse("""{ "type": "kde", "params": { "by": "", "field": "val", "kernel": "gaussian", "bandwidth": "silverman"} }""".stripMargin).asInstanceOf[JObject]
    val props = CoralActorFactory.getProps(createJson).get
    val actorRef = TestActorRef[KDEActor](props)
    actorRef.underlyingActor
  }

  "KDEActor" should {

    "obtain correct values from create json" in {
      val actor = createKDEActor
      actor.by should be("")
      actor.field should be("val")
      actor.kernel should be("gaussian")
      actor.bandwidth should be("silverman")
    }

    "have no state" in {
      val actor = createKDEActor
      actor.state should be(Map.empty)
    }

    "have no timer action" in {
      val actor = createKDEActor
      actor.timer should be(actor.noTimer)
    }

    // this should be better separated, even if only from a unit testing point of view
    "process trigger and collect data" in {
      val kde = createKDEActor
      val mockMemory = createMockMemoryActor("mock1", testData)
      mockMemory.data should be(testData)

      kde.collectSources = Map("memory" -> "/user/mock1")
      kde.trigger(parse(s"""{"val": 50.0 }""").asInstanceOf[JObject])

      awaitCond(kde.probability == 5)
      kde.emit(parse(s"""{"val": 50.0 }""").asInstanceOf[JObject]) should be(parse("""{"val": 50.0, "probability": 0.0}"""))
    }
  }
}
