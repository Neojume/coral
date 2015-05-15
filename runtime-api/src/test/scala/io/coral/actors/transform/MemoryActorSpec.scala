package io.coral.actors.transform

import akka.actor.ActorSystem
import akka.testkit.{TestActorRef, ImplicitSender, TestKit}
import akka.util.Timeout
import io.coral.actors.CoralActorFactory
import io.coral.api.DefaultModule
import org.json4s.JsonAST.JValue
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import scala.concurrent.duration._

class MemoryActorSpec(_system: ActorSystem)
  extends TestKit(_system)
  with ImplicitSender
  with WordSpecLike
  with Matchers
  with BeforeAndAfterAll {

  def this() = this(ActorSystem("MemoryActorSpec"))

  override def afterAll() {
    TestKit.shutdownActorSystem(system)
  }

  implicit val timeout = Timeout(100.millis)
  JValue

  implicit val injector = new DefaultModule(system.settings.config)

  def createMemoryActor: MemoryActor = {
    val createJson = parse( """{ "type": "memory" }""")
      .asInstanceOf[JObject]
    val props = CoralActorFactory.getProps(createJson).get
    val actorRef = TestActorRef[MemoryActor](props)
    actorRef.underlyingActor
  }

  "StatsActor" should {

    "supply its state" in {
      val actor = createMemoryActor
      actor.state should be(Map(("data", render(JNull))))
    }

    "store the trigger value as its state" in {
      val actor = createMemoryActor
      val triggerJson = parse( """{ "bla": 1.0, "val": 2.7 }""").asInstanceOf[JObject]
      actor.trigger(triggerJson)
      actor.state should be(Map(("data", render(triggerJson))))
    }

    "update its state upon a new trigger" in {
      val actor = createMemoryActor
      val triggerJson1 = parse("""{ "bla": 1.0, "val": 2.7 }""").asInstanceOf[JObject]
      val triggerJson2 = parse("""{ "bla": 2.0, "val": 8.5 }""").asInstanceOf[JObject]
      actor.trigger(triggerJson1)
      actor.state should be(Map(("data", render(triggerJson1))))
      actor.trigger(triggerJson2)
      actor.state should be(Map(("data", render(triggerJson2))))
    }

    "emit nothing" in {
      val actor = createMemoryActor
      actor.emit should be(actor.emitNothing)
    }

    "have no timer" in {
      val actor = createMemoryActor
      actor.timer should be(actor.noTimer)
    }
  }
}
