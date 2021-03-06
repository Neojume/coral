package io.coral.actors

import io.coral.actors.Messages._
import scala.collection.immutable.SortedMap
import akka.actor._
import scaldi.Injector

class RuntimeActor(implicit injector: Injector) extends Actor with ActorLogging {
  def actorRefFactory = context
  var actors = SortedMap.empty[Long, ActorPath]
  var count = 0L

  def receive = {
    case CreateActor(json) =>
      val props = CoralActorFactory.getProps(json)

      val actorId = props map { p =>
        count += 1
        val id = count
        val actor = actorRefFactory.actorOf(p, s"$id")
        actors += (id -> actor.path)
        id
      }

      sender ! actorId
    case RegisterActorPath(id, path) =>
      actors += (id -> path)
    case UnregisterActorId(id) =>
      actors -= id
    case GetCount() =>
      count += 1
      sender ! count
    case ListActors() =>
      sender ! actors.keys.toList
    case Delete(id: Long) =>
      actors.get(id).map { a => actorRefFactory.actorSelection(a) ! PoisonPill }
      actors -= id
    case DeleteAllActors() =>
      // do not reset the counter since poisoning is asynchrounous!
      actors.foreach { path => actorRefFactory.actorSelection(path._2) ! PoisonPill }
      actors = SortedMap.empty[Long, ActorPath]
      log.info(context.children.size.toString)
    case GetActorPath(id) =>
      val path = actors.get(id)
      sender ! path
  }
}