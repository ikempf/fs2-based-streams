package com.ikempf

import cats.Show
import cats.effect.{ConcurrentEffect, ExitCode, IO, IOApp}
import cats.implicits._
import fs2.Stream
import fs2.concurrent.Queue

import scala.concurrent.duration._

object Main extends IOApp {

  override def run(args: List[String]): IO[ExitCode] =
    (
      consumeSingletonStream,
      consumeSeqStream,
      multiConsumeSeqStream,
      splitConsumeSeqStream,
      consumeQueueStreamPost,
      consumeQueueStreamPre,
      splitConsumeQueue,
      multiConsumeQueue,
    )
      .tupled
      .as(ExitCode.Success)

  private def consumeSingletonStream: IO[Unit] =
    compile("Singleton", singletonStream)

  private def singletonStream: Stream[IO, String] =
    Stream.emit[IO, String]("a")

  private def consumeSeqStream: IO[Unit] =
    compile("Seq", seqStream)

  private def seqStream: Stream[IO, String] =
    Stream.emits[IO, String](List("m1", "m2", "m3", "m4", "m5"))

  private def multiConsumeSeqStream: IO[Unit] =
    (compile("SeqFst", seqStream), compile("SeqSnd", seqStream)).tupled.void

  private def splitConsumeSeqStream: IO[Unit] =
    (compile("SeqHead", seqStream.head), compile("SeqTail", seqStream.tail)).tupled.void

  private def consumeQueueStreamPost: IO[Unit] =
    queueStreamPost.flatMap(compile("Queue post enqueue", _))

  private def queueStreamPost: IO[Stream[IO, String]] =
    Queue.unbounded[IO, String].flatMap(queue => {
      val input = List("q1", "q2", "q3", "q4", "q5")
      ConcurrentEffect[IO].racePair(
        IO.sleep(1.second).productR(Stream.emits[IO, String](input).evalTap(queue.enqueue1).compile.drain),
        IO(queue.dequeue.take(input.length))
      ).flatMap {
        case Left((_, fiber)) => fiber.join
        case Right((fiber, a)) => fiber.join.as(a)
      }
    })

  private def consumeQueueStreamPre: IO[Unit] =
    queueInstantStream.flatMap(compile("Queue pre enqueue", _))

  private def queueInstantStream: IO[Stream[IO, String]] =
    Queue.unbounded[IO, String].flatMap(queue => {
      val input = List("q1", "q2", "q3", "q4", "q5")
      ConcurrentEffect[IO].racePair(
        Stream.emits[IO, String](input).evalTap(e => queue.enqueue1(e)).compile.drain,
        IO.sleep(1.second).as(queue.dequeue.take(input.length))
      ).flatMap {
        case Left((_, fiber)) => fiber.join
        case Right((fiber, a)) => fiber.join.as(a)
      }
    })

  private def multiConsumeQueue: IO[Unit] =
    queueStreamPost.flatMap(queue =>
      ConcurrentEffect[IO]
        .racePair(compile("QueueFst", queue.take(1)), compile("QueueSnd", queue.take(1)))
        .flatMap {
          case Left((_, fiber)) => fiber.join
          case Right((fiber, _)) => fiber.join
        }
        .void
    )

  private def splitConsumeQueue: IO[Unit] =
    queueStreamPost.flatMap(queue =>
      ConcurrentEffect[IO]
        .racePair(compile("QueueHead", queue.head), compile("QueueTail", queue.tail))
        .flatMap {
          case Left((_, fiber)) => fiber.join
          case Right((fiber, _)) => fiber.join
        }
        .void
    )

  private def compile[A: Show](name: String, stream: Stream[IO, A]): IO[Unit] =
    IO
      .delay(println(show"Consuming '$name' stream"))
      .productR(
        stream
          .evalTap(e => IO(print(show"$e;")))
          .compile
          .toList
          .flatMap(result => IO {
            println("")
            println(show"Consumed $name stream $result")
            println(show"----------------------")
          })
      )

}
