package com.ikempf

import cats.Show
import cats.effect.{ConcurrentEffect, ExitCode, IO, IOApp}
import cats.implicits._
import fs2.Stream
import fs2.concurrent.Queue

import scala.concurrent.duration._

object Main extends IOApp {

  // Flatmapping instead of Apply to run them sequentially
  override def run(args: List[String]): IO[ExitCode] =
    (
      consumeSingletonStream,
      consumeSeqStream,
      multiConsumeSeqStream,
      splitConsumeSeqStream,
      consumeQueueStreamPost,
      consumeQueueStreamPre,
      consumeQueueStreamPreDrop,
      splitConsumeQueue,
      multiConsumeQueue,
    )
      .tupled
      .as(ExitCode.Success)

  private def consumeSingletonStream: IO[Unit] =
    section(compile("Singleton", singletonStream))

  private def singletonStream: Stream[IO, String] =
    Stream.emit[IO, String]("a")

  private def consumeSeqStream: IO[Unit] =
    section(compile("Seq", seqStream))

  private def seqStream: Stream[IO, String] =
    Stream.emits[IO, String](List("m1", "m2", "m3", "m4", "m5"))

  private def multiConsumeSeqStream: IO[Unit] =
    section((compile("SeqFst", seqStream), compile("SeqSnd", seqStream)).tupled.void)

  private def splitConsumeSeqStream: IO[Unit] =
    section((compile("SeqHead", seqStream.head), compile("SeqTail", seqStream.tail)).tupled.void)

  // ----------------- Queue based streams -----------------
  val input = List("q1", "q2", "q3", "q4", "q5")

  private def consumeQueueStreamPost: IO[Unit] =
    section(queueStreamPost)

  private def queueStreamPost: IO[Unit] =
    Queue
      .unbounded[IO, String]
      .flatMap(queue => {
        ConcurrentEffect[IO]
          .racePair(
            IO.sleep(1.second).productR(Stream.emits[IO, String](input).evalTap(queue.enqueue1).compile.drain),
            compile("Queue post enqueue", queue.dequeue)
          )
          .flatMap {
            case Left((_, fiber))  => fiber.join
            case Right((fiber, a)) => fiber.join.as(a)
          }
      })

  private def consumeQueueStreamPre: IO[Unit] =
    section(queueStreamPre)

  private def queueStreamPre: IO[Unit] =
    Queue
      .bounded[IO, String](2)
      .flatMap(queue => {
        ConcurrentEffect[IO]
          .racePair(
            Stream.emits[IO, String](input).evalTap(queue.enqueue1).compile.drain,
            IO.sleep(1.second).productR(compile("Queue pre enqueue blocking backpressure", queue.dequeue))
          )
          .flatMap {
            case Left((_, fiber))  => fiber.join
            case Right((fiber, a)) => fiber.join.as(a)
          }
      })

  private def consumeQueueStreamPreDrop: IO[Unit] =
    section(queueStreamPreDrop)

  private def queueStreamPreDrop: IO[Unit] =
    Queue
      .circularBuffer[IO, String](2)
      .flatMap(queue => {
        ConcurrentEffect[IO]
          .racePair(
            Stream.emits[IO, String](input).evalTap(queue.enqueue1).compile.drain,
            IO.sleep(1.second).productR(compile("Queue pre enqueue dropping backpressure", queue.dequeue))
          )
          .flatMap {
            case Left((_, fiber))  => fiber.join
            case Right((fiber, a)) => fiber.join.as(a)
          }
      })

  private def aQueue =
    Queue
      .unbounded[IO, String]
      .flatMap(
        queue =>
          ConcurrentEffect[IO]
            .racePair(
              IO.sleep(1.second).productR(Stream.emits[IO, String](input).evalTap(queue.enqueue1).compile.drain),
              IO(queue.dequeue)
          ))
      .flatMap {
        case Left((_, fiber))  => fiber.join
        case Right((fiber, a)) => fiber.join.as(a)
      }

  private def splitConsumeQueue: IO[Unit] =
    section(splitStream)

  private def splitStream: IO[Unit] =
    aQueue
      .flatMap(
        queue =>
          ConcurrentEffect[IO]
            .racePair(compile("QueueSplitHead", queue.head), compile("QueueSplitTail", queue.tail))
            .flatMap {
              case Left((_, fiber))  => fiber.join
              case Right((fiber, _)) => fiber.join
            }
            .void)

  private def multiConsumeQueue: IO[Unit] =
    section(multiStream)

  private def multiStream: IO[Unit] =
    aQueue
      .flatMap(
        queue =>
          ConcurrentEffect[IO]
            .racePair(compile("QueueMultiFst", queue), compile("QueueMultiSnd", queue))
            .flatMap {
              case Left((_, fiber))  => fiber.join
              case Right((fiber, _)) => fiber.join
            }
            .void)

  private def section[A](block: IO[A]): IO[Unit] =
    IO.delay(println("-----------------------"))
      .productR(IO.delay(block.runCancelable(_ => IO.unit).unsafeRunSync()))
      .productL(IO.sleep(2.seconds))
      .flatten
      .productR(IO.delay(println("")))

  private def compile[A: Show](name: String, stream: Stream[IO, A]): IO[Unit] =
    IO.delay(println(show"Consuming '$name' stream"))
      .productR(
        stream
          .evalTap(e => IO(print(show"$name -> $e; ")))
          .compile
          .toList
          .productL(IO.delay(println("")))
          .flatMap(result => IO.delay(println(show"Consumed '$name' stream $result")))
      )

}
