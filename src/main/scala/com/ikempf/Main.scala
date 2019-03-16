package com.ikempf

import cats.{Apply, Show}
import cats.effect.{ConcurrentEffect, ExitCode, IO, IOApp}
import cats.implicits._
import fs2.Stream
import fs2.concurrent.Queue

import scala.concurrent.duration._

object Main extends IOApp {

  override def run(args: List[String]): IO[ExitCode] =
    Apply[IO]
      .tuple9(
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

  // Data is enqueued after consumption/dequeue starts. No data is lost
  private def queueStreamPost: IO[Unit] =
    Queue
      .unbounded[IO, String]
      .flatMap(queue =>
        ConcurrentEffect[IO]
          .racePair(
            IO.sleep(1.second).productR(Stream.emits[IO, String](input).evalTap(queue.enqueue1).compile.drain),
            compile("Queue post enqueue", queue.dequeue)
          )
          .flatMap {
            case Left((_, fiber))  => fiber.join
            case Right((fiber, a)) => fiber.join.as(a)
          }
      )

  private def consumeQueueStreamPre: IO[Unit] =
    section(queueStreamPre)

  // Data is enqueued before consumption. No data is lost since the bounded buffer blocks enqueue. This also works with an unbounded queue...
  private def queueStreamPre: IO[Unit] =
    Queue
      .bounded[IO, String](2)
      .flatMap(queue =>
        ConcurrentEffect[IO]
          .racePair(
            Stream.emits[IO, String](input).evalTap(queue.enqueue1).compile.drain,
            IO.sleep(1.second).productR(compile("Queue pre enqueue blocking backpressure", queue.dequeue))
          )
          .flatMap {
            case Left((_, fiber))  => fiber.join
            case Right((fiber, a)) => fiber.join.as(a)
          }
      )

  private def consumeQueueStreamPreDrop: IO[Unit] =
    section(queueStreamPreDrop)

  // Data is enqueued before consumption. Data is lost since the queue is backed by a limited, non blocking buffer
  private def queueStreamPreDrop: IO[Unit] =
    Queue
      .circularBuffer[IO, String](2)
      .flatMap(queue =>
        ConcurrentEffect[IO]
          .racePair(
            Stream.emits[IO, String](input).evalTap(queue.enqueue1).compile.drain,
            IO.sleep(1.second).productR(compile("Queue pre enqueue dropping backpressure", queue.dequeue))
          )
          .flatMap {
            case Left((_, fiber))  => fiber.join
            case Right((fiber, a)) => fiber.join.as(a)
          }
      )

  private def aQueue =
    Queue
      .unbounded[IO, String]
      .flatMap(queue =>
        ConcurrentEffect[IO].racePair(
          IO.sleep(1.second).productR(Stream.emits[IO, String](input).evalTap(queue.enqueue1).compile.drain),
          IO(queue.dequeue)
        )
      )
      .flatMap {
        case Left((_, fiber))  => fiber.join
        case Right((fiber, a)) => fiber.join.as(a)
      }

  private def splitConsumeQueue: IO[Unit] =
    section(splitStream)

  // Data is shared between both streams in an unpredictable manner.
  // Even though the streams only consume head/tail, all elements are effectively consumed even if some are ignored
  // The head might thus not be the first element (if the tail stream stole the n first elements)
  // The tail will contain some but not all elements. It might be missing more than just the head (if the head stream stole n first elements)
  private def splitStream: IO[Unit] =
    aQueue.flatMap(queue =>
      ConcurrentEffect[IO]
        .racePair(compile("QueueSplitHead", queue.head), compile("QueueSplitTail", queue.tail))
        .flatMap {
          case Left((_, fiber))  => fiber.join
          case Right((fiber, _)) => fiber.join
        }
    )

  private def multiConsumeQueue: IO[Unit] =
    section(multiStream)

  // Data is shared between both streams in an unpredictable manner.
  private def multiStream: IO[Unit] =
    aQueue.flatMap(queue =>
      ConcurrentEffect[IO]
        .racePair(compile("QueueMultiFst", queue), compile("QueueMultiSnd", queue))
        .flatMap {
          case Left((_, fiber))  => fiber.join
          case Right((fiber, _)) => fiber.join
        }
    )

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
