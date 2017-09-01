package com.ibm.crail.spark.tools.tpcds

import java.util.concurrent.LinkedBlockingQueue
import scala.collection.immutable.Stream
import scala.sys.process._

/**
  * Created by atr on 01.09.17.
  */

/**
  * Using ProcessBuilder.lineStream produces a stream, that uses
  * a LinkedBlockingQueue with a default capacity of Integer.MAX_VALUE.
  *
  * This causes OOM if the consumer cannot keep up with the producer.
  *
  * See scala.sys.process.ProcessBuilderImpl.lineStream
  */
//atr: http://www.scala-lang.org/api/2.12.1/scala/sys/process/index.html
object BlockingLineStream {
  // See scala.sys.process.Streamed
  private final class BlockingStreamed[T](
                                           val process:   T => Unit,
                                           val    done: Int => Unit,
                                           val  stream:  () => Stream[T]
                                         )

  // See scala.sys.process.Streamed
  private object BlockingStreamed {
    // scala.process.sys.Streamed uses default of Integer.MAX_VALUE,
    // which causes OOMs if the consumer cannot keep up with producer.
    val maxQueueSize = 65536

    def apply[T](nonzeroException: Boolean): BlockingStreamed[T] = {
      val q = new LinkedBlockingQueue[Either[Int, T]](maxQueueSize)

      def next(): Stream[T] = q.take match {
        case Left(0) => Stream.empty
        case Left(code) =>
          if (nonzeroException) scala.sys.error("Nonzero exit code: " + code) else Stream.empty
        case Right(s) => Stream.cons(s, next())
      }

      new BlockingStreamed((s: T) => q put Right(s), code => q put Left(code), () => next())
    }
  }

  // See scala.sys.process.ProcessImpl.Spawn
  private object Spawn {
    def apply(f: => Unit): Thread = apply(f, daemon = false)
    def apply(f: => Unit, daemon: Boolean): Thread = {
      val thread = new Thread() { override def run() = { f } }
      thread.setDaemon(daemon)
      thread.start()
      thread
    }
  }

  def apply(command: Seq[String]): Stream[String] = {
    val streamed = BlockingStreamed[String](true)
    val process = command.run(BasicIO(false, streamed.process, None))
    Spawn(streamed.done(process.exitValue()))
    streamed.stream()
  }
}