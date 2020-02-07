/*
 * Copyright 2014–2020 SlamData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tectonic
package json

import cats.effect.{Blocker, ContextShift, IO}

import _root_.fs2.Chunk
import _root_.fs2.io.file

import jawnfs2._

import org.openjdk.jmh.annotations.{Benchmark, BenchmarkMode, Mode, OutputTimeUnit, Param, Scope, State}
import org.openjdk.jmh.infra.Blackhole

import tectonic.fs2.StreamParser

import scala.collection.immutable.List
import scala.concurrent.ExecutionContext

import java.nio.file.Paths
import java.util.concurrent.{Executors, TimeUnit}

@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Array(Mode.AverageTime))
@State(Scope.Benchmark)
class ParserBenchmarks {
  val TectonicFramework = "tectonic"
  val JawnFramework = "jawn"

  private[this] implicit val CS: ContextShift[IO] =
    IO.contextShift(ExecutionContext.global)

  private[this] val BlockingPool =
    Blocker.liftExecutionContext(
      ExecutionContext.fromExecutor(Executors newCachedThreadPool { r =>
        val t = new Thread(r)
        t.setDaemon(true)
        t
      }))

  private[this] val ChunkSize = 65536

  private[this] val ResourceDir =
    Paths.get(System.getProperty("project.resource.dir"))

  import FacadeTuningParams._

  // params

  @Param(Array("tectonic", "jawn"))
  var framework: String = _

  @Param(Array(
    "bar (not wrapped)",
    "bla2 (not wrapped)",
    "bla25 (wrapped)",
    "countries.geo (not wrapped)",
    "dkw-sample (not wrapped)",
    "foo (wrapped)",
    "qux1 (not wrapped)",
    "qux2 (not wrapped)",
    "ugh10k (wrapped)"))
  var input: String = _

  // benchmarks

  // includes the cost of file IO; not sure if that's a good thing?
  @Benchmark
  def parseThroughFs2(bh: Blackhole): Unit = {
    val modeStart = input.indexOf('(')
    val inputMode = input.substring(modeStart + 1, input.length - 1) == "wrapped"
    val inputFile = input.substring(0, modeStart - 1)

    val plateF = BlackholePlate[IO](
      Tectonic.VectorCost,
      Tectonic.ScalarCost,
      Tectonic.TinyScalarCost,
      NumericCost,
      Tectonic.RowCost,
      Tectonic.BatchCost)

    implicit val facade = new BlackholeFacade(
      Jawn.VectorAddCost,
      Jawn.VectorFinalCost,
      Jawn.ScalarCost,
      Jawn.TinyScalarCost,
      NumericCost)

    val contents = file.readAll[IO](
      ResourceDir.resolve(inputFile + ".json"),
      BlockingPool,
      ChunkSize)

    val processed = if (framework == TectonicFramework) {
      val mode = if (inputMode) Parser.UnwrapArray else Parser.ValueStream
      val parser = StreamParser(Parser(plateF, mode): IO[BaseParser[IO, List[Nothing]]])(
        _ => Chunk.empty[Nothing],
        _ => Chunk.empty[Nothing])
      contents.through(parser)
    } else {
      if (inputMode)
        contents.chunks.unwrapJsonArray
      else
        contents.chunks.parseJsonStream
    }

    processed.compile.drain.unsafeRunSync()
  }
}
