/*
 * Copyright 2020 Precog Data
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

package tectonic.harness

import cats.effect.{Blocker, IO, Sync}
import cats.instances.long._

import fs2.{io, Chunk, Pipe}

import tectonic.{csv, json, Plate, Signal}
import tectonic.fs2.StreamParser

import scala.{Array, Boolean, Byte, Int, Long, Unit}
import scala.concurrent.ExecutionContext

import java.lang.{CharSequence, SuppressWarnings}
import java.nio.file.Path

object RowCountHarness {

  private implicit val CS = IO.contextShift(ExecutionContext.global)

  def jsonParser(mode: json.Parser.Mode): Pipe[IO, Byte, Long] =
    StreamParser(json.Parser(RowCountPlate[IO], mode))(Chunk.singleton(_))

  def csvParser(config: csv.Parser.Config): Pipe[IO, Byte, Long] =
    StreamParser(csv.Parser(RowCountPlate[IO], config))(Chunk.singleton(_))

  def rowCountJson(file: Path, mode: json.Parser.Mode): IO[Long] = {
    Blocker[IO].use(io.file.readAll[IO](file, _, 16384)
      .through(jsonParser(mode))
      .foldMonoid
      .compile.last
      .map(_.getOrElse(0L)))
  }

  def rowCountCsv(file: Path, config: csv.Parser.Config): IO[Long] = {
    Blocker[IO].use(io.file.readAll[IO](file, _, 16384)
      .through(csvParser(config))
      .foldMonoid
      .compile.last
      .map(_.getOrElse(0L)))
  }

  object RowCountPlate {
    def apply[F[_]: Sync]: F[Plate[Long]] = {
      Sync[F] delay {
        new Plate[Long] {
          @SuppressWarnings(Array("org.wartremover.warts.Var"))
          private var count: Long = 0

          def nul(): Signal = Signal.Continue
          def fls(): Signal = Signal.Continue
          def tru(): Signal = Signal.Continue
          def map(): Signal = Signal.Continue
          def arr(): Signal = Signal.Continue
          def num(s: CharSequence, decIdx: Int, expIdx: Int): Signal = Signal.Continue
          def str(s: CharSequence): Signal = Signal.Continue

          def nestMap(pathComponent: CharSequence): Signal = Signal.Continue
          def nestArr(): Signal = Signal.Continue
          def nestMeta(pathComponent: CharSequence): Signal = Signal.Continue

          def unnest(): Signal = Signal.Continue

          def finishRow(): Unit = count += 1

          def finishBatch(terminal: Boolean) = {
            val back = count
            count = 0
            back
          }

          def skipped(bytes: Int) = ()
        }
      }
    }
  }
}
