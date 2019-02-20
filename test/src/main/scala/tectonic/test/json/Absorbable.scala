/*
 * Copyright 2014–2018 SlamData Inc.
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
package test

import cats.effect.IO

import java.lang.String
import java.nio.ByteBuffer

import scala.{Array, Byte}
import scala.util.Either

sealed trait Absorbable[A] {
  def absorb[B](p: BaseParser[IO, B], a: A): IO[Either[ParseException, B]]
}

object Absorbable {

  def apply[A](implicit A: Absorbable[A]): Absorbable[A] = A

  implicit object StringAbs extends Absorbable[String] {
    def absorb[B](p: BaseParser[IO, B], str: String): IO[Either[ParseException, B]] =
      p.absorb(str)
  }

  implicit object ByteBufferAbs extends Absorbable[ByteBuffer] {
    def absorb[B](p: BaseParser[IO, B], bytes: ByteBuffer): IO[Either[ParseException, B]] =
      p.absorb(bytes)
  }

  implicit object BArrayAbs extends Absorbable[Array[Byte]] {
    def absorb[B](p: BaseParser[IO, B], bytes: Array[Byte]): IO[Either[ParseException, B]] =
      p.absorb(bytes)
  }
}
