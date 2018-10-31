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

/*
 * This file substantially copied from the Jawn project and lightly modified. All
 * credit (and much love and thanks!) to Erik Osheim and the other Jawn authors.
 * All copied lines remain under original copyright and license.
 *
 * Copyright Erik Osheim, 2012-2018
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * kDEALINGS IN THE SOFTWARE.
 */

import scala.{sys, Array, Boolean, Byte, Char, Int, Nothing, Unit}
import scala.Predef._
import scala.math.max
import scala.util.Either

import java.lang.{CharSequence, String, SuppressWarnings, System}
import java.nio.ByteBuffer
import java.nio.charset.Charset

@SuppressWarnings(
  Array(
    "org.wartremover.warts.Var",
    "org.wartremover.warts.PublicInference"))
abstract class BaseParser[A] {

  private[this] var data = new Array[Byte](131072)
  private[this] var len = 0
  private[this] var allocated = 131072
  protected[this] final var offset = 0

  // async positioning
  protected[this] final var curr: Int = 0

  private[this] var line = 0
  private[this] var pos = 0

  protected[this] final def newline(i: Int): Unit = { line += 1; pos = i + 1 }
  protected[this] final def column(i: Int) = i - pos

  protected[this] final var done: Boolean = false

  /**
   * More data has been received, consume as much as possible.
   */
  protected[this] def churn(): Either[ParseException, A]

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.NonUnitStatements",
      "org.wartremover.warts.Overloading"))
  final def absorb(buf: ByteBuffer): Either[ParseException, A] = {
    done = false
    val buflen = buf.limit() - buf.position()
    val need = len + buflen
    resizeIfNecessary(need)
    buf.get(data, len, buflen)
    len = need
    churn()
  }

  final def finish(): Either[ParseException, A] = {
    done = true
    churn()
  }

  private[this] final def resizeIfNecessary(need: Int): Unit = {
    // if we don't have enough free space available we'll need to grow our
    // data array. we never shrink the data array, assuming users will call
    // feed with similarly-sized buffers.
    if (need > allocated) {
      val doubled = if (allocated < 0x40000000) allocated * 2 else Int.MaxValue
      val newsize = max(need, doubled)
      val newdata = new Array[Byte](newsize)
      System.arraycopy(data, 0, newdata, 0, len)
      data = newdata
      allocated = newsize
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.Overloading"))
  final def absorb(bytes: Array[Byte]): Either[ParseException, A] =
    absorb(ByteBuffer.wrap(bytes))

  @SuppressWarnings(Array("org.wartremover.warts.Overloading"))
  final def absorb(s: String): Either[ParseException, A] =
    absorb(ByteBuffer.wrap(s.getBytes(BaseParser.Utf8)))


  /**
   * This is a specialized accessor for the case where our underlying data are
   * bytes not chars.
   */
  @SuppressWarnings(Array("org.wartremover.warts.Throw"))
  protected[this] final def byte(i: Int): Byte =
    if (i >= len) throw AsyncException else data(i)

  // we need to signal if we got out-of-bounds
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Throw",
      "org.wartremover.warts.Overloading"))
  protected[this] final def at(i: Int): Char =
    if (i >= len) throw AsyncException else data(i).toChar

  /**
   * Access a byte range as a string.
   *
   * Since the underlying data are UTF-8 encoded, i and k must occur on unicode
   * boundaries. Also, the resulting String is not guaranteed to have length
   * (k - i).
   */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Throw",
      "org.wartremover.warts.Overloading"))
  protected[this] final def at(i: Int, k: Int): CharSequence = {
    if (k > len) throw AsyncException
    val size = k - i
    val arr = new Array[Byte](size)
    System.arraycopy(data, i, arr, 0, size)
    new String(arr, BaseParser.Utf8)
  }

  // the basic idea is that we don't signal EOF until done is true, which means
  // the client explicitly send us an EOF.
  protected[this] final def atEof(i: Int): Boolean =
    if (done) i >= len else false

  // every 1M we shift our array back to the beginning.
  protected[this] final def reset(i: Int): Int = {
    if (offset >= 1048576) {
      val diff = offset
      curr -= diff
      len -= diff
      offset = 0
      pos -= diff
      System.arraycopy(data, diff, data, 0, len)
      i - diff
    } else {
      i
    }
  }

  /**
   * Used to generate error messages with character info and offsets.
   */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.NonUnitStatements",
      "org.wartremover.warts.Throw"))
  protected[this] final def die(i: Int, msg: String): Nothing = {
    val y = line + 1
    val x = column(i) + 1
    val s = "%s got %s (line %d, column %d)" format (msg, at(i), y, x)
    throw ParseException(s, i, y, x)
  }

  /**
   * Used to generate messages for internal errors.
   *
   * This should only be used in situations where a possible bug in
   * the parser was detected. For errors in user-provided JSON, use
   * die().
   */
  protected[this] final def error(msg: String) =
    sys.error(msg)
}

private[tectonic] object BaseParser {
  val Utf8 = Charset.forName("UTF-8")
}
