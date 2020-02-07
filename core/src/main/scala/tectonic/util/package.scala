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

import scala.{Array, Int, Long}

import java.lang.{CharSequence, SuppressWarnings}

package object util {

  /**
   * Any strings under this length which parse as numbers that lack
   * decimals or exponents may be safely consumed using parseLongUnsafe.
   */
  val MaxSafeLongLength: Int = Long.MaxValue.toString.length - 1

  /**
   * Parse the given character sequence as a single Long value (64-bit
   * signed integer) in decimal (base-10).
   *
   * Other than "0", leading zeros are not allowed, nor are leading
   * plusses. At most one leading minus is allowed. The value "-0" is
   * allowed, and is interpreted as 0.
   *
   * Stated more precisely, accepted values:
   *
   *   - conform to the pattern: -?(0|([1-9][0-9]*))
   *   - are within [-9223372036854775808, 9223372036854775807]
   *
   * This method will throw an `InvalidLong` exception on invalid
   * input.
   */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Var",
      "org.wartremover.warts.While",
      "org.wartremover.warts.ToString",
      "org.wartremover.warts.Throw",
      "org.wartremover.warts.Equals"))
  def parseLong(cs: CharSequence): Long = {

    // we store the inverse of the positive sum, to ensure we don't
    // incorrectly overflow on Long.MinValue. for positive numbers
    // this inverse sum will be inverted before being returned.
    var inverseSum: Long = 0L
    var inverseSign: Long = -1L
    var i: Int = 0

    if (cs.charAt(0) == '-') {
      inverseSign = 1L
      i = 1
    }

    val len = cs.length
    val size = len - i
    if (i >= len) throw InvalidLong(cs.toString)
    if (size > 19) throw InvalidLong(cs.toString)
    if (cs.charAt(i) == '0' && size > 1) throw InvalidLong(cs.toString)

    while (i < len) {
      val digit = cs.charAt(i).toInt - 48
      if (digit < 0 || 9 < digit) throw InvalidLong(cs.toString)
      inverseSum = inverseSum * 10L - digit
      i += 1
    }

    // detect and throw on overflow
    if (size == 19 && (inverseSum >= 0 || (inverseSum == Long.MinValue && inverseSign < 0))) {
      throw InvalidLong(cs.toString)
    }

    inverseSum * inverseSign
  }

  /**
   * Parse the given character sequence as a single Long value (64-bit
   * signed integer) in decimal (base-10).
   *
   * For valid inputs, this method produces the same values as
   * `parseLong`. However, by avoiding input validation it is up to
   * 50% faster.
   *
   * For inputs which `parseLong` throws an error on,
   * `parseLongUnsafe` may (or may not) throw an error, or return a
   * bogus value. This method makes no guarantees about how it handles
   * invalid input.
   *
   * This method should only be used on sequences which have already
   * been parsed (e.g. by a Jawn parser). When in doubt, use
   * `parseLong(cs)`, which is still significantly faster than
   * `java.lang.Long.parseLong(cs.toString)`.
   */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Var",
      "org.wartremover.warts.While",
      "org.wartremover.warts.ToString",
      "org.wartremover.warts.Throw",
      "org.wartremover.warts.Equals"))
  def parseLongUnsafe(cs: CharSequence): Long = {

    // we store the inverse of the positive sum, to ensure we don't
    // incorrectly overflow on Long.MinValue. for positive numbers
    // this inverse sum will be inverted before being returned.
    var inverseSum: Long = 0L
    var inverseSign: Long = -1L
    var i: Int = 0

    if (cs.charAt(0) == '-') {
      inverseSign = 1L
      i = 1
    }

    val len = cs.length
    while (i < len) {
      inverseSum = inverseSum * 10L - (cs.charAt(i).toInt - 48)
      i += 1
    }

    inverseSum * inverseSign
  }
}
