/*
 * Copyright 2014–2019 SlamData Inc.
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
package util

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
 * DEALINGS IN THE SOFTWARE.
 */

import scala.{inline, sys, Array, Char, Int, Unit}

import java.lang.{CharSequence, String, SuppressWarnings, System}

/**
 * CharBuilder is a specialized way to build Strings.
 *
 * It wraps a (growable) array of characters, and can accept
 * additional String or Char data to be added to its buffer.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
private[tectonic] final class CharBuilder {
  @inline final def INITIALSIZE = 32

  private var cs = new Array[Char](INITIALSIZE)
  private var capacity = INITIALSIZE
  private var len = 0

  def reset(): CharBuilder = {
    len = 0
    this
  }

  def makeString: String = new String(cs, 0, len)

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Return",
      "org.wartremover.warts.While"))
  def resizeIfNecessary(goal: Int): Unit = {
    if (goal <= capacity) return ()
    var cap = capacity
    while (goal > cap && cap > 0) cap *= 2
    if (cap > capacity) {
      val ncs = new Array[Char](cap)
      System.arraycopy(cs, 0, ncs, 0, capacity)
      cs = ncs
      capacity = cap
    } else if (cap < capacity) {
      sys.error("maximum string size exceeded")
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.While"))
  def extend(s: CharSequence): Unit = {
    val tlen = len + s.length
    resizeIfNecessary(tlen)
    var i = 0
    var j = len
    len = tlen
    while (i < s.length) {
      cs(j) = s.charAt(i)
      i += 1
      j += 1
    }
  }

  def append(c: Char): Unit = {
    val tlen = len + 1
    resizeIfNecessary(tlen)
    cs(len) = c
    len = tlen
  }
}
