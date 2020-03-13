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

package tectonic
package json

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

import cats.effect.Sync
import cats.syntax.all._

import tectonic.util.{BList, CharBuilder}

import scala.{
  inline,
  Array,
  Boolean,
  Char,
  Either,
  Int,
  Left,
  Long,
  StringContext,
  Right,
  Unit
}
// import scala._, Predef._
import scala.annotation.{switch, tailrec}

import java.lang.{CharSequence, IndexOutOfBoundsException, SuppressWarnings}

/**
 * Parser is able to parse chunks of data (encoded as
 * Option[ByteBuffer] instances) and parse asynchronously. You can
 * use the factory methods in the companion object to instantiate an
 * async parser.
 *
 * The async parser's fields are described below:
 *
 * The (state, curr, stack) triple is used to save and restore parser
 * state between async calls. State also helps encode extra
 * information when streaming or unwrapping an array.
 *
 * The (data, len, allocated) triple is used to manage the underlying
 * data the parser is keeping track of. As new data comes in, data may
 * be expanded if not enough space is available.
 *
 * The offset parameter is used to drive the outer async parsing. It
 * stores similar information to curr but is kept separate to avoid
 * "corrupting" our snapshot.
 *
 * The done parameter is used internally to help figure out when the
 * atEof() parser method should return true. This will be set when
 * apply(None) is called.
 *
 * The streamMode parameter controls how the asynchronous parser will
 * be handling multiple values. There are three states:
 *
 *    1: An array is being unwrapped. Normal JSON array rules apply
 *       (Note that if the outer value observed is not an array, this
 *       mode will toggle to the -1 mode).
 *
 *    0: A stream of individual JSON elements separated by whitespace
 *       are being parsed. We can return each complete element as we
 *       parse it.
 *
 *   -1: No streaming is occuring. Only a single JSON value is
 *       allowed.
 */
@SuppressWarnings(
  Array(
    "org.wartremover.warts.Var",
    "org.wartremover.warts.FinalVal",
    "org.wartremover.warts.PublicInference"))   // needed due to bug in WartRemover
final class Parser[F[_], A] private (
    plate: Plate[A],
    private[this] var state: Int,
    private[this] var ring: Long,
    private[this] var roffset: Int,
    private[this] var fallback: BList,
    private[this] var streamMode: Int)
    extends BaseParser[F, A] {

  /**
   * Explanation of the new synthetic states. The parser machinery
   * uses positive integers for states while parsing json values. We
   * use these negative states to keep track of the async parser's
   * status between json values.
   *
   * ASYNC_PRESTART: We haven't seen any non-whitespace yet. We
   * could be parsing an array, or not. We are waiting for valid
   * JSON.
   *
   * ASYNC_START: We've seen an array and have begun unwrapping
   * it. We could see a ] if the array is empty, or valid JSON.
   *
   * ASYNC_END: We've parsed an array and seen the final ]. At this
   * point we should only see whitespace or an EOF.
   *
   * ASYNC_POSTVAL: We just parsed a value from inside the array. We
   * expect to see whitespace, a comma, or a ].
   *
   * ASYNC_PREVAL: We are in an array and we just saw a comma. We
   * expect to see whitespace or a JSON value.
   */
  @inline private[this] final val ASYNC_PRESTART = -5
  @inline private[this] final val ASYNC_START = -4
  @inline private[this] final val ASYNC_END = -3
  @inline private[this] final val ASYNC_POSTVAL = -2
  @inline private[this] final val ASYNC_PREVAL = -1

  /**
   * Valid parser states.
   */
  @inline private[this] final val ARRBEG = 6
  @inline private[this] final val OBJBEG = 7
  @inline private[this] final val DATA = 1
  @inline private[this] final val KEY = 2
  @inline private[this] final val SEP = 3
  @inline private[this] final val ARREND = 4
  @inline private[this] final val OBJEND = 5

  @inline private[this] final val SKIP_MAIN = 8
  @inline private[this] final val SKIP_STRING = 9

  /*
   * This is slightly tricky. We're using the otherwise-unused
   * high order bits of `state` to track the depth of the structural
   * nesting when we are skipping. We can do this because we know
   * that the depth will always be positive. We posit that 134,217,727
   * levels is enough nesting for anyone.
   */

  @inline private[this] final val SKIP_DEPTH_SHIFT = 4

  // remove the sign and lowest four bits
  @inline private[this] final val SKIP_DEPTH_MASK =
    ~(Int.MinValue | (1 << SKIP_DEPTH_SHIFT) - 1)

  @inline private[this] final val SKIP_DEPTH_LIMIT = (2 << (31 - 4)) - 1

  // private[this] final val Continue = Signal.Continue
  private[this] final val SkipColumn = Signal.SkipColumn
  // private[this] final val SkipRow = Signal.SkipRow
  // private[this] final val Terminate = Signal.Terminate

  @SuppressWarnings(Array("org.wartremover.warts.While"))
  private[this] val HexChars: Array[Int] = {
    val arr = new Array[Int](128)
    var i = 0
    while (i < 10) { arr(i + '0') = i; i += 1 }
    i = 0
    while (i < 16) { arr(i + 'a') = 10 + i; arr(i + 'A') = 10 + i; i += 1 }
    arr
  }

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Equals",
      "org.wartremover.warts.While"))
  protected[this] def churn(): Either[ParseException, A] = {

    // we rely on exceptions to tell us when we run out of data
    try {
      while (true) {
        if (state < 0) {
          (at(offset): @switch) match {
            case '\n' =>
              newline(offset)
              offset += 1

            case ' ' | '\t' | '\r' =>
              offset += 1

            case '[' =>
              if (state == ASYNC_PRESTART) {
                offset += 1
                state = ASYNC_START
              } else if (state == ASYNC_END) {
                die(offset, "expected eof")
              } else if (state == ASYNC_POSTVAL) {
                die(offset, "expected , or ]")
              } else {
                state = 0
              }

            case ',' =>
              if (state == ASYNC_POSTVAL) {
                offset += 1
                state = ASYNC_PREVAL
              } else if (state == ASYNC_END) {
                die(offset, "expected eof")
              } else {
                die(offset, "expected json value")
              }

            case ']' =>
              if (state == ASYNC_POSTVAL || state == ASYNC_START) {
                if (streamMode > 0) {
                  offset += 1
                  state = ASYNC_END
                } else {
                  die(offset, "expected json value or eof")
                }
              } else if (state == ASYNC_END) {
                die(offset, "expected eof")
              } else {
                die(offset, "expected json value")
              }

            case c =>
              if (state == ASYNC_END) {
                die(offset, "expected eof")
              } else if (state == ASYNC_POSTVAL) {
                die(offset, "expected ] or ,")
              } else {
                if (state == ASYNC_PRESTART && streamMode > 0) streamMode = -1
                state = 0
              }
          }

        } else {
          // jump straight back into rparse
          offset = reset(offset)
          curr = reset(curr)    // we reset both of these, because offset only gets updated when the "row" finishes

          val j = if (state <= 0) {
            parse(offset)
          } else if (state >= 8) {
            val curr2 = rskip(state, curr)
            plate.skipped(curr2 - curr)
            rparse(if (enclosure(ring, roffset, fallback)) OBJEND else ARREND, curr2, ring, roffset, fallback)
          } else {
            rparse(state, curr, ring, roffset, fallback)
          }

          if (streamMode > 0) {
            state = ASYNC_POSTVAL
          } else if (streamMode == 0) {
            state = ASYNC_PREVAL
          } else {
            state = ASYNC_END
          }
          curr = j
          offset = j
        }
      }
      Right(plate.finishBatch(false))
    } catch {
      case AsyncException =>
        if (done) {
          // if we are done, make sure we ended at a good stopping point
          if (state == ASYNC_PREVAL || state == ASYNC_END) Right(plate.finishBatch(true))
          else Left(ParseException("exhausted input", -1, -1, -1))
        } else {
          // we ran out of data, so return what we have so far
          Right(plate.finishBatch(false))
        }

      case e: ParseException =>
        // we hit a parser error, so return that error and results so far
        Left(e)
    }
  }

  /**
   * We use this to keep track of the last recoverable place we've
   * seen. If we hit an AsyncException, we can later resume from this
   * point.
   *
   * This method is called during every loop of rparse, and the
   * arguments are the exact arguments we can pass to rparse to
   * continue where we left off.
   */
  protected[this] def checkpoint(state: Int, i: Int, ring: Long, offset: Int, fallback: BList): Unit = {
    this.state = state
    this.curr = i
    this.ring = ring
    this.roffset = offset
    this.fallback = fallback
  }

  /**
   * Parse the given number, and add it to the given context.
   *
   * We don't actually instantiate a number here, but rather pass the
   * string of for future use. Facades can choose to be lazy and just
   * store the string. This ends up being way faster and has the nice
   * side-effect that we know exactly how the user represented the
   * number.
   */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.While",
      "org.wartremover.warts.NonUnitStatements",
      "org.wartremover.warts.Equals"))
  protected[this] def parseNum(i: Int): Int = {
    var j = i
    var c = at(j)
    var decIndex = -1
    var expIndex = -1

    if (c == '-') {
      j += 1
      c = at(j)
    }
    if (c == '0') {
      j += 1
      c = at(j)
    } else if ('1' <= c && c <= '9') {
      while ('0' <= c && c <= '9') { j += 1; c = at(j) }
    } else {
      die(i, "expected digit")
    }

    if (c == '.') {
      decIndex = j - i
      j += 1
      c = at(j)
      if ('0' <= c && c <= '9') {
        while ('0' <= c && c <= '9') { j += 1; c = at(j) }
      } else {
        die(i, "expected digit")
      }
    }

    if (c == 'e' || c == 'E') {
      expIndex = j - i
      j += 1
      c = at(j)
      if (c == '+' || c == '-') {
        j += 1
        c = at(j)
      }
      if ('0' <= c && c <= '9') {
        while ('0' <= c && c <= '9') { j += 1; c = at(j) }
      } else {
        die(i, "expected digit")
      }
    }

    plate.num(at(i, j), decIndex, expIndex)
    j
  }

  /**
   * Parse the given number, and add it to the given context.
   *
   * This method is a bit slower than parseNum() because it has to be
   * sure it doesn't run off the end of the input.
   *
   * Normally (when operating in rparse in the context of an outer
   * array or object) we don't need to worry about this and can just
   * grab characters, because if we run out of characters that would
   * indicate bad input. This is for cases where the number could
   * possibly be followed by a valid EOF.
   *
   * This method has all the same caveats as the previous method.
   */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Var",
      "org.wartremover.warts.While",
      "org.wartremover.warts.Return",
      "org.wartremover.warts.NonUnitStatements",
      "org.wartremover.warts.Equals"))
  protected[this] def parseNumSlow(i: Int): Int = {
    var j = i
    var c = at(j)
    var decIndex = -1
    var expIndex = -1

    if (c == '-') {
      // any valid input will require at least one digit after -
      j += 1
      c = at(j)
    }
    if (c == '0') {
      j += 1
      if (atEof(j)) {
        plate.num(at(i, j), decIndex, expIndex)
        return j
      }
      c = at(j)
    } else if ('1' <= c && c <= '9') {
      while ('0' <= c && c <= '9') {
        j += 1
        if (atEof(j)) {
          plate.num(at(i, j), decIndex, expIndex)
          return j
        }
        c = at(j)
      }
    } else {
      die(i, "expected digit")
    }

    if (c == '.') {
      // any valid input will require at least one digit after .
      decIndex = j - i
      j += 1
      c = at(j)
      if ('0' <= c && c <= '9') {
        while ('0' <= c && c <= '9') {
          j += 1
          if (atEof(j)) {
            plate.num(at(i, j), decIndex, expIndex)
            return j
          }
          c = at(j)
        }
      } else {
        die(i, "expected digit")
      }
    }

    if (c == 'e' || c == 'E') {
      // any valid input will require at least one digit after e, e+, etc
      expIndex = j - i
      j += 1
      c = at(j)
      if (c == '+' || c == '-') {
        j += 1
        c = at(j)
      }
      if ('0' <= c && c <= '9') {
        while ('0' <= c && c <= '9') {
          j += 1
          if (atEof(j)) {
            plate.num(at(i, j), decIndex, expIndex)
            return j
          }
          c = at(j)
        }
      } else {
        die(i, "expected digit")
      }
    }

    plate.num(at(i, j), decIndex, expIndex)
    j
  }

  /**
   * Generate a Char from the hex digits of "\u1234" (i.e. "1234").
   *
   * NOTE: This is only capable of generating characters from the basic plane.
   * This is why it can only return Char instead of Int.
   */
  @SuppressWarnings(Array("org.wartremover.warts.While"))
  protected[this] def descape(s: CharSequence): Char = {
    val hc = HexChars
    var i = 0
    var x = 0
    while (i < 4) {
      x = (x << 4) | hc(s.charAt(i).toInt)
      i += 1
    }
    x.toChar
  }

  /**
   * See if the string has any escape sequences. If not, return the end of the
   * string. If so, bail out and return -1.
   *
   * This method expects the data to be in UTF-8 and accesses it as bytes. Thus
   * we can just ignore any bytes with the highest bit set.
   */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Equals",
      "org.wartremover.warts.Return",
      "org.wartremover.warts.While"))
  protected[this] def parseStringSimple(i: Int): Int = {
    var j = i
    var c: Int = byte(j) & 0xff
    while (c != 34) {
      if (c < 32) die(j, s"control char ($c) in string")
      if (c == 92) return -1
      j += 1
      c = byte(j) & 0xff
    }
    j + 1
  }

  /**
   * Parse the JSON string starting at 'i' and save it into the plate.
   * If key is true, save the string with 'nestMap', otherwise use 'str'.
   */
   @SuppressWarnings(
    Array(
      "org.wartremover.warts.Equals",
      "org.wartremover.warts.NonUnitStatements",
      "org.wartremover.warts.While",
      "org.wartremover.warts.Return"))
  protected[this] def parseString(i: Int, key: Boolean): Boolean = {
    val k = parseStringSimple(i + 1)
    if (k != -1) {
      val cs = at(i + 1, k - 1)
      val s = if (key) plate.nestMap(cs) else plate.str(cs)
      this.curr = k
      return (s ne SkipColumn)
    }

    // TODO: we might be able to do better by identifying where
    // escapes occur, and then translating the intermediate strings in
    // one go.

    var j = i + 1
    val sb = new CharBuilder

    var c: Int = byte(j) & 0xff
    while (c != 34) { // "
      if (c == 92) { // \
        (byte(j + 1): @switch) match {
          case 98 => { sb.append('\b'); j += 2 }
          case 102 => { sb.append('\f'); j += 2 }
          case 110 => { sb.append('\n'); j += 2 }
          case 114 => { sb.append('\r'); j += 2 }
          case 116 => { sb.append('\t'); j += 2 }

          case 34 => { sb.append('"'); j += 2 }
          case 47 => { sb.append('/'); j += 2 }
          case 92 => { sb.append('\\'); j += 2 }

          // if there's a problem then descape will explode
          case 117 => { sb.append(descape(at(j + 2, j + 6))); j += 6 }

          case c => die(j, s"invalid escape sequence (\\${c.toChar})")
        }
      } else if (c < 32) {
        die(j, s"control char ($c) in string")
      } else if (c < 128) {
        // 1-byte UTF-8 sequence
        sb.append(c.toChar)
        j += 1
      } else if ((c & 224) == 192) {
        // 2-byte UTF-8 sequence
        sb.extend(at(j, j + 2))
        j += 2
      } else if ((c & 240) == 224) {
        // 3-byte UTF-8 sequence
        sb.extend(at(j, j + 3))
        j += 3
      } else if ((c & 248) == 240) {
        // 4-byte UTF-8 sequence
        sb.extend(at(j, j + 4))
        j += 4
      } else {
        die(j, "invalid UTF-8 encoding")
      }
      c = byte(j) & 0xff
    }
    val s = if (key) plate.nestMap(sb.makeString) else plate.str(sb.makeString)
    this.curr = j + 1
    s ne SkipColumn
  }

  /**
   * Parse the JSON constant "true".
   *
   * Note that this method assumes that the first character has already been checked.
   */
  @SuppressWarnings(Array("org.wartremover.warts.Equals"))
  protected[this] def parseTrue(i: Int): Unit =
    if (at(i + 1) == 'r' && at(i + 2) == 'u' && at(i + 3) == 'e') {
      val _ = plate.tru()
      ()
    } else {
      die(i, "expected true")
    }

  /**
   * Parse the JSON constant "false".
   *
   * Note that this method assumes that the first character has already been checked.
   */
  @SuppressWarnings(Array("org.wartremover.warts.Equals"))
  protected[this] def parseFalse(i: Int): Unit =
    if (at(i + 1) == 'a' && at(i + 2) == 'l' && at(i + 3) == 's' && at(i + 4) == 'e') {
      val _ = plate.fls()
      ()
    } else {
      die(i, "expected false")
    }

  /**
   * Parse the JSON constant "null".
   *
   * Note that this method assumes that the first character has already been checked.
   */
  @SuppressWarnings(Array("org.wartremover.warts.Equals"))
  protected[this] def parseNull(i: Int): Unit =
    if (at(i + 1) == 'u' && at(i + 2) == 'l' && at(i + 3) == 'l') {
      val _ = plate.nul()
      ()
    } else {
      die(i, "expected null")
    }

  /**
   * Parse and return the next JSON value and the position beyond it.
   */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Throw",
      "org.wartremover.warts.Recursion",
      "org.wartremover.warts.Null"))
  protected[this] def parse(i: Int): Int = try {
    (at(i): @switch) match {
      // ignore whitespace
      case ' ' => parse(i + 1)
      case '\t' => parse(i + 1)
      case '\r' => parse(i + 1)
      case '\n' => newline(i); parse(i + 1)

      // if we have a recursive top-level structure, we'll delegate the parsing
      // duties to our good friend rparse().
      case '[' => rparse(ARRBEG, i + 1, 0L, 0, null)
      case '{' => rparse(OBJBEG, i + 1, 1L, 0, null)

      // we have a single top-level number
      case '-' | '0' | '1' | '2' | '3' | '4' | '5' | '6' | '7' | '8' | '9' =>
        val j = parseNumSlow(i)
        plate.finishRow()
        j

      // we have a single top-level string
      case '"' =>
        val _ = parseString(i, false)
        plate.finishRow()
        curr

      // we have a single top-level constant
      case 't' =>
        parseTrue(i)
        plate.finishRow()
        i + 4

      case 'f' =>
        parseFalse(i)
        plate.finishRow()
        i + 5

      case 'n' =>
        parseNull(i)
        plate.finishRow()
        i + 4

      // invalid
      case _ => die(i, "expected json value")
    }
  } catch {
    case _: IndexOutOfBoundsException =>
      throw IncompleteParseException("exhausted input")
  }

  /**
   * Tail-recursive parsing method to do the bulk of JSON parsing.
   *
   * This single method manages parser states, data, etc. Except for
   * parsing non-recursive values (like strings, numbers, and
   * constants) all important work happens in this loop (or in methods
   * it calls, like reset()).
   *
   * Currently the code is optimized to make use of switch
   * statements. Future work should consider whether this is better or
   * worse than manually constructed if/else statements or something
   * else. Also, it may be possible to reorder some cases for speed
   * improvements.
   */
  @tailrec
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.NonUnitStatements",
      "org.wartremover.warts.Equals"))
  protected[this] def rparse(state: Int, j: Int, ring: Long, offset: Int, fallback: BList): Int = {
    val i = reset(j)
    checkpoint(state, i, ring, offset, fallback)

    val c = at(i)

    if (c == '\n') {
      newline(i)
      rparse(state, i + 1, ring, offset, fallback)
    } else if (c == ' ' || c == '\t' || c == '\r') {
      rparse(state, i + 1, ring, offset, fallback)
    } else if (state == DATA) {
      // we are inside an object or array expecting to see data
      if (c == '[') {
        var offset2 = offset
        var ring2 = ring
        var fallback2 = fallback

        if (checkPushEnclosure(ring, offset, fallback)) {
          offset2 = offset + 1
          ring2 = pushEnclosureRing(ring, offset, false)
        } else {
          fallback2 = pushEnclosureFallback(fallback, false)
        }

        rparse(ARRBEG, i + 1, ring2, offset2, fallback2)
      } else if (c == '{') {
        var offset2 = offset
        var ring2 = ring
        var fallback2 = fallback

        if (checkPushEnclosure(ring, offset, fallback)) {
          offset2 = offset + 1
          ring2 = pushEnclosureRing(ring, offset, true)
        } else {
          fallback2 = pushEnclosureFallback(fallback, true)
        }

        rparse(OBJBEG, i + 1, ring2, offset2, fallback2)
      } else {
        if ((c >= '0' && c <= '9') || c == '-') {
          val j = parseNum(i)
          rparse(if (enclosure(ring, offset, fallback)) OBJEND else ARREND, j, ring, offset, fallback)
        } else if (c == '"') {
          parseString(i, false)
          rparse(if (enclosure(ring, offset, fallback)) OBJEND else ARREND, curr, ring, offset, fallback)
        } else if (c == 't') {
          parseTrue(i)
          rparse(if (enclosure(ring, offset, fallback)) OBJEND else ARREND, i + 4, ring, offset, fallback)
        } else if (c == 'f') {
          parseFalse(i)
          rparse(if (enclosure(ring, offset, fallback)) OBJEND else ARREND, i + 5, ring, offset, fallback)
        } else if (c == 'n') {
          parseNull(i)
          rparse(if (enclosure(ring, offset, fallback)) OBJEND else ARREND, i + 4, ring, offset, fallback)
        } else {
          die(i, "expected json value")
        }
      }
    } else if (
      (c == ']' && (state == ARREND || state == ARRBEG)) ||
      (c == '}' && (state == OBJEND || state == OBJBEG))
    ) {
      // we are inside an array or object and have seen a key or a closing
      // brace, respectively.

      var offset2 = offset
      var fallback2 = fallback

      if (checkPopEnclosure(ring, offset, fallback))
        offset2 = offset - 1
      else
        fallback2 = popEnclosureFallback(fallback)

      (state: @switch) match {
        case ARRBEG => plate.arr()
        case OBJBEG => plate.map()
        case ARREND | OBJEND => plate.unnest()
      }

      if (offset2 < 0) {
        plate.finishRow()
        i + 1
      } else {
        rparse(if (enclosure(ring, offset2, fallback2)) OBJEND else ARREND, i + 1, ring, offset2, fallback2)
      }
    } else if (state == KEY) {
      // we are in an object expecting to see a key.
      if (c == '"') {
        if (parseString(i, true)) {
          rparse(SEP, curr, ring, offset, fallback)
        } else {
          val i2 = rskip(SKIP_MAIN, curr)
          plate.skipped(i2 - curr)
          rparse(OBJEND, i2, ring, offset, fallback)
        }
      } else {
        die(i, "expected \"")
      }
    } else if (state == SEP) {
      // we are in an object just after a key, expecting to see a colon.
      if (c == ':') {
        rparse(DATA, i + 1, ring, offset, fallback)
      } else {
        die(i, "expected :")
      }
    } else if (state == ARREND) {
      // we are in an array, expecting to see a comma (before more data).
      if (c == ',') {
        plate.unnest()
        if (plate.nestArr() eq SkipColumn) {
          val i2 = rskip(SKIP_MAIN, i + 1)
          plate.skipped(i2 - (i + 1))
          rparse(ARREND, i2, ring, offset, fallback)
        } else {
          rparse(DATA, i + 1, ring, offset, fallback)
        }
      } else {
        die(i, "expected ] or ,")
      }
    } else if (state == OBJEND) {
      // we are in an object, expecting to see a comma (before more data).
      if (c == ',') {
        plate.unnest()
        rparse(KEY, i + 1, ring, offset, fallback)
      } else {
        die(i, "expected } or ,")
      }
    } else if (state == ARRBEG) {
      // we are starting an array, expecting to see data or a closing bracket.
      if (plate.nestArr() eq SkipColumn) {
        val i2 = rskip(SKIP_MAIN, i)
        plate.skipped(i2 - i)
        rparse(ARREND, i2, ring, offset, fallback)
      } else {
        rparse(DATA, i, ring, offset, fallback)
      }
    } else {
      // we are starting an object, expecting to see a key or a closing brace.
      rparse(KEY, i, ring, offset, fallback)
    }
  }

  @tailrec
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.NonUnitStatements",
      "org.wartremover.warts.Equals"))
  private[this] final def rskip(state: Int, j: Int): Int = {
    // we might miss parse errors within a skip block (e.g. closing an object with ']'). this is by design
    val i = reset(j)

    // don't bother checkpointing every time when we skip
    val c = if (i > unsafeLen() - 1) {
      plate.skipped(i - curr)
      checkpoint(state, i, ring, roffset, fallback)
      at(i)
    } else {
      // skip the redundant bounds check
      unsafeData()(i).toChar
    }

    if (c == '\n') {
      newline(i)
    }

    // we're hiding our structural depth within the high-order bits to avoid extra integers
    val realState = state & ~SKIP_DEPTH_MASK
    val skipDepthBits = state & SKIP_DEPTH_MASK
    val skipDepth = skipDepthBits >> SKIP_DEPTH_SHIFT

    (realState: @switch) match {
      case SKIP_MAIN =>
        (c: @switch) match {
          case '"' => rskip(skipDepthBits | SKIP_STRING, i + 1)

          case '{' | '[' =>
            val skipDepth2 = skipDepth + 1
            if (skipDepth2 >= SKIP_DEPTH_LIMIT) error("cannot skip over structure with more than 134,217,727 levels of nesting")
            val skipDepthBits = skipDepth2 << SKIP_DEPTH_SHIFT
            rskip(skipDepthBits | SKIP_MAIN, i + 1)

          case ']' | '}' =>
            if (skipDepth <= 0) {
              i
            } else {
              val skipDepthBits = (skipDepth - 1) << SKIP_DEPTH_SHIFT
              rskip(skipDepthBits | SKIP_MAIN, i + 1)
            }

          case ',' if skipDepth == 0 => i

          case _ => rskip(state, i + 1)
        }

      case SKIP_STRING =>
        (c: @switch) match {
          case '\\' => rskip(skipDepthBits | SKIP_STRING, i + 2)
          case '"' => rskip(skipDepthBits | SKIP_MAIN, i + 1)
          case _ => rskip(state, i + 1)
        }

      case _ =>
        error("invalid state in rskip: " + state.toString)
    }
  }

  /**
   * A value of true indicates an object, and false indicates an array. Note that
   * a non-existent enclosure is indicated by offset < 0
   */
  @inline
  @SuppressWarnings(Array("org.wartremover.warts.Equals"))
  private[this] final def enclosure(ring: Long, offset: Int, fallback: BList): Boolean = {
    if (fallback == null)
      (ring & (1L << offset)) != 0
    else
      fallback.head
  }

  @inline
  @SuppressWarnings(Array("org.wartremover.warts.Equals"))
  private[this] final def checkPushEnclosure(ring: Long, offset: Int, fallback: BList): Boolean =
    fallback == null

  @inline
  private[this] final def pushEnclosureRing(ring: Long, offset: Int, enc: Boolean): Long = {
    if (enc)
      ring | (1L << (offset + 1))
    else
      ring & ~(1L << (offset + 1))
  }

  @inline
  private[this] final def pushEnclosureFallback(fallback: BList, enc: Boolean): BList =
    enc :: fallback

  @inline
  @SuppressWarnings(Array("org.wartremover.warts.Equals"))
  private[this] final def checkPopEnclosure(ring: Long, offset: Int, fallback: BList): Boolean =
    fallback == null

  @inline
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.IsInstanceOf",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null"))
  private[this] final def popEnclosureFallback(fallback: BList): BList = {
    if (fallback.isInstanceOf[BList.Last])
      null
    else
      fallback.asInstanceOf[BList.Cons].tail
  }
}

object Parser {

  sealed abstract class Mode(val start: Int, val value: Int)
  case object UnwrapArray extends Mode(-5, 1)
  case object ValueStream extends Mode(-1, 0)
  case object SingleValue extends Mode(-1, -1)

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.DefaultArguments",
      "org.wartremover.warts.Null"))
  def apply[F[_]: Sync, A](plateF: F[Plate[A]], mode: Mode = SingleValue) : F[BaseParser[F, A]] = {
    plateF flatMap { plate =>
      Sync[F].delay(new Parser(plate, state = mode.start,
        ring = 0L, roffset = -1, fallback = null,
        streamMode = mode.value))
    }
  }
}
