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

import scala.Int

sealed class Signal(final val ordinal: Int)

object Signal {
  case object Continue extends Signal(1)
  case object SkipColumn extends Signal(2)
  case object SkipRow extends Signal(-2)
  case object Terminate extends Signal(3)

  private val _Continue = Continue
  private val _SkipColumn = SkipColumn

  def and(s1: Signal, s2: Signal): Signal = {
    val s1o: Int = s1.ordinal
    val s2o: Int = s2.ordinal

    if (s1o == s2o)
      s1
    else if ((s1o + s2o) == 0)
      _SkipColumn
    else
      _Continue
  }
}
