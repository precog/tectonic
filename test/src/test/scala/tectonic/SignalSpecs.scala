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
package test

import tectonic.Signal.{Continue, SkipColumn, SkipRow, Terminate}

import org.specs2.mutable.Specification

class SignalSpecs extends Specification {

  "Signal.and" should {

    "compare pairs" >> {

      "(Continue, Continue)" in {
        Signal.and(Continue, Continue) mustEqual Continue
      }

      "(Continue, SkipColumn)" in {
        Signal.and(Continue, SkipColumn) mustEqual Continue
      }

      "(Continue, SkipRow)" in {
        Signal.and(Continue, SkipRow) mustEqual Continue
      }

      "(Continue, Terminate)" in {
        Signal.and(Continue, Terminate) mustEqual Continue
      }

      "(SkipColumn, Continue)" in {
        Signal.and(SkipColumn, Continue) mustEqual Continue
      }

      "(SkipColumn, SkipColumn)" in {
        Signal.and(SkipColumn, SkipColumn) mustEqual SkipColumn
      }

      "(SkipColumn, SkipRow)" in {
        Signal.and(SkipColumn, SkipRow) mustEqual SkipColumn
      }

      "(SkipColumn, Terminate)" in {
        Signal.and(SkipColumn, Terminate) mustEqual Continue
      }

      "(SkipRow, Continue)" in {
        Signal.and(SkipRow, Continue) mustEqual Continue
      }

      "(SkipRow, SkipColumn)" in {
        Signal.and(SkipRow, SkipColumn) mustEqual SkipColumn
      }

      "(SkipRow, SkipRow)" in {
        Signal.and(SkipRow, SkipRow) mustEqual SkipRow
      }

      "(SkipRow, Terminate)" in {
        Signal.and(SkipRow, Terminate) mustEqual Continue
      }

      "(Terminate, Continue)" in {
        Signal.and(Terminate, Continue) mustEqual Continue
      }

      "(Terminate, SkipColumn)" in {
        Signal.and(Terminate, SkipColumn) mustEqual Continue
      }

      "(Terminate, SkipRow)" in {
        Signal.and(Terminate, SkipRow) mustEqual Continue
      }

      "(Terminate, Terminate)" in {
        Signal.and(Terminate, Terminate) mustEqual Terminate
      }
    }
  }
}
