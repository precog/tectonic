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
package test

import org.scalacheck.{Arbitrary, Gen}

import scala.{AnyVal, Array, Boolean, Char, Int, List, Nil, Predef, Unit}, Predef._
import scala.language.postfixOps

import java.lang.SuppressWarnings

object Generators {
  import Arbitrary.arbitrary

  type GenF[A] = Gen[Plate[A] => Unit]

  // the existing one is terrible and filters things bizarrely
  implicit def arbitraryList[A: Arbitrary]: Arbitrary[List[A]] =
    Arbitrary(genList[A])

  def genList[A: Arbitrary]: Gen[List[A]] =
    Gen.oneOf(Gen.const(Nil), Gen.delay(genCons[A]))

  def genCons[A: Arbitrary]: Gen[List[A]] =
    arbitrary[A].flatMap(hd => genList[A].map(tl => hd :: tl))

  implicit def arbitraryPlateF[A]: Arbitrary[∀[λ[α => Plate[α] => Unit]]] = {
    // technically this is all safe because we're skolemizing on the Unit
    val gen = genPlate[Unit] map { f =>
      new ∀[λ[α => Plate[α] => Unit]] {
        @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
        def apply[α] = f.asInstanceOf[Plate[α] => Unit]
      }
    }

    Arbitrary(gen)
  }

  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  def genPlate[A]: GenF[A] = {
    val genSubPlate = Gen.frequency(
      20000 -> genRow[A] *>> genFinishRow[A],
      1 -> genFinishBatch[A])

    for {
      gfs <- Gen.containerOf[List, Plate[A] => Unit](genSubPlate)
      gff <- genFinishBatch[A]
    } yield { p =>
      gfs.foreach(_(p))
      gff(p)
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  def genRow[A]: GenF[A] =
    Gen.frequency(
      1 -> genNul[A],
      1 -> genFls[A],
      1 -> genTru[A],
      1 -> genMap[A],
      1 -> genArr[A],
      1 -> genNum[A],
      1 -> genStr[A],
      5 -> Gen.delay(genNestMap[A] *>> genRow[A] *>> genUnnest[A]),
      5 -> Gen.delay(genNestArr[A] *>> genRow[A] *>> genUnnest[A]),
      1 -> Gen.delay(genNestMeta[A] *>> genRow[A] *>> genUnnest[A]),
      1 -> genSkipped[A])

  def genFinishRow[A]: GenF[A] =
    Gen.const(p => p.finishRow())

  def genFinishBatch[A]: GenF[A] = {
    Gen const { p =>
      val _ = p.finishBatch(false)
      ()
    }
  }

  def genNul[A]: GenF[A] = {
    Gen const { p =>
      val _ = p.nul()
      ()
    }
  }

  def genFls[A]: GenF[A] = {
    Gen const { p =>
      val _ = p.fls()
      ()
    }
  }


  def genTru[A]: GenF[A] = {
    Gen const { p =>
      val _ = p.tru()
      ()
    }
  }

  def genMap[A]: GenF[A] = {
    Gen const { p =>
      val _ = p.map()
      ()
    }
  }

  def genArr[A]: GenF[A] = {
    Gen const { p =>
      val _ = p.arr()
      ()
    }
  }

  // I mean, it's not that hard to do this right. may as well
  // note that this is more robust than generating doubles since we can get much larger numbers
  @SuppressWarnings(Array("org.wartremover.warts.StringPlusAny"))
  def genNum[A]: GenF[A] = {
    val genNumberStr =
      Gen.containerOf[λ[α => String], Char](Gen.choose('0', '9')).filter(_.length > 0)

    for {
      negative <- arbitrary[Boolean]
      integer <- genNumberStr

      hasDecimal <- arbitrary[Boolean]
      decimal <- if (hasDecimal)
        genNumberStr.map("." +)
      else
        Gen.const("")

      hasExponent <- arbitrary[Boolean]
      exponent <- if (hasExponent)
        genNumberStr.map("e" +)
      else
        Gen.const("")

      str = (if (negative) "-" else "") + integer + decimal + exponent
    } yield { p =>
      val _ = p.num(str, str.indexOf("."), str.indexOf("e"))
      ()
    }
  }

  def genStr[A]: GenF[A] = {
    arbitrary[String] map { s =>
      { p =>
        val _ = p.str(s)
        ()
      }
    }
  }

  def genNestMap[A]: GenF[A] = {
    arbitrary[String] map { s =>
      { p =>
        val _ = p.nestMap(s)
        ()
      }
    }
  }

  def genNestArr[A]: GenF[A] = {
    Gen const { p =>
      val _ = p.nestArr()
      ()
    }
  }

  def genNestMeta[A]: GenF[A] = {
    arbitrary[String] map { s =>
      { p =>
        val _ = p.nestMeta(s)
        ()
      }
    }
  }

  def genUnnest[A]: GenF[A] = {
    Gen const { p =>
      val _ = p.unnest()
      ()
    }
  }

  def genSkipped[A]: GenF[A] = {
    Gen.posNum[Int].filter(_ > 0) map { i =>
      { p =>
        val _ = p.skipped(i)
        ()
      }
    }
  }

  private implicit class GenFSyntax[A](val gf: Gen[A => Unit]) extends AnyVal {
    def *>>(gf2: Gen[A => Unit]): Gen[A => Unit] = {
      for {
        f <- gf
        f2 <- gf2
      } yield { a =>
        f(a)
        f2(a)
      }
    }
  }

  trait ∀[F[_]] {
    def apply[A]: F[A]
  }
}
