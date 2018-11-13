# Tectonic [![Build Status](https://travis-ci.org/slamdata/tectonic.svg?branch=master)](https://travis-ci.org/slamdata/tectonic) [![Bintray](https://img.shields.io/bintray/v/slamdata-inc/maven-public/tectonic.svg)](https://bintray.com/slamdata-inc/maven-public/tectonic) [![Discord](https://img.shields.io/discord/373302030460125185.svg?logo=discord)](https://discord.gg/QNjwCg6)

A columnar fork of [Jawn](https://github.com/non/Jawn) with added backend support for CSV. The distinction between "columnar" and its asymmetric opposite, "row-oriented", is in the orientation of data structures which you are expected to create in response to the event stream. Jawn expects a single, self-contained value with internal recursive structure per row, and its `Facade` trait is designed around this idea. Tectonic expects many rows to be combined into a much larger batch with a flat internal structure, and the `Plate` class is designed around this idea.

Tectonic is also designed to support multiple backends, making it possible to write a parser for any sort of input stream (e.g. CSV, XML, etc) while driving a single `Plate`. At present, both CSV and JSON are supported, and we have plans to support XML in the future.

Finally, Tectonic implements a form of fast *skipping* based on signals returned from the user-supplied `Plate` implementation driving a particular parse. In this way, it is possible to achieve [Mison](https://www.semanticscholar.org/paper/Mison%3A-A-Fast-JSON-Parser-for-Data-Analytics-Li-Katsipoulakis/6ac1276a15ae28ee7a1f3d0d7b323c1a5c55a86f)-style projection pushdown into the parse process. At present, skip scans do *not* compile down to vectorized assembly instructions ([SIMD](https://en.wikipedia.org/wiki/SIMD)), but it is theoretically possible to achieve this, despite sitting on top of the JVM.

All of these differences have led to some relatively meaningful changes within the parser implementation. Despite that, the bulk of the ideas and, in some areas, the *vast* bulk of the implementation of Tectonic's JSON parser is drawn directly from Jawn. **Special heartfelt thanks to [Erik Osheim](https://github.com/d6) and the rest of the Jawn contributors, without whom this project would not be possible.**

Tectonic is very likely the optimal JSON parser on the JVM for producing columnar data structures. It is *definitely* the optimal JSON parser for producing columnar structures when you are able to compute some projections or row filters in advance, allowing skipping. When producing row-oriented structures (such as conventional JSON ASTs), it falls considerably behind Jawn both in terms of performance and usability. Tectonic is also relatively opinionated in that it assumes you will be applying it to variably-framed input stream (corresponding to Jawn's `AsyncParser`) and does not provide any other operating modes.

## Usage

```sbt
libraryDependencies += "com.slamdata" %% "tectonic" % <version>

// if you wish to use Tectonic with fs2 (recommended)
libraryDependencies += "com.slamdata" %% "tectonic-fs2" % <version>
```

If using Tectonic via fs2, you can take advantage of the `StreamParser` `Pipe` to perform all of the heavy lifting:

```scala
import cats.effect.IO

import tectonic.json.Parser
import tectonic.fs2.StreamParser

// assuming MyPlate.apply[F[_]] returns an F[Plate[A]]
val parserF = 
  Parser(MyPlate[IO], Parser.ValueStream)) // assuming whitespace-delimited json

val input: Stream[IO, Byte] = ...
input.through(StreamParser(parserF))    // => Stream[IO, Foo]
```

Parse errors will be captured by the stream as exceptions.

## Backend Formats

Tectonic supports two formats: JSON and CSV. Each format has a number of different configuration modes which may be defined.

### JSON

There are three modes in which the JSON parser may run:

- Whitespace-Delimited (`Parser.ValueStream`)
- Array-Wrapped (`Parser.UnwrapArray`)
- Just One Value (`Parser.SingleValue`)

Whitespace-delimited is very standard when processing very large JSON input streams, where the whitespace is likely to be a newline. "Just One Value" is quite uncommon, since it only applies to scenarios wherein you have a *single* row in the data. Array wrapping is common, but not universal. Any one of these modes may be passed to the parser upon initialization.

### CSV

Similar to the JSON parser, the CSV parser takes its configuration as a parameter. However, the configuration is considerably more complex since there is no standard CSV mode. Delimiters and escaping vary considerably from instance to instance. Some CSV files even fail to include a header indicating field names!

To account for this, the CSV parser accepts a `Config` object wherein all of these values are tunable. The defaults are as follows:

```scala
final case class Config(
    header: Boolean = true,
    record: Byte = ',',
    row1: Byte = '\r',
    row2: Byte = '\n',        // if this is unneeded, it should be set to \0
    openQuote: Byte = '"',    // e.g. “
    closeQuote: Byte = '"',   // e.g. ”
    escape: Byte = '"')       // e.g. \\
```

This roughly corresponds to the CSV style emitted by Microsoft Excel. Almost any values may be used here. The restrictions are as follows:

- `row2` must not equal `closeQuote`
- `record` must not equal `row1`
- `row2` may not *validly* be byte value `0`, since that is the indicator for "only use the first row delimiter"

Beyond this, any characters are valid. You will note that, in the Excel defaults, the escape character is in fact the same as the close (and open) quote characters, meaning that quoted values are enclosed in `"`...`"` and interior quote characters are represented by `""`. Backslash (`\`) is also a relatively common choice here.

Just to be clear, if you wish to use a singular `\n` as the row delimiter, you should do something like this:

```scala
Parser.Config().copy(
  row1 = '\n',
  row2 = 0)
```

If the supplied configuration defines `header = false`, then the inference will follow the same strategy that Excel uses. Namely: `A`, `B`, `C`, ..., `Z`, `AA`, `AB`, ...

When invoking the `Plate`, each CSV record will be represented as a `str(...)` invocation, wrapped in a `nestMap`/`unnest` call, where the corresponding header value is the map key.

## Leveraging `SkipColumn`/`SkipRow`

Depending on your final output representation and exactly what actions you're planning to perform on that representation, you may find that you don't need *all* of the data. A particularly common case (especially in columnar representations) is projection pushdown. Imagine you're evaluating an operation which is conceptually like the following SQL:

```sql
SELECT a + b FROM foo.json
```

The *foo.json* input data may contain many, *many* more columns than just `.a` and `.b`, and those columns may have significant substructure, numerics (which are often extremely expensive to consume), and more. In such a case, it is often extremely beneficial to instruct the parser to efficiently scan past bytes which are constituent to this superfluous structure, never even generating the `Plate` events which correspond to that input (see below for a rough benchmark measurement of *how* beneficial this can be in a contrived – but not necessarily best-case – scenario).

This technique was pioneered (to our knowledge) by IBM with the [Mison Parser](https://www.semanticscholar.org/paper/Mison%3A-A-Fast-JSON-Parser-for-Data-Analytics-Li-Katsipoulakis/6ac1276a15ae28ee7a1f3d0d7b323c1a5c55a86f), and then later expanded upon by the Future Data team at Stanford in the [Sparser](https://dawn.cs.stanford.edu/2018/08/07/sparser/) project.

Tectonic implements this technique through the use of the `Signal` ADT, which is returned from most event-carrying methods on `Plate`:

```scala
sealed trait Signal

object Signal {
  case object SkipRow extends Signal
  case object SkipColumn extends Signal
  case object Continue extends Signal
  case object Terminate extends Signal
}
```

The meaning of these various signals is as follows:

- `SkipRow`: Efficiently scan to the end of the current *row*, run `FinishRow`, and then proceed with normal parsing. Useful for implementing *filter* pushdown (`WHERE` in SQL)
- `SkipColumn`: Efficiently scan to the end of the current *column* and then proceed with normal parsing. Useful for implementing *projection* pushdown (`SELECT` in SQL). Ignored when returned from anything other than a `nest` method (it's impossible to skip something after you've already parsed it).
- `Continue`: Proceed as usual
- `Terminate`: Halt the parse immediately with an error

**All signals are *hints*.** The underlying parser backend is always free to ignore them. Your `Plate` implementation should not assume that things will actually be skipped, since under certain circumstances (depending on the parser state machine and the nature of the input data format), skipping may not be possible. With that said, it is still best practice to produce the appropriate `Skip` whenever you can, as it can result in massive performance gains that are otherwise out of reach from user-land code.

Returning to the example above, you can use the `DelegatingPlate` utility class to implement projection pushdown of `.a` and `.b` via `SkipColumn` in a relatively straightforward fashion:

```scala
def pushdown[F[_]: Sync, A](delegate: Plate[A]): F[Plate[A]] = {
  Sync[F] delay {
    new DelegatingPlate[A](delegate) {
      private var depth = 0
      private var skipping = false

      override def nestMap(name: CharSequence): Signal = {
        if (depth == 0) {
          depth += 1
          if (name.toString == "a" || name.toString == "b") {
            super.nestMap(name)
          } else {
            skipping = true
            Signal.SkipColumn
          }
        } else {
          depth += 1
          Signal.SkipColumn
        }
      }

      override def nestArr(): Signal = {
        depth += 1
        if (skipping)
          Signal.SkipColumn
        else
          super.nestArr()
      }

      override def nestMeta(name: CharSequence): Signal = {
        depth += 1
        if (skipping)
          Signal.SkipColumn
        else
          super.nestMeta(name)
      }

      override def unnest(): Signal = {
        depth -= 1
        if (depth == 0 && skipping) {
          skipping = false
          Signal.Continue
        } else {
          super.unnest()
        }
      }
    }
  }
}
```

There's a fair bit of boilerplate here needed to track the depth of nesting. If `SkipColumn` were not a hint but rather an absolute mandate, this would be somewhat easier. Either way though, hopefully the general idea is clear. Wrapping this function around any other `Plate` will result in that delegate plate receiving almost exactly the same event stream that it *would* have received if the underlying dataset exclusively contained `.a` and `.b` columns.

We say "almost exactly" because there is one difference: `skipped`. The `Plate#skipped(Int)` method will be invoked one or more times by the backend any time a chunk of bytes is skipped due to a `SkipColumn` or `SkipRow` signal. The `Int` parameter represents the number of bytes skipped. Under some circumstances, this value may be `0`, and absolute accuracy is not *guaranteed* (though you can probably rely on it to be within 1 or 2 bytes each time). The default implementation of this function on `Plate` is to simply ignore the event.

The purpose of `skipped` is to allow `Plate`s to efficiently build up metrics on data even when it isn't consumed. This can be extremely useful for some types of algorithms.

### Parse Errors

Tectonic backends are free to bypass error discovery in bytes which are ingested during a skip. For example, imagine a `SkipColumn` on the `.a` column in the following data:

```
{ "a": [1, 2}, "b": 42 }
```

Notice the mismatched braces (`[` with `}`) within the `.a` column. If the `.a` column is skipped, this will *not* be considered a parse error by Tectonic JSON. This may seem somewhat bizarre, but it's actually quite important. Detecting errors in the input stream is actually a relatively expensive process (for example, it requires a stack representation to track array vs map nesting). The skip scan can be made *vastly* more efficient when error detection is discarded.

Error detection resumes appropriately as soon as the skip scan finishes. Thus, any data which is *not* skipped will be appropriately checked for parse errors.

## Performance

Broadly-speaking, the performance of Tectonic JSON is roughly on-par with Jawn. Depending on your measurement assumptions, it may actually be slightly faster. It's very very difficult to setup a fair series of measurements, due to the fact that Tectonic produces batched columnar data while Jawn produces rows on an individual basis.

Our solution to this is to use the JMH `Blackhole.consumeCPU` function in a special Jawn `Facade` and Tectonic `Plate`. Each event is implemented with a particular consumption, weighted by the following constants:

```scala
object FacadeTuningParams {
  object Tectonic {
    val VectorCost: Long = 4   // Cons object allocation + memory store
    val TinyScalarCost: Long = 8    // hashmap get + bitset put
    val ScalarCost: Long = 16   // hashmap get + check on array + amortized resize/allocate + array store
    val RowCost: Long = 2   // increment integer + bounds check + amortized reset
    val BatchCost: Long = 1   // (maybe) reset state + bounds check
  }

  val NumericCost: Long = 512   // scalarCost + crazy numeric shenanigans

  object Jawn {
    val VectorAddCost: Long = 32   // hashmap something + checks + allocations + stuff
    val VectorFinalCost: Long = 4   // final allocation + memory store
    val ScalarCost: Long = 2     // object allocation
    val TinyScalarCost: Long = 1   // non-volatile memory read
  }
}
```

- Vectors are arrays or objects
- Tiny scalars are any scalars which can be implemented with particularly concise data structures (e.g. `jnull` for Jawn, or `arr` for Tectonic)
- Scalars are everything else
- Numerics are special, since we assume realistic facades will be performing numerical parsing to ensure maximally efficient representations. Thus, both facades check for decimal and exponent. If these are lacking, then the cost paid is the `ScalarCost`. If these are present, then the cost paid is `NumericCost`, simulating a trip through `BigDecimal`.
- `Add` and `Final` costs for Jawn are referring to the `Context` functions, which are generally implemented with growable, associative data structures set to small sizes

Broadly, these costs were strongly inspired by the internal data structures of [the Mimir database engine](https://github.com/slamdata/quasar) and [the QData representation](https://github.com/slamdata/qdata). In the direct comparison measurements, `Signal.Continue` is universally assumed as the result from `Plate`, voiding any advantages Tectonic can derive from projection/predicate pushdown.

Both frameworks are benchmarked *through* [fs2](https://fs2.io) (in the case of Jawn, using [jawn-fs2](https://github.com/http4s/jawn-fs2)), which is assumed to be the mode in which both parsers will be run. This allows the benchmarks to capture nuances such as `ByteVectorChunk` handling, `ByteBuffer` unpacking and such.

As an aside, even apart from the columnar vs row-oriented data structures, Tectonic does have some meaningful optimizations relative to Jawn. In particular, Tectonic is able to maintain a much more efficient object/array parse state due to the fact that it is not relying on an implicit stack of `Context`s to maintain that state for it. This is particularly noticeable for object/array nesting depth less than 64 levels, which seems to be far-and-away the most common case.

### Benchmark Comparison to Jawn

The following were run on my laptop in powered mode with networking disabled, 20 warmup iterations and 20 measurement runs in a forked JVM. You can find all of the sources in the **benchmarks** subproject. Please note that these results are extremely dependent on the assumptions codified in `FacadeTuningParams`. Lower numbers are better.

| Framework     |                 Input | Milliseconds |   Error |
| ---           |                   --: |          --: |     :-- |
|      tectonic |            `bar.json` |        0.092 | ± 0.001 |
|      **jawn** |            `bar.json` |    **0.090** | ± 0.002 |
|  **tectonic** |           `bla2.json` |    **0.424** | ± 0.003 |
|          jawn |           `bla2.json` |        0.527 | ± 0.004 |
|  **tectonic** |         `bla25.json`  |   **15.603** | ± 0.121 |
|          jawn |         `bla25.json`  |       20.814 | ± 0.180 |
|  **tectonic** |  `countries.geo.json` |   **26.435** | ± 0.193 |
|          jawn |  `countries.geo.json` |       28.551 | ± 0.216 |
|  **tectonic** |     `dkw-sample.json` |    **0.116** | ± 0.001 |
|          jawn |     `dkw-sample.json` |        0.121 | ± 0.002 |
|  **tectonic** |           `foo.json`  |    **0.566** | ± 0.005 |
|          jawn |           `foo.json`  |        0.608 | ± 0.005 |
|  **tectonic** |           `qux1.json` |    **9.973** | ± 0.063 |
|          jawn |           `qux1.json` |       10.863 | ± 0.062 |
|  **tectonic** |           `qux2.json` |   **17.717** | ± 0.096 |
|          jawn |           `qux2.json` |       19.153 | ± 0.119 |
|  **tectonic** |        `ugh10k.json`  |  **115.467** | ± 0.838 |
|          jawn |        `ugh10k.json`  |      130.876 | ± 0.972 |

#### Column Skip Benchmarks

Column skips – otherwise known as *projection pushdown* – are one of the unique features of the Tectonic parsing framework. The exact performance benefits you gain from returning `SkipColumn` vary considerably depending on exactly when you skip (e.g. one column, or a lot of columns), what your data looks like, and how expensive your `Plate` implementation is on a per-scalar basis. We can get a general idea of the performance impact of skipping though by contriving a benchmark based on the `ugh10k.json` dataset, which is not *particularly* wide, but has enough substructure to demonstrate interesting skipping.

The following benchmark selects just the `.bar` field out of the top-level object in each row, skipping *everything* else. It's being run in tectonic using the `FacadeTuningParams` from above, both with and without skips to demonstrate the impact:

| Skips? | Milliseconds | Error     |
| ---    | --:          | :--       |
| 😁     | **33.716**   | **0.231** |
| 😑     | 117.025      | 0.978     |

So that's a roughly **3.47x** performance jump. It's difficult to draw general conclusions from this data, other than the fact that having skips is better than not having skips. In testing on realistic workloads in Spark, IBM found that their Mison parser improved overall batch run times by up to 20x. Tectonic is (presently) unable to benefit from the CPU vectorized instructions that Mison is using to achieve theoretically optimal skip scan performance, but it's at least *theoretically* capable of hitting those kinds of performance gains on certain datasets with amenable workloads. 

At the very least, skipping is never a *bad* idea.

### Row-Counting Benchmark for CSV

Inspired by the [uniVocity benchmarks](https://github.com/uniVocity/csv-parsers-comparison#jdk-8), the Tectonic CSV benchmarks load from the 144 MB [Maxmind worldcities.csv](http://www.maxmind.com/download/worldcities/worldcitiespop.txt.gz) dataset, counting the total number of records. Unfortunately, we were unable to find any other async (streaming) CSV parsers on the JVM, and so the only performance comparison we are able to provide is to the time it takes to count the number of `\n` characters in the same file.

| Test       | Milliseconds | Error    |
| ---        |          --: | :--      |
| Parse      | 2023.705     | ± 27.075 |
| Line Count | 225.589      | ± 4.474  |

The line count test really just serves as a lower-bound on how long it takes fs2-io to read the file contents. Thus, *parsing* as opposed to just *scanning the characters* adds a roughly an order of magnitude in overhead. However, if you compare to the uniVocity benchmarks, which were performed on a more powerful machine and *without* the overhead of fs2, it appears that Tectonic CSV is relatively middle-of-the-road in terms of high performance CSV parsers on the JVM. That's with almost no time spent optimizing (there's plenty of low-hanging fruit) *and* the fact that Tectonic CSV is an async parser, which imposes some meaningful overhead.

## License

To the extent that lines of code have been copied from the Jawn codebase, they retain their original copyright and license, which is [The MIT License](https://opensource.org/licenses/MIT). Original code which is unique to Tectonic is licensed under [The Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0), copyright [SlamData](https://slamdata.com). Files which are substantially drawn from Jawn retain *both* copyright headers, as well as a special thank-you to the Jawn contributors.
