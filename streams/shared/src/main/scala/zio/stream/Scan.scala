package zio.stream

import zio._
import zio.stream.Scan.SafeFn

import scala.annotation.tailrec
import scala.util.hashing.MurmurHash3

sealed trait Scan[-I, +O] { self =>
  type State
  def initial: State
  def transform: ((State, I)) => (State, Chunk[O])
  def onComplete: State => Chunk[O]

  final def transformAccumulate(s: State, inputs: Chunk[I]): (State, Chunk[O]) = {
    var state = s
    val builder = ChunkBuilder.make[O]()
    inputs.foreach { i =>
      val (sNext, out) = transform((state, i))
      builder ++= out
      state = sNext
    }
    (state, builder.result())
  }

  final def toChannel: ZChannel[Any, Nothing, Chunk[I], Any, Nothing, Chunk[O], State] =
    ZChannel.fromZIO(ZIO.succeed(initial)).flatMap { initState =>
      def process[E](state: State): ZChannel[Any, E, Chunk[I], Any, Nothing, Chunk[O], State] =
        ZChannel.readWithCause(
          (in: Chunk[I]) => {
            val (newState, outs) = transformAccumulate(state, in)
            ZChannel.write(outs) *> process(newState)
          },
          (err: Cause[E]) => ZChannel.refailCause(err.stripFailures),
          _ => ZChannel.write(onComplete(state)) *> ZChannel.succeed(state)
        )
      process(initState)
    }

  final def toPipeline: ZPipeline[Any, Nothing, I, O] =
    toChannel.toPipeline

  final def toSink: ZSink[Any, Nothing, I, O, State] =
    toChannel.toSink


  final def dimap[I2, O2](f: I2 => I)(g: O => O2): Scan.Aux[State, I2, O2] = {
    new Scan[I2, O2] {
      type State = self.State
      val initial = self.initial
      val transform = self.transform
        .compose[(self.State, I2)] { case (s, i2) => (s, f(i2)) }
        .andThen { case (s, os) => (s, os.map(g)) }
      val onComplete = self.onComplete.andThen(_.map(g))
    }
  }

  final def imapState[S2](to: State => S2)(from: S2 => State): Scan.Aux[S2, I, O] = {
    new Scan[I, O] {
      type State = S2
      val initial = to(self.initial)
      val transform = SafeFn { case (s2: S2, i: I) =>
        val s = from(s2)
        val (sNext, os) = self.transform(s, i)
        (to(sNext), os)
      }
      val onComplete = self.onComplete.compose(from)
    }
  }

  final def semilens[I2, O2](extract: I2 => Either[O2, I], inject: (I2, O) => O2)
  : Scan.Aux[State, I2, O2] = {
    val f = self.transform
    new Scan[I2, O2] { inner: Scan.Aux[self.State, I2, O2] =>
      type State = self.State
      val initial: State = self.initial
      override val transform: SafeFn[(State, I2), (State, Chunk[O2])] = SafeFn {
        case (s: State, i2: I2) =>
          extract(i2) match {
            case Left(o2) => (s, Chunk.single(o2))
            case Right(i) =>
              val (sNext, os) = f(s, i)
              (sNext, os.map(o => inject(i2, o)))
          }
      }
      val onComplete = SafeFn.emptyFn
    }
  }

  final def left[A]: Scan.Aux[State, Either[I, A], Either[O, A]] =
    semilens[Either[I, A], Either[O, A]](
      {
        case Left(i)  => Right(i)
        case Right(a) => Left(Right(a))
      },
      (_, o) => Left(o)
    )

  final def right[A]: Scan.Aux[State, Either[A, I], Either[A, O]] =
    semilens[Either[A, I], Either[A, O]](
      {
        case Left(a)  => Left(Left(a))
        case Right(i) => Right(i)
      },
      (_, o) => Right(o)
    )
}

object Scan {

  type Aux[S, -I, +O] = Scan[I, O] { type State = S }

  type Stateless[-I, +O] = Scan[I, O] { type State = Unit }

  object Stateless {
    def makeChunked[I, O](f: I => Chunk[O]): Stateless[I, O] =
      new StatelessN[I, O] { final val transformN = SafeFn(f) }

    def make[I, O](f: I => O): Stateless[I, O] =
      Stateless1(SafeFn(f))
  }

  private[stream] final case class Stateful[S0, I, O]
  (@transient initialThunk: () => S0,
   transform: SafeFn[(S0, I), (S0, Chunk[O])],
   onComplete: SafeFn[S0, Chunk[O]]
  ) extends Scan[I, O] {
    type State = S0
    lazy val initial: S0 = initialThunk()

    override lazy val hashCode: Int =
      MurmurHash3.productHash(("Stateful", initial, transform, onComplete))
  }

  private[stream] final case class AndThen[S, S2, S3, I, O, O2]
  (
    first: Aux[S, I, O],
    second: Aux[S2, O, O2],
    z: ZipUnzip.Aux[S, S2, S3]
  ) extends Scan[I, O2] {

    override lazy val hashCode: Int =
      MurmurHash3.productHash(("AndThen", first, second))

    override type State = S3

    lazy val initial: State = z.zip(first.initial, second.initial)

    final val transform = SafeFn { case (s12: State, i: I) =>
      val (s1, s2) = z.unzip(s12)
      val (s1Next, outs) = first.transform((s1, i))
      val (s2Next, outs2) = second.transformAccumulate(s2, outs)
      (z.zip(s1Next, s2Next), outs2)
    }

    final val onComplete = SafeFn(
      z.unzip(_) match {
        case (s1: first.State, s2: second.State) =>
          second.transformAccumulate(s2, first.onComplete(s1)) match {
            case (s3, outs) => outs ++ second.onComplete(s3)
          }
      }
    )
  }

  private[stream] trait StatelessN[-I, +O] extends Scan[I, O] { self: Stateless[I, O] =>

    val transformN: SafeFn[I, Chunk[O]]

    override lazy val hashCode: Int =
      MurmurHash3.productHash(("StatelessN", transformN))

    final type State = Unit
    @inline final def initial = ()
    @inline final def onComplete = SafeFn.emptyFn
    final lazy val transform =
      SafeFn.secondFn[I] andThen transformN andThen (initial -> _)
  }

  private[stream] final case class Stateless1[-I, +O]
  (transform1: SafeFn[I, O])
    extends StatelessN[I, O] {
    val transformN: SafeFn[I, Chunk[O]] = transform1.andThen(Chunk.single)
    def andThen[O2](other: Stateless1[O, O2]): Stateless1[I, O2] =
      Stateless1(transform1 andThen other.transform1)
  }

  /**
   * Builds a `Scan` from an `initial` state and two functions:
   *   1. `(S, I) => (S, Chunk[O])` 2. `S => Chunk[O]` for final emission
   */
  def apply[S, I, O](
                      initial: => S
                    )(
                      transform: ((S, I)) => (S, Chunk[O]),
                      onComplete: S => Chunk[O] = SafeFn.emptyFn
                    ): Scan.Aux[S, I, O] =
    Stateful(() => initial, SafeFn(transform), SafeFn(onComplete))

  def stateful[S, I, O](initial: => S)(f: (S, I) => (S, Chunk[O])): Scan.Aux[S, I, O] =
    apply(initial)(f.tupled)

  def stateful1[S, I, O](initial: => S)(f: (S, I) => (S, O)): Aux[S, I, O] =
    stateful(initial)((s, i) => f(s, i) match { case (s, o) => (s, Chunk.single(o)) })

  def stateless[I, O](f: I => Chunk[O]): Stateless[I, O] =
    Stateless.makeChunked(f)

  def stateless1[I, O](f: I => O): Stateless[I, O] =
    Stateless.make(f)

  def id[I]: Stateless[I, I] =
    stateless1(ZIO.identityFn[I])

  implicit class ScanOps[S, -I, +O](private val self: Scan.Aux[S, I, O]) extends AnyVal {

    def andThen[S2, O2](that: Scan.Aux[S2, O, O2])
                       (implicit z: ZipUnzip[S, S2]): Scan.Aux[z.Out, I, O2] =
      AndThen(self, that, z)

    def map[O2](f: O => O2): Aux[S, I, O2] =
      self match {
        case s if s.isInstanceOf[Stateless1[_, _]] =>
          stateless1(
            s.asInstanceOf[Stateless1[I, O]].transform1.andThen(f)
          ).asInstanceOf[Aux[S, I, O2]]
        case _ =>
          Scan(self.initial)(
            self.transform.andThen { case (s: S, os: Chunk[O2]) => (s, os.map(f)) },
            self.onComplete.andThen((_: Chunk[O]).map(f))
          )
      }

    def contramap[I2](f: I2 => I): Aux[S, I2, O] =
      self match {
        case s if s.isInstanceOf[Stateless1[_, _]] =>
          stateless1(s.asInstanceOf[Stateless1[I, O]].transform1.compose(f))
            .asInstanceOf[Aux[S, I2, O]]
        case _ =>
          Scan(self.initial)(
            self.transform.compose { case (s: S, i2: I2) => (s, f(i2)) },
            self.onComplete
          )
      }
  }

  sealed abstract class SafeFn[-A, +B] extends (A => B) with Serializable { self =>
    import SafeFn._

    final override lazy val hashCode: Int = {
      self match {
        case Single(f, _) => MurmurHash3.productHash(("Single", f))
        case Concat(left, right) => MurmurHash3.productHash(("Concat", left, right))
        case Identity() => MurmurHash3.productHash(("Identity", ZIO.identityFn[Any]))
      }
    }

    def apply(a: A): B =
      runLoop(a)

    // Compose two SafeFn in left-to-right order
    override def andThen[C](that: B => C): SafeFn[A, C] =
      SafeFn.andThen(this, SafeFn(that))

    // Compose two SafeFn in right-to-left order
    override def compose[Z](that: Z => A): SafeFn[Z, B] =
      SafeFn.andThen(SafeFn(that), this)

    final def runLoop(a: A): B = {
      // Iterative approach:
      var curAT: SafeFn[Any, Any] = this.asInstanceOf[SafeFn[Any, Any]]
      var curA: Any               = a

      while (true) {
        curAT match {
          case Single(f, _) =>
            // final function call => return
            return f(curA.asInstanceOf[A]).asInstanceOf[B]

          case Identity() =>
            // no transformation => return the same value
            return curA.asInstanceOf[B]

          case Concat(Single(f, _), right) =>
            // apply the single function
            curA = f(curA.asInstanceOf[A])
            // continue with the right side
            curAT = right

          case Concat(Identity(), right) =>
            // Identity doesn't change input => skip to right
            curAT = right

          case Concat(Concat(l, r), c) =>
            // flatten left-nested: replace `curAT` with `l.rotateAccum(r, c)`
            curAT = l.rotateAccum(r, c)
        }
      }
      // Unreachable, but Scala demands a return type
      scala.sys.error("SafeFn.apply loop broke unexpectedly")
    }

    /**
     * Reassociates nested `Concat` to the right for stack-safety.
     */
    private def rotateAccum[X, Y, Z >: B](mid: SafeFn[X, Y], right: SafeFn[Y, Z]): SafeFn[A, Z] = {
      @tailrec
      def _rotateAccum(self: SafeFn[A, X], mid: SafeFn[X, Y], right: SafeFn[Y, Z]): SafeFn[A, Z] = {
        if (self.isInstanceOf[SafeFn.Single[_, _]] | self.isInstanceOf[SafeFn.Identity[_]]) {
          Concat(self, Concat(mid, right))
        } else {
          val c = self.asInstanceOf[SafeFn.Concat[A, X, Y]]
          _rotateAccum(c.left, c.right, right)
        }
      }

      _rotateAccum(self.asInstanceOf[SafeFn[A, X]], mid, right)
    }
  }

  object SafeFn {

    private val _SecondFn: SafeFn[(Any, Any), Any] = SafeFn { case (_: Any, b: Any) => b }

    private[zio] def secondFn[A]: SafeFn[(Any, A), A] = _SecondFn.asInstanceOf[SafeFn[(Any, A), A]]

    private[zio] def identityFn[A]: A => A = Identity()

    private val MaxFusion = 128

    val emptyFn: SafeFn[Any, Chunk[Nothing]] = SafeFn((_: Any) => Chunk.empty[Nothing])

    private final case class Single[-A, +B](f: A => B, idx: Int)                      extends SafeFn[A, B] {
      override def toString(): String = "Single(" + idx + ", " + f.hashCode() + ")"
    }
    private final case class Concat[A, B, C](left: SafeFn[A, B], right: SafeFn[B, C]) extends SafeFn[A, C]{
      override def toString(): String = "Concat(" + left + ", " + right + ")"
    }
    private final case class Identity[A]() extends SafeFn[A, A] {
      override def toString(): String = "Identity"
      override def apply(a: A): A                         = a
      override def andThen[C](that: A => C): SafeFn[A, C] = SafeFn(that)
      override def compose[C](that: C => A): SafeFn[C, A] = SafeFn(that)
    }

    /**
     * Compose ab -> bc into ac, with fusion up to `MaxFusion`.
     */
    private[stream] def andThen[A, B, C](ab: SafeFn[A, B], bc: SafeFn[B, C]): SafeFn[A, C] =
      (ab, bc) match {
        // small optimization: fuse Single => Single up to `MaxFusion`
        case (Single(f, i1), Single(g, i2)) if i1 + i2 < MaxFusion =>
          Single(f.andThen(g), i1 + i2 + 1)
        case (Identity(), ac) =>
          ac.asInstanceOf[SafeFn[A, C]]
        case (ac, Identity()) =>
          ac.asInstanceOf[SafeFn[A, C]]
        case (Concat(f, Single(g, i2)), Single(ff, i1)) if i1 + i2 < MaxFusion =>
          Concat(f, Single(g.andThen(ff), i1 + i2 + 1))
        case _ => Concat(ab, bc)
      }

    /**
     * `toRightAssociated` re-associates all compositions to the right, for big
     * chains.
     */
    def toRightAssociated[A, B](fn: SafeFn[A, B]): SafeFn[A, B] = {
      @tailrec
      def loop[X, Y](
                      left: SafeFn[A, X],
                      middle: SafeFn[X, Y],
                      right: SafeFn[Y, B],
                      rightDone: Boolean
                    ): SafeFn[A, B] =
        if (rightDone) {
          // we already re-associated the right side
          middle match {
            case Single(_, i) =>
              // fuse single if possible
              val newRight = andThen(middle, right)
              left match {
                case Single(f, j) if i + j < MaxFusion =>
                  Single(f.andThen(newRight), i + j + 1)
                    .asInstanceOf[SafeFn[A, B]]
                case Concat(l1, l2) =>
                  loop(l1, l2, newRight, rightDone = true)
                case _ => Concat(left, newRight)
              }
            case Concat(m1, m2) =>
              loop(Concat(left, m1), m2, right, rightDone = true)

            case Identity() =>
              loop(SafeFn.identityFn.asInstanceOf, left.asInstanceOf, right, rightDone = true)
          }
        } else {
          right match {
            case Single(_, _) | Identity() =>
              // now right is fully associated
              loop(left, middle, right, rightDone = true)
            case Concat(r1, r2) =>
              // push r1 into the middle
              loop(left, Concat(middle, r1), r2, rightDone = false)
          }
        }

      fn match {
        case Single(_, _) | Identity() => fn
        case Concat(a, b) =>
          b match {
            case Concat(b1, b2)            => loop(a, b1, b2, rightDone = false)
            case Single(_, _)              => fn
          }
      }
    }

    def apply[A, B](f: A => B): SafeFn[A, B] = f match {
      case f: SafeFn[A, B]          => f
      case _ if f == ZIO.identityFn => Identity().asInstanceOf[SafeFn[A, B]]
      case _                        => Single(f, 0)
    }

    def id[A]: SafeFn[A, A] = Identity()
  }

  // ------------------------------------------------------------------------
  // ZipUnzip: merges/unmerges two state types, typically into a tuple.
  // ------------------------------------------------------------------------
  trait ZipUnzip[S1, S2] extends Zippable[S1, S2] with Unzippable[S1, S2] {
    type Out
    override type In = Out
    def zippable: Zippable.Out[S1, S2, Out]
    def unzippable: Unzippable.In[S1, S2, In]
    override def discardsLeft: Boolean  = zippable.discardsLeft
    override def discardsRight: Boolean = zippable.discardsRight
    final def zip(s1: S1, s2: S2): Out  = zippable.zip(s1, s2)
    final def unzip(out: Out): (S1, S2) = unzippable.unzip(out)
  }

  object ZipUnzip {
    type Aux[S1, S2, Out0] = ZipUnzip[S1, S2] { type Out = Out0 }

    implicit def zipUnzip[S1, S2, O](implicit
                                     z: Zippable.Out[S1, S2, O],
                                     u: Unzippable.In[S1, S2, O]
                                    ): Aux[S1, S2, O] =
      new ZipUnzip[S1, S2] {
        type Out = O
        val zippable: Zippable.Out[S1, S2, O]    = z
        val unzippable: Unzippable.In[S1, S2, O] = u
      }
  }
}
