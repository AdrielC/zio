package zio.stream

import zio._
import zio.stream.Scan.{AndThen, Aux, SafeFn, Stateful, Stateless, Stateless1, StatelessN, Zipper, stateful, stateless1}

import java.time.{Instant, ZoneId, ZonedDateTime}
import scala.annotation.tailrec
import scala.util.hashing.MurmurHash3

sealed trait Scan[-I, +O] extends Serializable { self =>

  type State
  def initial: State
  def modifyAll: ((State, Chunk[I])) => (State, Chunk[O])

  lazy val modify: ((State, I)) => (State, Chunk[O]) =
    SafeFn.toRightAssociated(SafeFn(modifyAll) compose SafeFn.singleFn[I].second[State])

  def onComplete: State => Chunk[O]

  def andThen[S2, O2](that: Scan.Aux[S2, O, O2])
                     (implicit z: Zipper[State, S2]): Scan.Aux[z.Out, I, O2] = {
    (self, that) match {
      case (_: Stateless1[I, O], s2: Stateless1[O, O2])      => self.map(s2)
        .transformState(z.zip(_, that.initial))(z.unzip(_)._1)
      case (s1: Stateful[State, I, O], s2: StatelessN[O, O2]) => s1.flatMap(s2.transformN)
        .transformState(z.zip(_, that.initial))(z.unzip(_)._1)
      case _ => AndThen[State, S2, z.Out, I, O, O2](self, that, z)
    }
  }

  def map[O2](f: O => O2): Aux[State, I, O2] =
    self match {
      case s: Stateless1[I, O] => new Stateless1(s.transform1 andThen SafeFn(f))
        .transformState(s => s.asInstanceOf[State])(s => s)
      case s: StatelessN[I, O] => Stateless.makeChunked[I, O2](s.transformN andThen SafeFn((_: Chunk[O]).map(f)))
        .transformState(s => s.asInstanceOf[State])(s => s)
      case _ =>
        Scan(initial)(
          modify.andThen { case (s: State, os: Chunk[O2]) => (s, os.map(f)) },
          onComplete.andThen((_: Chunk[O]).map(f))
        )
    }

  def flatMap[O2](f: O => Chunk[O2]): Aux[State, I, O2] =
    self match {
      case s: Stateless1[I, O] => Stateless.makeChunked(s.transform1 andThen SafeFn(f)).asInstanceOf[Scan.Aux[State, I, O2]]
      case s: StatelessN[I, O] => Stateless.makeChunked(s.transformN.andThen(_.flatMap(f))).asInstanceOf[Scan.Aux[State, I, O2]]
      case _ =>
        val fm = SafeFn((_: Chunk[O]).flatMap(f))
        Scan(initial)(modify andThen fm.second, onComplete andThen fm)
    }

  def contramap[I2](f: I2 => I): Aux[State, I2, O] =
    self match {
      case s: Stateless1[I, O] => (stateless1(f) andThen s).asInstanceOf[Scan.Aux[State, I2, O]]
      case s: StatelessN[I, O] => (Stateless.make(f) andThen s).asInstanceOf[Scan.Aux[State, I2, O]]
      case _ => Scan(self.initial)(SafeFn(f).second[State] andThen self.modify, self.onComplete)
    }

  final def toChannel: ZChannel[Any, Nothing, Chunk[I], Any, Nothing, Chunk[O], State] =
    ZChannel.fromZIO(ZIO.succeed(initial)).flatMap { initState =>
      def process[E](state: State): ZChannel[Any, E, Chunk[I], Any, Nothing, Chunk[O], State] =
        ZChannel.readWithCause(
          (in: Chunk[I]) => {
            val (newState, outs) = modifyAll(state, in)
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

  final def transform[I2, O2](f: I2 => I)(g: O => O2): Scan.Aux[State, I2, O2] =
    self.map(g).contramap(f)

  final def transformState[S2](to: State => S2)(from: S2 => State): Scan.Aux[S2, I, O] = {
    self match {
      case stateful: Stateful[_, _, _] => new Stateful[S2, I, O](
        () => to(stateful.initial),
        SafeFn { case (s2: S2, i: Chunk[I]) =>
          val s = from(s2)
          val (sNext, os) = self.modifyAll(s, i)
          (to(sNext), os)
        },
        SafeFn(from) andThen self.onComplete
      )
      case StatelessN(transformN, _initial) =>
        StatelessN(transformN, () => to(_initial().asInstanceOf[State]))
          .asInstanceOf[Scan.Aux[S2, I, O]]
    }
  }

  final def semilens[I2, O2](extract: I2 => Either[O2, I], inject: ((I2, O)) => O2)
  : Scan.Aux[State, I2, O2] = {
    val f = SafeFn.toRightAssociated(SafeFn(self.modify))
    new Scan.Stateful[self.State, I2, O2](
      () => self.initial,
      SafeFn { case (s: State, i2: I2) =>
        extract(i2) match {
          case Left(o2) => (s, Chunk.single(o2))
          case Right(i) =>
            val (sNext, os) = f(s, i)
            (sNext, os.map(o => inject(i2, o)))
        }
      },
      SafeFn.emptyFn
    )
  }

  final def left[A]: Scan.Aux[State, Either[I, A], Either[O, A]] =
    semilens(SafeFn.getLeftFn, SafeFn.secondFn andThen SafeFn.leftFn)

  final def right[A]: Scan.Aux[State, Either[A, I], Either[A, O]] =
    semilens(SafeFn.getRightFn, SafeFn.secondFn andThen SafeFn.rightFn)
}

object Scan extends ZIOAppDefault {

  val atZone = Scan.stateless1((_: Instant).atZone(ZoneId.systemDefault()))

  val getHour = Scan.stateful1(Set.empty[Int])((s, i: ZonedDateTime) => (s + i.getHour, i))

  val getMinute = Scan.stateful1(Set.empty[Int])((s, i: ZonedDateTime) => (s + i.getMinute, i))

  val scan =
    atZone andThen                        getHour andThen getMinute andThen
      atZone.contramap(_.toInstant) andThen getHour andThen getMinute

  val run =
    ZStream.tick(50.millis)
      .mapZIO(_ => Clock.instant)
      .debug("tick")
      .run(scan.toSink)
      .debug("Hour, minute")

  type Aux[S, -I, +O] = Scan[I, O] { type State = S }

  type Stateless[-I, +O] = Scan[I, O] { type State = Unit }

  private object Stateless {
    def makeChunked[I, O](f: I => Chunk[O]): Stateless[I, O] = new StatelessN(SafeFn(f), () => ())
    def make[I, O](f: I => O): Stateless[I, O] = new Stateless1(SafeFn(f))
  }

  private[stream] class Stateful[S0, I, O]
  (_initial: () => S0,
   _modify: => SafeFn[(S0, I), (S0, Chunk[O])],
   override val onComplete: SafeFn[S0, Chunk[O]]
  ) extends Scan[I, O] {
    type State = S0
    lazy val initial: S0 = _initial()

    override lazy val modify = _modify

    override lazy val hashCode: Int =
      MurmurHash3.productHash(("Stateful", initial, modify, onComplete))

    override def modifyAll: ((S0, Chunk[I])) => (S0, Chunk[O]) = SafeFn {
      case (s, i) => i.mapAccum(s)(modify(_, _)) match {
        case (s, os) => (s, os.flatten)
      }
    }
  }

  private[stream] final case class AndThen[S, S2, S3, I, O, O2]
  (
    first: Aux[S, I, O],
    second: Aux[S2, O, O2],
    z: Zipper.WithOut[S, S2, S3]
  ) extends Stateful[S3, I, O2](
    () => z.zip(first.initial, second.initial),
    SafeFn { case (s12: S3, i: I) =>
      val (s1, s2) = z.unzip(s12)
      val (s1Next, outs) = first.modify((s1, i))
      val (s2Next, outs2) = second.modifyAll(s2, outs)
      (z.zip(s1Next, s2Next), outs2)
    },
    SafeFn(z.unzip(_) match { case (s1, s2) =>
      second.modifyAll(s2, first.onComplete(s1)) match {
        case (s2, outs) => outs ++ second.onComplete(s2)
      }
    })
  ) {
    override lazy val hashCode: Int =
      MurmurHash3.productHash(("AndThen", first, second))
  }

  private[stream] case class StatelessN[-I, +O](transformN: SafeFn[I, Chunk[O]],
                                                _initial: () => Any)
    extends Scan[I, O] { self: Stateless[I, O] =>

    override lazy val hashCode: Int =
      MurmurHash3.productHash(("StatelessN", transformN, initial))

    type State = Unit

    lazy val initial: State = _initial.asInstanceOf[State]

    final val onComplete = SafeFn.emptyFn

    protected def stateLeft[OO >: O] =
      SafeFn(initial -> (_: Chunk[OO]))

    override final lazy val modify = SafeFn.toRightAssociated(
      SafeFn.secondFn[I] andThen transformN andThen stateLeft
    )

    override lazy val modifyAll: ((State, Chunk[I])) => (State, Chunk[O]) =
      SafeFn.secondFn[Chunk[I]] andThen SafeFn(_.flatMap(transformN)) andThen stateLeft
  }

  private[stream] final class Stateless1[-I, +O](private[zio] val transform1: SafeFn[I, O])
    extends StatelessN[I, O](transform1 andThen SafeFn.singleFn[O], () => ()) with (I => O) {

    override def apply(a: I): O = transform1(a)

    override lazy val modifyAll: ((State, Chunk[I])) => (State, Chunk[O]) =
      SafeFn.secondFn[Chunk[I]] andThen SafeFn(_.map(transform1)) andThen super.stateLeft

    def andThen[O2](other: Stateless1[O, O2]): Stateless1[I, O2] =
      new Stateless1(transform1 andThen other.transform1)

    def compose[Z](other: Stateless1[Z, I]): Stateless1[Z, O] =
      new Stateless1(transform1 compose other.transform1)

    override def map[O2](other: O => O2): Stateless1[I, O2] =
      new Stateless1(transform1 andThen SafeFn(other))

    override def contramap[I2](f: I2 => I): Stateless1[I2, O] =
      new Stateless1(SafeFn(f) andThen transform1)
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
    new Stateful(() => initial, SafeFn(transform), SafeFn(onComplete))

  def stateful[S, I, O](initial: => S)(f: (S, I) => (S, Chunk[O])): Scan.Aux[S, I, O] =
    apply(initial)(f.tupled)

  def stateful1[S, I, O](initial: => S)(f: (S, I) => (S, O)): Aux[S, I, O] =
    apply(initial)(f.tupled andThen SafeFn.singleFn[O].second)

  def stateless[I, O](f: I => Chunk[O]): Stateless[I, O] =
    Stateless.makeChunked(f)

  def stateless1[I, O](f: I => O): Stateless[I, O] =
    Stateless.make(f)

  def id[I]: Stateless[I, I] =
    stateless1(ZIO.identityFn[I])


  sealed trait SafeFn[-A, +B] extends (A => B) with Serializable { self =>
    import SafeFn._

    def second[I]: SafeFn[(I, A), (I, B)] = SafeFn { case (i, a) => (i, apply(a)) }

    def first[I]: SafeFn[(A, I), (B, I)] = SafeFn { case (a, i) => (apply(a), i) }

    final override lazy val hashCode: Int = {
      self match {
        case Single(f, _) => MurmurHash3.productHash(("Single", f))
        case Concat(left, right) => MurmurHash3.productHash(("Concat", left, right))
        case _: Identity[_] => MurmurHash3.productHash(("Identity", ZIO.identityFn[Any]))
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

          case Identity(_, _) =>
            // no transformation => return the same value
            return curA.asInstanceOf[B]

          case Concat(Identity(_, _), right) =>
            // Identity doesn't change input => skip to right
            curAT = right

          case Concat(Single(f, _), right) =>
            // apply the single function
            curA = f(curA.asInstanceOf[A])
            // continue with the right side
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

    private val _SecondFn: SafeFn[(Any, Any), Any] = SafeFn(ZIO.secondFn[Any].tupled)

    private val _SingleFn: SafeFn[AnyRef, Chunk[AnyRef]] = SafeFn(Chunk.single[AnyRef])

    private val _RightFn: SafeFn[Any, Either[Any, Any]] = SafeFn(Right(_))

    private val _LeftFn: SafeFn[Any, Either[Any, Any]] = SafeFn(Left(_))

    private val _UnitLeft = SafeFn(() -> (_: Any))

    private val _GetLeft: SafeFn[Either[Any, Any], Either[Either[Any, Any], Any]] = SafeFn {
      case Left(i)  => Right(i)
      case Right(a) => Left(Right(a))
    }

    private val _GetRight: SafeFn[Either[Any, Any], Either[Either[Any, Any], Any]] = SafeFn {
      case Left(a)  => Left(Left(a))
      case Right(i) => Right(i)
    }

    private[zio] def secondFn[A]: SafeFn[(Any, A), A] = _SecondFn.asInstanceOf[SafeFn[(Any, A), A]]

    private[zio] def singleFn[A]: SafeFn[A, Chunk[A]] = _SingleFn.asInstanceOf[SafeFn[A, Chunk[A]]]

    private[zio] def rightFn[A]: SafeFn[A, Either[Nothing, A]] = _RightFn.asInstanceOf[SafeFn[A, Either[Nothing, A]]]

    private[zio] def leftFn[A]: SafeFn[A, Either[A, Nothing]] = _LeftFn.asInstanceOf[SafeFn[A, Either[A, Nothing]]]

    private[zio] def identityFn[A]: SafeFn[A, A] = Identity()

    private[zio] def unitLeft[A]: SafeFn[A, (Unit, A)] = _UnitLeft.asInstanceOf[SafeFn[A, (Unit, A)]]

    private[zio] def getRightFn[I, O, A]: SafeFn[Either[I, A], Either[Either[I, O], A]] =
      _GetRight.asInstanceOf[SafeFn[Either[I, A], Either[Either[I, O], A]]]

    private [zio] def getLeftFn[I, O, A]: SafeFn[Either[A, I], Either[Either[O, I], A]] =
      _GetLeft.asInstanceOf[SafeFn[Either[A, I], Either[Either[O, I], A]]]

    private[zio] val emptyFn: SafeFn[Any, Chunk[Nothing]] = SafeFn((_: Any) => Chunk.empty[Nothing])

    private val MaxFusion = 128

    private final case class Single[-A, +B](f: A => B, idx: Int)                      extends SafeFn[A, B] {
      override def toString(): String = "Single(" + idx + ", " + f.hashCode() + ")"
    }
    private object Single {
      def unapply[A, B](a: SafeFn[A, B]): Option[(A => B, Int)] = a match {
        case s: Single[A, B] @unchecked => Some((s.f, s.idx))
        case Concat(_, _) => None
        case Identity(f, idx) => Some((f, idx))
      }
    }
    private final case class Concat[A, B, C](left: SafeFn[A, B], right: SafeFn[B, C]) extends SafeFn[A, C]{
      override def toString(): String = "Concat(" + left + ", " + right + ")"
    }
    private final case class Identity[A]() extends SafeFn[A, A] {
      override val toString: String = "Identity()"
      override def apply(a: A): A                         = a
      override def andThen[C](that: A => C): SafeFn[A, C] = SafeFn(that)
      override def compose[C](that: C => A): SafeFn[C, A] = SafeFn(that)
    }
    private object Identity {
      def unapply[A, B](a: SafeFn[A, B]): Option[(SafeFn[A, B], Int)] = a match {
        case i: Identity[A] => Some((i, 0))
        case _ => None
      }
    }

    /**
     * Compose ab -> bc into ac, with fusion up to `MaxFusion`.
     */
    private[stream] def andThen[A, B, C](ab: SafeFn[A, B], bc: SafeFn[B, C]): SafeFn[A, C] =
      (ab, bc) match {
        // small optimization: fuse Single => Single up to `MaxFusion`
        case (Single(f, i1), Single(g, i2)) if i1 + i2 < MaxFusion =>
          Single(f.andThen(g), i1 + i2 + 1)
        case (Concat(f, Single(g, i2)), Single(ff, i1)) if i1 + i2 < MaxFusion =>
          Concat(f, Single(g.andThen(ff), i1 + i2 + 1))
        case (Identity(_, _), ac: SafeFn[A, C] @unchecked) => ac
        case (ac: SafeFn[A, C] @unchecked, Identity(_, _)) => ac
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
              loop(left, m1, m2 andThen right, rightDone = true)
          }
        } else {
          right match {
            case Single(_, _) =>
              // now right is fully associated
              loop(left, middle, right, rightDone = true)
            case Concat(r1, r2) =>
              // push r1 into the middle
              loop(left, Concat(middle, r1), r2, rightDone = false)
          }
        }

      fn match {
        case Single(_, _) => fn
        case Concat(a, b) =>
          b match {
            case Concat(b1, b2)            => loop(a, b1, b2, rightDone = false)
            case Single(_, _)              => fn
          }
      }
    }

    def apply[A, B](f: A => B): SafeFn[A, B] = f match {
      case f: SafeFn[A, B]          => f
      case _ if f == ZIO.identityFn => id[A].asInstanceOf
      case _                        => Single(f, 0)
    }

    def id[A]: SafeFn[A, A] = identityFn[A]
  }

  // ------------------------------------------------------------------------
  // Zipper: merges/unmerges two state types, typically into a tuple.
  // ------------------------------------------------------------------------
  trait Zipper[S1, S2] extends Zippable[S1, S2] with Unzippable[S1, S2] with Serializable {
    type Out

    override final type In = Out

    type Patch

    def differ: Differ[Out, Patch]

    def zipDiffer[P1, P2](s1: Differ[S1, P1], s2: Differ[S2, P2]): Differ[Out, (P1, P2)] =
      (s1 zip s2).transform((zip _).tupled, unzip)
  }

  object Zipper {
    type WithOut[S1, S2, Out0] = Zipper[S1, S2] { type Out = Out0 }
    type WithPatch[S1, S2, Out0, P] = Zipper[S1, S2] { type Out = Out0; type Patch = P }

    abstract class ZipperProxy[S1, S2, O, P]
    (override val differ: Differ[O, P],
     private val zippable: Zippable.Out[S1, S2, O],
     private val unzippable: Unzippable.In[S1, S2, O])
      extends Zipper[S1, S2] {
      override type Out = O
      override type Patch = P
      override def discardsRight: Boolean = zippable.discardsRight
      override def discardsLeft: Boolean = zippable.discardsLeft
      override def unzip(in: O): (S1, S2) = unzippable.unzip(in)
      override def zip(left: S1, right: S2): O = zippable.zip(left, right)
    }

    implicit def zipUnzip[S1, S2, O]
    (implicit
     z: Zippable.Out[S1, S2, O],
     u: Unzippable.In[S1, S2, O]
    ): WithPatch[S1, S2, O, O => O] =
      new ZipperProxy(Differ.update[O], z, u) {

      }
  }
}
