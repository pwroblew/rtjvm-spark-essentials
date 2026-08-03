package x_part7bigdata

case class DistanceBucket(min: Double, max: Double)
object DistanceBucket {
  val b000  = DistanceBucket(0.0d, 0.5d)
  val b005  = DistanceBucket(0.5d, 1.0d)
  val b010  = DistanceBucket(1.0d, 1.5d)
  val b015  = DistanceBucket(1.5d, 2.0d)
  val b020  = DistanceBucket(2.0d, 2.5d)
  val b025  = DistanceBucket(2.5d, 3.0d)
  val b030  = DistanceBucket(3.0d, 3.5d)
  val b035  = DistanceBucket(3.5d, 4.0d)
  val b040  = DistanceBucket(4.0d, 4.5d)
  val b045  = DistanceBucket(4.5d, 5.0d)
  val b050  = DistanceBucket(5.0d, 5.5d)
  val b055  = DistanceBucket(5.5d, 6.0d)
  val b060  = DistanceBucket(6.0d, 6.5d)
  val b065  = DistanceBucket(6.5d, 7.0d)
  val b070  = DistanceBucket(7.0d, 7.5d)
  val b075  = DistanceBucket(7.5d, 8.0d)
  val b080  = DistanceBucket(8.0d, 8.5d)
  val b085  = DistanceBucket(8.5d, 9.0d)
  val b090  = DistanceBucket(9.0d, 9.5d)
  val b095  = DistanceBucket(9.5d, 10.0d)
  val b100  = DistanceBucket(10.0d, 10.5d)
  val b105  = DistanceBucket(10.5d, 11.0d)
  val Loong = DistanceBucket(11.0d, 1000.0d)

  val values: List[DistanceBucket] = List(
    b000,
    b005,
    b010,
    b015,
    b020,
    b025,
    b030,
    b035,
    b040,
    b045,
    b050,
    b055,
    b060,
    b065,
    b070,
    b075,
    b080,
    b085,
    b090,
    b095,
    b100,
    b105,
    Loong
  )

  def from(distance: Double): DistanceBucket = {
    values
      .find(bucket => bucket.min <= distance && bucket.max > distance)
      .getOrElse(Loong)
  }

}
