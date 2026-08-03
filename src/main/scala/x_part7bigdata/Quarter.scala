package x_part7bigdata

final case class Hour private (value: Int) extends AnyVal

object Hour {
  def from(value: Int): Option[Hour] =
    if (0 <= value && value <= 23) Some(new Hour(value))
    else None
}

final case class Quarter private (index: Int) extends AnyVal {

  def hour: Int =
    index / 4

  def quarterInHour: Int =
    index % 4

  def minute: Int =
    quarterInHour * 15

  override def toString: String =
    f"$hour%02d:$minute%02d"
}

object Quarter {

  val Count = 96

  def from(index: Int): Option[Quarter] =
    if (0 <= index && index < Count)
      Some(new Quarter(index))
    else
      None

  def from(hour: Hour, quarterInHour: Int): Option[Quarter] =
    if (
      0 <= hour.value && hour.value < 24 &&
      0 <= quarterInHour && quarterInHour < 4
    )
      Some(new Quarter(hour.value * 4 + quarterInHour))
    else
      None

}

case class QuarterCount(
    quarter: Quarter,
    count: Int
)
