package shreddb

case class ShredQuery(
    select: Seq[Aggregation],
    where: Seq[Criteria],
    groupBy: Seq[String]
) {
  def whereFields: Seq[String] = where.map(criteria => criteria.field)
  def selectFields: Seq[String] = select.map(aggregation => aggregation.field)
}

sealed trait Criteria {
  def field: String
}
case class Is(field: String, value: String) extends Criteria
case class In(field: String, values: Set[String]) extends Criteria

sealed trait Aggregation {
  def field: String
}
case class Sum(field: String) extends Aggregation
case class Average(field: String) extends Aggregation
