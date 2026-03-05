package shreddb

case class ShredQuery(
    select: Seq[Aggregation],
    whereClause: Seq[Criteria] = Seq.empty,
    groupByFields: Seq[String] = Seq.empty
) {
  def where(criteria: Criteria*): ShredQuery = {
    copy(whereClause = criteria)
  }

  def groupBy(fields: String*): ShredQuery = {
    copy(groupByFields = fields)
  }

  def whereFields: Seq[String] = whereClause.map(criteria => criteria.field)
  def selectFields: Seq[String] = select.map(aggregation => aggregation.field)
}
object ShredQuery {
  def select(aggregations: Aggregation*): ShredQuery = {
    new ShredQuery(aggregations)
  }
}

sealed trait Criteria {
  def field: String
}
case class Is(field: String, value: String) extends Criteria
case class In(field: String, values: Set[String]) extends Criteria

object ShredQueryCriteriaImplicits {
  extension(field: String) def is(value: String) = Is(field, value)
  extension(field: String) def in(values: Set[String]) = In(field, values)
}

sealed trait Aggregation {
  def field: String
}
case class Sum(field: String) extends Aggregation
object Sum {
  def of(field: String): Sum = new Sum(field)
}

case class Average(field: String) extends Aggregation
object Average {
  def of(field: String): Average = new Average(field)
}