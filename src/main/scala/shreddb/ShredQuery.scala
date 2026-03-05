package shreddb

case class ShredQuery(
    select: Seq[Aggregation],
    whereClause: Seq[Criteria] = Seq.empty,
    groupByFields: Seq[String] = Seq.empty
) {
  def whereFields: Seq[String] = whereClause.map(criteria => criteria.field)
  def selectFields: Seq[String] = select.map(aggregation => aggregation.field)

  def where(criteria: Criteria*): ShredQuery = {
    copy(whereClause = criteria)
  }

  def groupBy(fields: String*): ShredQuery = {
    copy(groupByFields = fields)
  }
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
  def preApply(f: BigDecimal => BigDecimal): Aggregation = this match {
    case t: TransformedAggregation => new TransformedAggregation(t.delegate, before = f, after = t.after)
    case agg: Aggregation => new TransformedAggregation(agg, before = f)
  }
  def postApply(f: BigDecimal => BigDecimal): Aggregation = this match {
    case t: TransformedAggregation => new TransformedAggregation(t.delegate, before = t.before, after = f)
    case agg: Aggregation => new TransformedAggregation(agg, after = f)
  }
}

case class Sum(field: String) extends Aggregation
object Sum {
  def of(field: String): Aggregation = new Sum(field)
}

case class Average(field: String) extends Aggregation
object Average {
  def of(field: String): Aggregation = new Average(field)
}

class TransformedAggregation(val delegate: Aggregation, val before: BigDecimal => BigDecimal = { d => d }, val after: BigDecimal => BigDecimal = { d => d }) extends Aggregation {
  override def field: String = delegate.field
}