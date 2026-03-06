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
case class SuchThat(field: String, f: String => Boolean) extends Criteria

case class DecimalIs(field: String, value: BigDecimal) extends Criteria
case class DecimalIn(field: String, values: Set[BigDecimal]) extends Criteria
case class DecimalGreaterThan(field: String, value: BigDecimal) extends Criteria
case class DecimalGreaterThanOrEqual(field: String, value: BigDecimal) extends Criteria
case class DecimalLessThan(field: String, value: BigDecimal) extends Criteria
case class DecimalLessThanOrEqual(field: String, value: BigDecimal) extends Criteria
case class DecimalBetween(field: String, inclusiveFloor: BigDecimal, exclusiveCeiling: BigDecimal) extends Criteria

object ShredQueryCriteriaImplicits {
  extension(field: String) def is(value: String) = Is(field, value)
  extension(field: String) def in(values: Set[String]) = In(field, values)
  extension(field: String) def suchThat(f: String => Boolean) = SuchThat(field, f)
  
  extension(field: String) def is(value: BigDecimal) = DecimalIs(field, value)
  extension(field: String) def in(values: Set[BigDecimal]) = DecimalIn(field, values)
  extension(field: String) def gt(value: BigDecimal) = DecimalGreaterThan(field, value)
  extension(field: String) def gte(value: BigDecimal) = DecimalGreaterThanOrEqual(field, value)
  extension(field: String) def lt(value: BigDecimal) = DecimalLessThan(field, value)
  extension(field: String) def lte(value: BigDecimal) = DecimalLessThanOrEqual(field, value)
  extension(field: String) def between(inclusiveFloor: BigDecimal, exclusiveCeiling: BigDecimal) = 
    DecimalBetween(field, inclusiveFloor, exclusiveCeiling)
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