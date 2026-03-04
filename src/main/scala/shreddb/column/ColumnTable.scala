package shreddb.column

import shreddb.aggregate.{Aggregator, AggregatorFactory, SumAggregatorFactory}
import shreddb.storage.Storage
import shreddb.{Aggregation, ShredQuery, ShredQueryException, ShredResultSet, ShredResultSetRow, ShredTable, Sum, TableDescriptor}

import java.util
import scala.:+
import scala.jdk.CollectionConverters.*
import scala.collection.mutable

class ColumnTable(storage: Storage, val name: String, columns: Seq[Column]) extends ShredTable {
  override val columnNames: Seq[String] = columns.map { c => c.name }

  private val columnsByName: Map[String, Column] = columns.map { c => (c.name, c)}.toMap

  override def query(query: ShredQuery): ShredResultSet = {
    validateQuery(query)

    val builder = newResultSetBuilder(query)

    val columnReadersByName: Map[String, ColumnReader] = {
      val names =
        mutable.LinkedHashSet.empty[String].addAll(query.groupBy).addAll(query.whereFields).addAll(query.selectFields)
      names.map { name => (name, columnsByName(name)) }.map { (name, column) => (name, column.newReader()) }.toMap
    }

    val whereColumnReaders: Seq[FilteredColumnReader] = query.where.map { criteria =>
      columnReadersByName(criteria.field).filteredBy(criteria)
    }

    val groupByColumnReaders: Seq[ColumnReader] = query.groupBy.map { field => columnReadersByName(field) }
    val valueColumnReaders: Seq[ColumnReader] = query.select.map { agg => columnReadersByName(agg.field) }

    var done = false
    var tip: Long = 0
    while (!done) {
      findFirstMatchingIndex(whereColumnReaders, tip) match {
        case Some(idx) =>
          addValues(builder, idx, groupByColumnReaders, valueColumnReaders)
          tip = idx + 1
        case None =>
          done = true
      }
    }

    builder.build()
  }

  def findFirstMatchingIndex(readers: Seq[FilteredColumnReader], tip: Long): Option[Long] = {
    var result: Option[Long] = None
    var newTip = tip
    var done = false
    var toExamine = readers
    var examined = Seq.empty[FilteredColumnReader]

    while (!done) {
      val next = toExamine.head
      examined = next +: examined
      toExamine = toExamine.tail
      next.nextMatchingIndexAtOrAfter(newTip) match {
        case None =>
          done = true
        case Some(idx) if idx == newTip =>
          if (toExamine.isEmpty) {
            result = Some(newTip)
            done = true
          }
        case Some(idx) =>
          toExamine = toExamine ++: examined
          examined = Seq.empty
          newTip = idx
      }
    }

    result
  }

  def addValues(builder: ColumnTableResultSetBuilder, idx: Long, groupByReaders: Seq[ColumnReader], valueReaders: Seq[ColumnReader]): Unit = {
    val groupByValues: Seq[Object] = groupByReaders.map { reader => reader.valueAt(idx).get }
    val values: Seq[BigDecimal] = valueReaders.map { reader => reader.valueAt(idx).get.asInstanceOf[BigDecimal] }
    builder.add(groupByValues, values)
  }

  private def newResultSetBuilder(query: ShredQuery): ColumnTableResultSetBuilder = {
    val groupByColumns = query.groupBy.map { name => columnsByName(name) }

    ColumnTableResultSetBuilder.empty(groupByColumns, query.select)
  }

  private def validateQuery(query: ShredQuery): Unit = {
    val columnNameSet = columnNames.toSet

    query.selectFields.foreach { field =>
      if (!columnNameSet.contains(field)) {
        throw new ShredQueryException(s"Select field $field is not in $name: $columnNames")
      }}

    query.whereFields.foreach { field =>
      if (!columnNameSet.contains(field)) {
        throw new ShredQueryException(s"Where field $field is not in $name: $columnNames")
      }
    }

    query.groupBy.foreach { field =>
      if (!columnNameSet.contains(field)) {
        throw new ShredQueryException(s"Group by field $field is not in $name: $columnNames")
      }
    }
  }
}
object ColumnTable {
  def apply(storage: Storage, tableDescriptor: TableDescriptor): ShredTable = {
    val columns: Seq[Column] = tableDescriptor.columns.map{ c =>
      c.format match {
        case DecimalColumnFormat => new DecimalColumn(c.name, tableDescriptor.numRows, storage, c.resource.get)
        case EnumeratedColumnFormat(RawEnumeration, charset) => new RawEnumeratedColumn(c.name, tableDescriptor.numRows, storage, c.resource.get, charset)
        case EnumeratedColumnFormat(TokenizedEnumeration, charset) => new TokenizedEnumeratedColumn(c.name, tableDescriptor.numRows, storage, c.resource.get, charset)
      }
    }
    new ColumnTable(storage, tableDescriptor.name, columns)
  }
}

class ColumnTableResultSetBuilder(rows: mutable.Map[Seq[Object], Seq[Aggregator]], groupByAccessors: Seq[GroupByValueAccessor], aggregatorFactories: Seq[AggregatorFactory]) {
  def add(group: Seq[Object], values: Seq[BigDecimal]): Unit = {
    val aggregators = rows.get(group) match {
      case Some(aggregators) => aggregators
      case None => {
        val aggregators = aggregatorFactories.map(f => f.newAggregator())
        rows.put(group, aggregators)
        aggregators
      }
    }

    aggregators.zip(values).foreach { (aggregator, value) =>
      aggregator.add(value)
    }
  }

  def build(): ShredResultSet = {
    val resultSetRows = rows.map { (rawGroup, aggregators) =>
      val group = groupByAccessors.zip(rawGroup).map { (accessor, rawValue) => accessor.getValue(rawValue) }
      val values = aggregators.map { agg => agg.value }

      new ShredResultSetRow(group, values)
    }.toSeq

    new ShredResultSet(resultSetRows)
  }
}
object ColumnTableResultSetBuilder {
  def empty(groupColumns: Seq[Column], aggregations: Seq[Aggregation]): ColumnTableResultSetBuilder = {
    val groupByAccessors = groupColumns.map {
      case groupable: GroupByColumn => groupable.valueAccessor
      case column: Column => throw new ShredQueryException(s"${column.name} cannot be used in the group by clause")
    }

    val aggregatorFactories = aggregations.map {
      case s: Sum => SumAggregatorFactory
    }

    new ColumnTableResultSetBuilder(mutable.LinkedHashMap.empty, groupByAccessors, aggregatorFactories)
  }
}