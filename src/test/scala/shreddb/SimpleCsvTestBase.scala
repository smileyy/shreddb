package shreddb

import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.is
import org.junit.jupiter.api.Test
import shreddb.ShredQuery.select
import shreddb.ShredQueryCriteriaImplicits.*
import shreddb.input.Input
import shreddb.input.csv.CsvInput

import java.io.InputStreamReader
import java.nio.charset.StandardCharsets

trait SimpleCsvTestBase {
  def request: ShredRequest

  val input: Input = new CsvInput(
    new InputStreamReader(classOf[SimpleCsvTestBase].getResourceAsStream("simple-test-data.csv"), StandardCharsets.UTF_8)
  )

  @Test
  def simple(): Unit = {
    val shred = new ShredDb(TestConfiguration)

    val mf = shred.shred("simple-csv-test", request)

    val table = shred.getTable(mf)
    assertThat(table.name, is("simple-csv-test"))
    assertThat(table.columnNames, is(Seq("a", "b", "c", "tag:user", "qty")))

    // Ungrouped query with sum aggregation
    testQuery(table, select(Sum.of("qty")).where("a".in(Set("A", "C")), "c".is("SQS"))) { rs =>
      assertThat(rs.size, is(1))
      assertThat(rs.rows.head.group, is(Seq.empty))
      assertThat(rs.rows.head.values, is(Seq(BigDecimal(16))))
    }

    // Grouped query with sum aggregation
    testQuery(table, select(Sum.of("qty")).where("a".in(Set("A", "C")), "c".is("SQS")).groupBy("a")) { rs =>

      assertThat(rs.size, is(2))
      val it = rs.iterator
      val row1 = it.next()
      assertThat(row1.group, is(Seq("A")))
      assertThat(row1.values, is(Seq(BigDecimal(1))))
      val row2 = it.next()
      assertThat(row2.group, is(Seq("C")))
      assertThat(row2.values, is(Seq(BigDecimal(15))))
    }

    // Grouped query with average aggregation
    testQuery(table, select(Average.of("qty")).where("a".in(Set("A", "C")), "c".is("SQS")).groupBy("a")) { rs =>
      assertThat(rs.size, is(2))
      val averagedResultSetIterator = rs.iterator
      val row1 = averagedResultSetIterator.next()
      assertThat(row1.group, is(Seq("A")))
      assertThat(row1.values, is(Seq(BigDecimal(1))))
      val row2 = averagedResultSetIterator.next()
      assertThat(row2.group, is(Seq("C")))
      assertThat(row2.values, is(Seq(BigDecimal("7.5"))))
    }

    // Multiple aggregations of the same column
    testQuery(table, select(Sum.of("qty"), Average.of("qty"))
      .where("a".in(Set("A", "C")), "c".is("SQS"))
      .groupBy("a")) { rs =>

      assertThat(rs.size, is(2))
      val averagedResultSetIterator = rs.iterator
      val row1 = averagedResultSetIterator.next()
      assertThat(row1.group, is(Seq("A")))
      assertThat(row1.values, is(Seq(BigDecimal(1), BigDecimal(1))))
      val row2 = averagedResultSetIterator.next()
      assertThat(row2.group, is(Seq("C")))
      assertThat(row2.values, is(Seq(BigDecimal(15), BigDecimal("7.5"))))
    }

    // Multiple aggregations of the same column
    testQuery(table, select(Sum.of("qty").preApply({ d => -d})).where("c".is("SQS"))) { rs =>
      assertThat(rs.size, is(1))
      assertThat(rs.rows.head.values, is(Seq(BigDecimal(-16))))
    }
  }

  private def testQuery(table: ShredTable, query: ShredQuery)(verify: ShredResultSet => Unit): Unit = {
    verify(table.query(query))
  }
}