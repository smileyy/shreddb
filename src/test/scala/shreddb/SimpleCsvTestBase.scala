package shreddb

import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.is
import org.junit.jupiter.api.Test
import shreddb.input.Input
import shreddb.input.csv.CsvInput

import java.io.InputStreamReader
import java.nio.charset.StandardCharsets

trait SimpleCsvTestBase {
  def request: ShredRequest

  def input: Input = new CsvInput(
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
    testQuery(table, ShredQuery(
        select = Seq(Sum.of("qty")),
        where = Seq(
          In("a", Set("A", "C")),
          Is("c", "SQS")
        ),
        groupBy = Seq.empty
      )) { rs =>
      assertThat(rs.size, is(1))
      assertThat(rs.rows.head.group, is(Seq.empty))
      assertThat(rs.rows.head.values, is(Seq(BigDecimal(16))))
    }

    // Grouped query with sum aggregation
    testQuery(table, ShredQuery(
        select = Seq(Sum.of("qty")),
        where = Seq(
          In("a", Set("A", "C")),
          Is("c", "SQS")
        ),
        groupBy = Seq("a")
      )
    ) { rs => 

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
    testQuery(table, ShredQuery(
        select = Seq(Average.of("qty")),
        where = Seq(
          In("a", Set("A", "C")),
          Is("c", "SQS")
        ),
        groupBy = Seq("a")
      )
    ) { rs => 
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
    testQuery(table, ShredQuery(
      select = Seq(Sum.of("qty"), Average.of("qty")),
      where = Seq(
        In("a", Set("A", "C")),
        Is("c", "SQS")
      ),
      groupBy = Seq("a")
    )
    ) { rs =>
      assertThat(rs.size, is(2))
      val averagedResultSetIterator = rs.iterator
      val row1 = averagedResultSetIterator.next()
      assertThat(row1.group, is(Seq("A")))
      assertThat(row1.values, is(Seq(BigDecimal(1), BigDecimal(1))))
      val row2 = averagedResultSetIterator.next()
      assertThat(row2.group, is(Seq("C")))
      assertThat(row2.values, is(Seq(BigDecimal(15), BigDecimal("7.5"))))
    }
  }
  
  private def testQuery(table: ShredTable, query: ShredQuery)(verify: ShredResultSet => Unit): Unit = {
    verify(table.query(query))
  }
}