package shreddb.column

import shreddb.{Criteria, In, Is, ResourceDescriptor}
import shreddb.storage.{ResourceSink, ResourceSource, Storage}
import shreddb.token.{TokenLookupTable, TokenLookupTableBuilder}

import java.io.{DataInputStream, DataOutputStream, InputStream}
import java.nio.charset.Charset

class TokenizedStringColumn(val name: String, numRows: Long, storage: Storage, resource: ResourceDescriptor, charset: Charset) extends GroupByColumn {
  private val resourceSource = storage.newResourceSource(resource)

  private val lookupTable: TokenLookupTable = {
    val input = new DataInputStream(resourceSource.newInputStream(".lookup"))
    val builder = TokenLookupTableBuilder.newBuilder()

    val numberOfTokens = input.readInt()
    (0 until numberOfTokens).foreach { _ =>
      val token = input.readInt()
      val valueLength = input.readInt()
      val valueBytes = new Array[Byte](valueLength)
      input.read(valueBytes)
      val value = new String(valueBytes, charset)

      builder.add(token, value)
    }

    input.close()
    builder.build()
  }

  override def newReader(): ColumnReader = new TokenizedStringColumnReader(numRows, lookupTable, resourceSource)

  override def valueAccessor: GroupByValueAccessor = new TokenizedGroupByValueAccessor(lookupTable)
}

class TokenizedStringColumnWriter(val resource: ResourceSink, val name: String, charset: Charset) extends ColumnWriter {
  private val valueOutput = new DataOutputStream(resource.newOutputStream(".values"))
  private val tableOutput = new DataOutputStream(resource.newOutputStream(".lookup"))

  private val lookupTable = TokenLookupTable.empty()

  override def addValue(value: String): Unit = {
    valueOutput.writeInt(lookupTable.getOrAddToken(value))
  }

  override def close(): Unit = {
    tableOutput.writeInt(lookupTable.size)
    lookupTable.table.foreach { (value, token) =>
      tableOutput.writeInt(token)

      val bytes = value.getBytes(charset)
      val length = bytes.length

      tableOutput.writeInt(length)
      tableOutput.write(bytes)
    }

    tableOutput.close()
    valueOutput.close()
  }

  override def format: ColumnFormat = TokenizedStringColumnFormat(charset)
}

class TokenizedStringColumnReader(numRows: Long, val lookupTable: TokenLookupTable, source: ResourceSource) extends AbstractColumnReader(numRows) {
  override protected def newInputStream(): InputStream = source.newInputStream(".values")
  override protected def readNext(input: DataInputStream): Any = input.readInt()

  override def filteredBy(criteria: Criteria): FilteredColumnReader = FilteredTokenizedStringColumnReader(this, criteria)
}

class FilteredTokenizedStringColumnReader(reader: TokenizedStringColumnReader, criteria: TokenizedCriteria) extends FilteredColumnReader {
  override def nextMatchingIndexAtOrAfter(idx: Long): Option[Long] = {
    reader.valueAt(idx) match {
      case Some(_) =>
      case None => return None
    }

    var done = false
    var result: Option[Long] = None
    while (!done) {
      if (accepts(reader.currentValue)) {
        result = Some(reader.currentIndex)
        done = true
      } else if (reader.hasNext) {
        reader.next()
      } else {
        done = true
      }
    }

    result
  }

  def accepts(value: Any): Boolean = {
    criteria match {
      case TokenizedIs(token) => token == value
      case TokenizedIn(tokens) => tokens.contains(value.asInstanceOf[Int])
      case TokenDoesNotExist => false
    }
  }
}
object FilteredTokenizedStringColumnReader {
  def apply(reader: TokenizedStringColumnReader, criteria: Criteria): FilteredTokenizedStringColumnReader = {
    val lookupTable = reader.lookupTable
    val tokenizedCriteria: TokenizedCriteria = criteria match {
      case Is(_, value) => lookupTable.getToken(value) match {
        case Some(token) => TokenizedIs(token)
        case None => TokenDoesNotExist
      }
      case In(_, values) =>
        TokenizedIn(values.flatMap { value => lookupTable.getToken(value) })
    }

    new FilteredTokenizedStringColumnReader(reader, tokenizedCriteria)
  }
}

sealed trait TokenizedCriteria
case class TokenizedIs(value: Int) extends TokenizedCriteria
case class TokenizedIn(values: Set[Int]) extends TokenizedCriteria
case object TokenDoesNotExist extends TokenizedCriteria

class TokenizedGroupByValueAccessor(lookupTable: TokenLookupTable) extends GroupByValueAccessor {
  override def getValue(raw: Any): String = lookupTable.resolve(raw.asInstanceOf[Int])
}