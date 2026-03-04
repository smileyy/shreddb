package shreddb.token

import shreddb.storage.Storage

class TokenLookupTable(private var valuesToTokens: Map[String, Int]) {
  private lazy val tokensToValues = valuesToTokens.map { (v, t) => (t, v) }
  
  def getOrAddToken(value: String): Int = {
    valuesToTokens.get(value) match {
      case Some(token) => token
      case None => {
        val nextToken = valuesToTokens.size
        valuesToTokens = valuesToTokens + (value -> nextToken)
        nextToken
      }
    }
  }
  
  def getToken(value: String): Option[Int] = {
    valuesToTokens.get(value)
  }
  
  def resolve(token: Int): String = {
    tokensToValues(token)
  }
  
  def size: Int = valuesToTokens.size
  
  def table: Map[String, Int] = valuesToTokens
  
  def values: Set[String] = valuesToTokens.keySet
}
object TokenLookupTable {
  def empty(): TokenLookupTable = {
    new TokenLookupTable(Map.empty)
  }
}

class TokenLookupTableBuilder(private var valuesToTokens: Map[String, Int]) {
  def add(token: Int, value: String): Unit = {
    valuesToTokens = valuesToTokens + (value -> token)
  }

  def build(): TokenLookupTable = {
    new TokenLookupTable(valuesToTokens)
  }
}
object TokenLookupTableBuilder {
  def newBuilder(): TokenLookupTableBuilder = new TokenLookupTableBuilder(Map.empty)
}