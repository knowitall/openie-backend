package edu.knowitall.openie.models.util

import java.io.StringReader
import edu.knowitall.tool.chunk.ChunkedToken
import edu.knowitall.tool.postag.PostaggedToken
import edu.knowitall.tool.tokenize.Token
import uk.ac.susx.informatics.Morpha
import edu.knowitall.tool.stem.MorphaStemmer

/**
 * Copied and translated from a java implementation, which itself was originally adapted from other Java source
 * Has the advantage that is takes postagged tokens and passes postags to morpha.
 */
object TaggedStemmer extends TaggedStemmer

class TaggedStemmer private (private val lexer: MorphaStemmer) {
  def this() = this(new MorphaStemmer)

  // There must have been some slight difference between
  // two different POS tag sets:
  private def mapTag(posTag: String): String = {
    if (posTag.startsWith("NNP"))
      "NP";
    else
      posTag;
  }

  def stemAll(tokens: Seq[PostaggedToken]): Iterable[String] = {
    tokens map stem
  }

  def stem(token: PostaggedToken): String = stem(token.string, token.postag)

  // this still closely mirrors the original textrunner implementation
  private def stem(word: String, oldTag: String): String = {
    val tag = mapTag(oldTag)
    var returnVal = lexer.stem(word, tag)

    // Morpha doesn't properly singularize a plural proper noun.
    if (oldTag.equalsIgnoreCase("NNPS")) {
      if (returnVal.endsWith("es") && returnVal.length() > 2) {
        returnVal = returnVal.substring(0, returnVal.length() - 2)
      } else if (returnVal.endsWith("s")) {
        returnVal = returnVal.substring(0, returnVal.length() - 1)
      }
    }

    return returnVal;
  }

  private def isPunct(tag: String): Boolean = {
    !Character.isLetter(tag.charAt(0))
  }
}
