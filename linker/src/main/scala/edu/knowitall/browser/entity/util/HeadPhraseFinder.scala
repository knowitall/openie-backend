package edu.knowitall.browser.entity.util

import edu.knowitall.browser.util.CrosswikisHandler
import edu.knowitall.tool.postag.PostaggedToken
import edu.knowitall.browser.entity.CandidateFinder

object HeadPhraseFinder {
  def getHeadPhrase(postagTokens: Seq[PostaggedToken], candidateFinder: CandidateFinder): String = {
    var headPhrase = postagTokens

    // Strip "(DT | CD | JJ | RBS) of" from the beginning.
    if (postagTokens.length >= 3 && postagTokens(1).string == "of") {
      val firstToken = postagTokens(0)
      if (firstToken.isDeterminer || firstToken.isCardinalNumber || firstToken.isPlainAdjective
          || firstToken.isSuperlativeAdverb) {
        headPhrase = headPhrase.drop(2)
      }
    }

    // Strip "(DT)+ JJ of" from the beginning.
    if (postagTokens.length >= 4 && postagTokens(0).isDeterminer) {
      val dtIndex = postagTokens.lastIndexWhere(postagToken => postagToken.isDeterminer)
      if (postagTokens.length >= dtIndex+4 && postagTokens(dtIndex+1).isPlainAdjective
          && postagTokens(dtIndex+2).string == "of") {
        headPhrase = postagTokens.drop(dtIndex+3)
      }
    }

    // Truncate at first punctuation, conjunction, or preposition.
    val truncateIndex = headPhrase.indexWhere(postagToken =>
      postagToken.isPunctuation || postagToken.isConjunction || postagToken.isPreposition
    )
    if (truncateIndex + 1 < headPhrase.length) {
      headPhrase = headPhrase.drop(truncateIndex + 1)
    }

    // Remove any post modifiers.
    val lastNounIndex = headPhrase.lastIndexWhere(postagToken =>
      postagToken.isNoun || postagToken.isPluralNoun
    )
    if (lastNounIndex >= 0) {
      headPhrase = headPhrase.take(lastNounIndex + 1)
    }

    // Remove determiners and possessive pronouns.
    val filtered = headPhrase.filter { token => !(token.isDeterminer || token.isPossessivePronoun) }
    if (!filtered.isEmpty) {
      headPhrase = filtered
    }

    // Check if the phrase is in Crosswikis, stripping off the leading word until one is found. If
    // no phrase is found, return headPhrase.
    var dropIndex = 0
    while (headPhrase.length > dropIndex
        && !candidateFinder.hasCandidates(headPhrase.drop(dropIndex).map(_.string).mkString(" "))) {
      dropIndex += 1
    }
    if (dropIndex != headPhrase.length) {
      headPhrase = headPhrase.drop(dropIndex)
    }
    headPhrase.map(_.string).mkString(" ")
  }
}
