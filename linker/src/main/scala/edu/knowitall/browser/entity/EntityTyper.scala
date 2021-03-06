package edu.knowitall.browser.entity

import java.io.File

import edu.knowitall.browser.entity.util.FbTypeLookup
import scala.collection.JavaConversions.seqAsJavaList

/**
  * Does type lookup for freebase entities (fills in the argXTypes field in an extractionGroup)
  */
class EntityTyper(val fbLookupTable: FbTypeLookup) {

  def this(basePath: File) = this(
    new FbTypeLookup(
      Constants.typeLookupIndexPath(basePath),
      Constants.freeBaseTypeEnumFile(basePath)
    )
  )
    
  /**
   * mutator method to
   */
  def typeEntity(link: EntityLink): EntityLink = {

    val fbid = link.entity.fbid

    val types = fbLookupTable.getTypesForEntity(link.entity.fbid)

    link.attachTypes(types)

    return link
  }

  def typeFbid(fbid: String): Iterable[String] = fbLookupTable.getTypesForEntity(fbid)
}

object EntityTyper {
  val typeLookupIndex = "type-lookup-index"
  val fbTypeEnumFile = "fbTypeEnum.txt"
}
