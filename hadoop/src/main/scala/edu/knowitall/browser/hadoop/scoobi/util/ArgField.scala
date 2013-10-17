package edu.knowitall.browser.hadoop.scoobi.util

import edu.knowitall.openie.models.ReVerbExtraction
import edu.knowitall.openie.models.FreeBaseEntity
import edu.knowitall.openie.models.FreeBaseType
import edu.knowitall.openie.models.Instance
import edu.knowitall.browser.hadoop.scoobi.ExtractionTuple

sealed abstract class ArgField {
  // abstract members
  def name: String
  def getArgNorm[T <: ExtractionTuple](reg: T): String
  def getTypeStrings[T <: ExtractionTuple](reg: T): Set[String]
  def attachTypes[T <: ExtractionTuple](reg: T, typeInts: Seq[Int]): T
  def loadEntityInfo[T <: ExtractionTuple](group: T): Option[EntityInfo]

  // concrete members
  protected def fbTypeToString(fbType: FreeBaseType): String = "/%s/%s".format(fbType.domain, fbType.typ)
  protected def intToFbType(typeInt: Int): Option[String] = TypeInfoUtils.typeEnumMap.get(typeInt).map(_.typeString)
  protected def typeToInt(typ: String): Int = TypeInfoUtils.typeStringMap(typ).enum

}

case object Arg1 extends ArgField {
  override val name = "arg1"
  override def getArgNorm[T <: ExtractionTuple](reg: T) = reg.arg1Norm
  override def getTypeStrings[T <: ExtractionTuple](reg: T) = reg.arg1Types filter TypeInfoUtils.typeFilter
  override def attachTypes[T <: ExtractionTuple](reg: T, typeInts: Seq[Int]) = reg.setArg1Types(typeInts flatMap intToFbType toSet).asInstanceOf[T]
  override def loadEntityInfo[T <: ExtractionTuple](reg: T): Option[EntityInfo] = reg.arg1EntityId map { e =>
    EntityInfo(e, getTypeStrings(reg) map typeToInt)
  }
}
case object Arg2 extends ArgField {
  override val name = "arg2"
  override def getArgNorm[T <: ExtractionTuple](reg: T) = reg.arg2Norm
  override def getTypeStrings[T <: ExtractionTuple](reg: T) = reg.arg2Types filter TypeInfoUtils.typeFilter
  override def attachTypes[T <: ExtractionTuple](reg: T, typeInts: Seq[Int]) = reg.setArg2Types(typeInts flatMap intToFbType toSet).asInstanceOf[T]
  override def loadEntityInfo[T <: ExtractionTuple](reg: T): Option[EntityInfo] = reg.arg2EntityId map { e =>
    EntityInfo(e, getTypeStrings(reg) map typeToInt)
  }
}