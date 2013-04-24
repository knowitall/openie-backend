package edu.knowitall.openie.models

object Resources {
  val groupsFile = "test-groups.txt"
  val groupsUrl = this.getClass.getResource(groupsFile)
  require(groupsUrl != null, "Could not find resource: " + groupsFile)

  val reverbExtractionsFile = "TestReVerbExtractions.txt"
  val reverbExtractionsUrl = this.getClass.getResource(reverbExtractionsFile)
  require(reverbExtractionsUrl != null, "Could not find resource: " + reverbExtractionsFile)
}