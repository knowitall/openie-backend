package edu.knowitall.browser.util

import scala.io.Source
import java.io.PrintStream
import java.io.FileOutputStream

import scopt.OptionParser

import edu.knowitall.common.Resource.using

import edu.knowitall.browser.extraction.ReVerbExtractionGroup
import edu.knowitall.browser.extraction.ExtractionGroup
import edu.knowitall.browser.extraction.ReVerbExtraction

/**
 * Reads in REGs and just writes them back out. Useful if you make a change to serialization, such
 * as truncating the number of decimals in confidence.
 */
object TestDataUpdater {
  
  type REG = ExtractionGroup[ReVerbExtraction]

  def main(args: Array[String]): Unit = {
    
    var inpath = ""
    var outpath = ""
      
    val parser = new OptionParser() {
      arg("input", "input file",   { s => inpath = s })
      arg("output", "output file", { s => outpath = s })
    }
    
    if (!parser.parse(args)) return
    
    def deserialize(line: String) = ReVerbExtractionGroup.deserializeFromString(line)
    def serialize(group: REG) = ReVerbExtractionGroup.serializeToString(group)
    
    using(Source.fromFile(inpath)) { source => 
      using(new PrintStream(new FileOutputStream(outpath))) { output => 
        source.getLines flatMap deserialize map serialize foreach output.println
      }
    }
  }
}