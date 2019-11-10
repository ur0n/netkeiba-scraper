package netkeibascraper

import java.io.File

import org.apache.commons.io.FileUtils

object RaceListScraper {

  def extractRaceList(baseUrl: String) = {
    "/race/list/\\d+/".r.findAllIn(io.Source.fromURL(baseUrl, "EUC-JP").mkString).toList.
      map("https://db.netkeiba.com" + _).
      distinct
  }

  def extractPrevMonth(baseList: String) = {
    println(baseList)
    "/\\?pid=[^\"]+".r.findFirstIn(
      io.Source.fromURL(baseList, "EUC-JP").getLines
        .filter(line => {
          println(line)
          line.contains("race_calendar_rev_02.gif")
        }).toList.head).
      map("https://db.netkeiba.com" + _).
      get
  }

  def extractRace(listUrl: String) = {
    "/race/\\d+/".r.findAllIn(io.Source.fromURL(listUrl, "EUC-JP").mkString).toList.
      map("https://db.netkeiba.com" + _).
      distinct
  }

  def scrape(period: Int) = {
    var baseUrl = "https://db.netkeiba.com/?pid=race_top"
    var i = 0

    while (i < period) {
      Thread.sleep(1000)
      val raceListPages =
        extractRaceList(baseUrl)
      val racePages =
        raceListPages.map { url =>
          Thread.sleep(1000)
          extractRace(url)
        }.flatten

      racePages.foreach { url =>
        FileUtils.writeStringToFile(new File("race_url.txt"), url + "\n", true)
      }

      baseUrl = extractPrevMonth(baseUrl)
      println(i + ": collecting URLs from " + baseUrl)
      i += 1
    }
  }

}

