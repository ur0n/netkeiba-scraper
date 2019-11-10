import netkeibascraper._
import scalikejdbc._

object Main {

  def init() = {
    Class.forName("org.sqlite.JDBC")
    ConnectionPool.singleton("jdbc:sqlite:race.db", null, null)
  }

  def main(args: Array[String]): Unit = {
    args.headOption match {
      case Some("collecturl") =>
        //過去10年分のURLを収集する
        RaceListScraper.scrape(period = 12 * 10)
      case Some("scrapehtml") =>
        RaceScraper.scrape()
      case Some("extract") =>
        init()
        DB.localTx { implicit s =>
          RaceInfoDao.createTable()
          RaceResultDao.createTable()
          PayoffDao.createTable()
          RowExtractor.extract()
        }
      case Some("genfeature") =>
        GlobalSettings.loggingSQLAndTime = new LoggingSQLAndTimeSettings(
          enabled = false,
          logLevel = 'info
        )
        init()
        DB.localTx { implicit s =>
          FeatureDao.createTable()
        }
        FeatureDao.rr2f()
      case _ =>
        sys.error("invalid argument")
    }
  }
}
