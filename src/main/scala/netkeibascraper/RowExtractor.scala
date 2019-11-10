package netkeibascraper

import java.io.{File, StringReader}

import nu.validator.htmlparser.sax.HtmlParser
import org.apache.commons.io.FilenameUtils

import scala.util.Try
import scala.xml.InputSource
import scala.xml.parsing.NoBindingFactoryAdapter
import scalikejdbc._

object RowExtractor {

  def str2raceInfo(race_info: Array[String]): RaceInfo = {
    val DateRe(d) = race_info(8)
    RaceInfo(race_info(0),
      race_info(1),
      race_info(2).toInt,
      race_info(3),
      race_info(4),
      race_info(5),
      race_info(6).toInt,
      Try(race_info(7).toInt).toOption,
      Util.date2sqliteStr(d),
      race_info(9),
      race_info(10))
  }

  def str2raceResult(race_id: Int, race_result: Array[String]): RaceResult = {
    RaceResult(race_id,
      race_result(0),
      race_result(1).toInt,
      race_result(2).toInt,
      race_result(3).split("horse/")(1).takeWhile(_ != '/'),
      race_result(4),
      race_result(5).toInt,
      race_result(6).toDouble,
      race_result(7).split("jockey/")(1).takeWhile(_ != '/'),
      race_result(8),
      race_result(9),
      Try(race_result(10).toInt).toOption,
      race_result(11),
      Try(race_result(12).toDouble).toOption,
      Try(race_result(13).toDouble).toOption,
      Try(race_result(14).toInt).toOption,
      race_result(15),
      {
        val remark = race_result(18)
        if (remark.isEmpty) None else Some(remark)
      },
      race_result(19),
      race_result(20).split("trainer/")(1).takeWhile(_ != '/'),
      race_result(21).split("owner/")(1).takeWhile(_ != '/'),
      Try(race_result(22).toDouble).toOption
    )
  }

  private def parseHtml(html: String) = {
    val hp = new HtmlParser
    val saxer = new NoBindingFactoryAdapter
    hp.setContentHandler(saxer)
    hp.parse(new InputSource(new StringReader(html)))
    saxer.rootElem
  }

  def extract()(implicit s: DBSession) = {

    val files = new File("html").listFiles().reverse

    val entityId = """<a href="/\w+/(\d+)/"[^>]*>[^<]*</a>"""

    val oikiriIconLink =
      """<a href="/\?pid=horse_training&amp;id=\d+&amp;rid=\d+"><img src="/style/netkeiba.ja/image/ico_oikiri.gif" border="0" height="13" width="13"/></a>"""

    val commentIconLink =
      """<a href="/\?pid=horse_comment&amp;id=\d+&amp;rid=\d+"><img src="/style/netkeiba.ja/image/ico_comment.gif" border="0" height="13" width="13"/></a>"""

    def clean(s: String) = {
      s.replaceAll("<span>|</span>|<span/>", "").
        replaceAll(",", "").
        replaceAll(entityId, "$1").
        replaceAll(oikiriIconLink, "").
        replaceAll(commentIconLink, "")
    }

    val usefulFiles =
      files.
        //タイム指数が表示される2006年以降のデータだけ使う
        filter(file => FilenameUtils.getBaseName(file.getName).take(4) >= "2006").
        //以下のデータは壊れているので除外する。恐らくnetkeiba.comの不具合。
        filter(file => FilenameUtils.getBaseName(file.getName) != "200808020398").
        filter(file => FilenameUtils.getBaseName(file.getName) != "200808020399").
        toArray.
        reverse

    var i = 0

    while (i < usefulFiles.size) {

      val file = usefulFiles(i)
      i += 1

      val lines = io.Source.fromFile(file).getLines.toList

      val rdLines = {
        lines.
          dropWhile(!_.contains("<dl class=\"racedata fc\">")).
          takeWhile(!_.contains("</dl>"))
      }

      val rdHtml = rdLines.mkString + "</dl>"

      val (condition, name) = {
        val elem = parseHtml(rdHtml)

        val condition = elem.\\("span").text
        val name = elem.\\("h1").text
        (condition, name)
      }

      val round = file.getName.replace(".html", "").takeRight(2)

      def extractDateInfo(lines: Seq[String]) = {
        lines.
          toList.
          dropWhile(!_.contains("<p class=\"smalltxt\">")).
          tail.head.replaceAll("<[^>]+>", "")
      }

      val dateInfo = extractDateInfo(lines).trim

      val fieldScore = Try {
        val rt2Lines = {
          lines.
            dropWhile(!_.contains("class=\"result_table_02\"")).
            takeWhile(!_.contains("</table>"))
        }

        val rt2Html = rt2Lines.mkString + "</table>"

        val text = parseHtml(rt2Html).\\("td").head.text
        text.filter(_ != '?').replaceAll("\\(\\s*\\)", "").trim.toInt.toString
      }.getOrElse("")

      val rt1Lines = {
        lines.
          dropWhile(s => !s.contains("race_table_01")).
          takeWhile(s => !s.contains("/table"))
      }

      val rt1Html = rt1Lines.mkString.drop(17) + "</table>"

      val conditions = {
        condition.
          split("/").
          map(_.replace("天候 :", "").
            replace("発走 : ", ""))
      }

      val raceInfoStr =
        (Array(name) ++
          (conditions.head.
            replaceAll(" ", "").
            replaceAll("([^\\d]+)(\\d+)m", "$1,$2") +: conditions.tail) ++
          Array(round.split(" ").head, fieldScore) ++ {
          val di = dateInfo.split(" ")
          di.take(2) :+ di.drop(2).mkString(" ")
        }).
          map(_.filter(_ != '?').replaceAll("(^\\h*)|(\\h*$)", "")).
          mkString(",").split(",")

      val raceInfo = str2raceInfo(raceInfoStr)

      RaceInfoDao.insert(raceInfo)

      val lastRowId = RaceInfoDao.lastRowId()

      val payoffLines = {
        lines.
          dropWhile(!_.contains("<dl class=\"pay_block\">")).
          takeWhile(!_.contains("</dl>"))
      }
      val payoffHtml =
        payoffLines.mkString + "</dl>"
      val payoffs = {
        parseHtml(payoffHtml).\\("tr").map { tr =>
          val ticketType = tr.\\("th").text.trim
          tr.\\("td").map(_.toString.replaceAll("</?td[^>]*>", "").split("<br/>").map(_.trim.replaceAll(",", ""))).transpose.
            map((ticketType, _))
        }.flatten.
          map { case (ticketStr, list) =>
            Payoff(
              race_id = lastRowId,
              ticket_type = Util.str2ticketType(ticketStr),
              horse_number = list(0),
              payoff = list(1).toDouble,
              popularity = list(2).toInt
            )
          }
      }

      payoffs.foreach(PayoffDao.insert)

      Try {
        parseHtml(rt1Html).\\("tr").tail
      }.foreach { xs =>
        xs.map(_.\\("td").map(_.child.mkString).map(clean)).
          foreach { line =>
            val row =
              line.map(_.replaceAll("  ", "").
                replaceAll("<diary_snap_cut></diary_snap_cut>", "").
                replaceAll("(牝|牡|セ)(\\d{1,2})", "$1,$2").
                replaceAll("\\[(西|地|東|外)\\]", "$1,")).
                map(_.filter(_ != '?').trim).
                mkString(",").split(",")
            val raceResult = str2raceResult(lastRowId, row)
            RaceResultDao.insert(raceResult)
          }
      }
    }
  }

}
