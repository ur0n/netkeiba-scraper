package netkeibascraper

import java.util.{Calendar, Date}

object DateRe {

  val dateRe =
    "(\\d\\d\\d\\d)年(\\d\\d?)月(\\d\\d?)日".r

  def unapply(s: String) = {
    s match {
      case dateRe(y, m, d) =>
        val cal = Calendar.getInstance
        cal.set(y.toInt, m.toInt - 1, d.toInt)
        Some(new Date(cal.getTimeInMillis))
      case _ =>
        None
    }
  }

}
