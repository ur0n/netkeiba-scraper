package netkeibascraper

import scalikejdbc._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.Exception._

object FeatureGenerator {

  def iterator()(implicit s: DBSession): Iterator[FeatureGenerator] = {
    println("iterator")
    val race_infos = {
      sql"select race_id, horse_number from race_result".
        map(rs => (rs.int("race_id"), rs.int("horse_number"))).
        list.
        apply
    }

    var count = 0
    val totalCount = race_infos.size.toDouble

    println(totalCount)
    race_infos.map(info => {
      count += 1
      if (count % 1000 == 0) println("処理中 ... %7.3f％完了".format(100.0 * count / totalCount))
      println(info, count)
      new FeatureGenerator(info._1, info._2)
    }).toIterator
  }
}

object RaceResultRepository {
  def findByRaceIdAndHorseNumber(raceId: Int, horseNumber: Int)(implicit ec: ExecutionContext, session: DBSession): Future[RaceResult] = {
    Future {
      sql"""
        select
          *
        from
          race_result
        where
          race_id = ${raceId}
        and
          horse_number = ${horseNumber}
      """.map(dto2model).single().apply().get
    }
  }

  def pre(horseId: String, date: String, distance: Int)(implicit ec: ExecutionContext, session: DBSession): Future[PreRating] = {
    Future {
      val pre100 =
        sql"""
        select
          race_id,
          jockey_id,
          distance,
          earning_money,
          speed_figure,
          order_of_finish,
          (order_of_finish = '1') as is_win,
          remark,
          (remark = '出遅れ') as is_late,
          last_phase,
          length,
          (julianday(${date}) - julianday(date)) as dsl
          pass
        from
          race_result
        inner join
          race_info
        on
          race_result.race_id = race_info.id
        where
          horse_id = ${horseId}
        and
          race_info.date < ${date}
        order by date desc
        limit 100
      """.map(dto2Pre).list().apply()

      val raceId = pre100.head.raceId
      val jockeyId = pre100.head.jockeyId
      val oof = pre100.head.oof
      val lateStart = pre100.head.lateStart
      val lastPhase = pre100.head.lastPhase
      val margin = pre100.head.margin

      val firstHavingSpeedFigure: Option[Pre] = pre100.collectFirst { case pre if pre.speedFigure.nonEmpty => pre }
      val sra: Option[Int] = firstHavingSpeedFigure.flatMap(_.speedFigure)
      val dsl: Option[Double] = firstHavingSpeedFigure.map(_.dsl)
      val oof2 = allCatch opt {
        val oofArr = pre100.map(_.oof).toArray
        oofArr(1)
      }

      val lateList = pre100.map(_.isLate)
      val lateStartPer = lateList.sum.toDouble / lateList.size

      val (disRoc, eps, winRun) = if (pre100.isEmpty) {
        (None, None, None)
      } else {
        val distances = pre100.map(_.distance)
        val mean = distances.sum.toDouble / distances.size
        val disRoc = Some((distance - mean) / mean)

        val earningMoney = pre100.map(_.earningMoney).collect { case Some(em) => em }
        val eps = Some(earningMoney.sum / earningMoney.size)

        val wins = pre100.map(_.isWin)
        val winRun = Some(wins.sum.toDouble / wins.size)

        (disRoc, eps, winRun)
      }

      val diff = pre100
        .filter(pre => pre.oof.forall(c => '0' < c && c < '9'))
        .filterNot(pre => pre.pass.isEmpty)
        .map(pre => {
          val xs = pre.pass.split("-")
          xs.map(_.toInt).sum.toDouble / xs.size - pre.oof.toInt
        })

      val runningStyle = diff.sum / diff.size

      PreRating(raceId, jockeyId, disRoc, eps, sra, dsl, oof, oof2, winRun, lateStart, lateStartPer, lastPhase, margin, runningStyle)
    }
  }

  def avgsr4(horseId: String, date: String)(implicit ec: ExecutionContext, session: DBSession): Future[Option[Double]] = {
    Future {
      val srs =
        sql"""
        select
          speed_figure
        from
          race_result
        inner join
          race_info
        on
          race_result.race_id = race_info.id
        where
          horse_id = ${horseId}
        and
          race_info.date < ${date}
        and
          speed_figure is not null
        order by date desc
        limit 4
      """.map(_.double("speed_figure")).list.apply()

      if (srs.isEmpty)
        None
      else
        Some(srs.sum / srs.size)
    }
  }

  def avgWin4(horseId: String, date: String)(implicit ec: ExecutionContext, session: DBSession): Future[Option[Double]] = {
    Future {
      val wins =
        sql"""
          select
            (order_of_finish = '1' or order_of_finish = '2' or order_of_finish = '3') as is_win
          from
            race_result
          inner join
            race_info
          on
            race_result.race_id = race_info.id
          where
            horse_id = ${horseId}
          and
            race_info.date < ${date}
          and
            speed_figure is not null
          order by date desc
          limit 4
        """.map(_.double("is_win")).list.apply()

      if (wins.isEmpty)
        None
      else
        Some(wins.sum / wins.size)
    }
  }

  def jAvgWin4(date: String, jockeyId: String)(implicit ec: ExecutionContext, session: DBSession): Future[Option[Double]] = {
    Future {
      val wins =
        sql"""
          select
            (order_of_finish = '1' or order_of_finish = '2' or order_of_finish = '3') as is_win
          from
            race_result
          inner join
            race_info
          on
            race_result.race_id = race_info.id
          where
            jockey_id = ${jockeyId}
          and
            race_info.date < ${date}
          and
            speed_figure is not null
          order by date desc
          limit 4
        """.map(_.double("is_win")).list.apply()

      if (wins.isEmpty)
        None
      else
        Some(wins.sum / wins.size)
    }
  }


  def disavesr(horseId: String, date: String, distance: Int)(implicit ec: ExecutionContext, session: DBSession): Future[Option[Double]] = {
    Future {
      val srs =
        sql"""
          select
            speed_figure
          from
            race_result
          inner join
            race_info
          on
            race_result.race_id = race_info.id
          where
            horse_id = ${horseId}
          and
            race_info.date < ${date}
          and
            distance = ${distance}
          and
            speed_figure is not null
          order by date desc
          limit 100
        """.map(_.double("speed_figure")).list.apply()

      if (srs.isEmpty)
        None
      else
        Some(srs.sum / srs.size)
    }
  }

  def twinper(horseId: String, date: String, trainerId: String)(implicit ec: ExecutionContext, session: DBSession): Future[Option[Double]] = {
    Future {
      val wins =
        sql"""
          select
            (order_of_finish = '1') as is_win
          from
            race_result
          inner join
            race_info
          on
            race_result.race_id = race_info.id
          where
            trainer_id = ${trainerId}
          and
            race_info.date < ${date}
          order by date desc
          limit 100
        """.map(_.int("is_win")).list.apply

      if (wins.isEmpty)
        None
      else
        Some(wins.sum.toDouble / wins.size)
    }
  }

  def owinper(horseId: String, date: String, ownerId: String)(implicit ec: ExecutionContext, session: DBSession): Future[Option[Double]] = {
    Future {
      val wins =
        sql"""
          select
            (order_of_finish = '1') as is_win
          from
            race_result
          inner join
            race_info
          on
            race_result.race_id = race_info.id
          where
            owner_id = ${ownerId}
          and
            race_info.date < ${date}
          order by date desc
          limit 100
        """.map(_.int("is_win")).list.apply

      if (wins.isEmpty)
        None
      else
        Some(wins.sum.toDouble / wins.size)
    }
  }


  def enterTimes(horseId: String, date: String)(implicit ec: ExecutionContext, session: DBSession): Future[Double] = {
    Future {
      sql"""
        select
          count(*) as count
        from
          race_result
        inner join
          race_info
        on
          race_result.race_id = race_info.id
        where
          horse_id = ${horseId}
        and
          race_info.date < ${date}
      """.map(_.double("count")).single.apply.get
    }
  }

  def headCount(raceId: Int)(implicit ec: ExecutionContext, session: DBSession): Future[Double] = {
    Future {
      sql"""
        select
          count(*) as head_count
        from
          race_result
        where
          race_id = ${raceId}
      """.map(_.double("head_count")).single.apply.get
    }
  }

  def preByJockey(jockeyId: String, date: String)(implicit ec: ExecutionContext, session: DBSession): Future[PreByJockey] = {
    Future {
      val byJockey =
        sql"""
          select
            earning_money,
            order_of_finish = '1' as is_win
          from
            race_result
          inner join
            race_info
          on
            race_result.race_id = race_info.id
          where
            jockey_id = ${jockeyId}
          and
            race_info.date < ${date}
          order by date desc
          limit 100
        """.map(result => (result.doubleOpt("earning_money"), result.double("is_win"))).list.apply()

      if (byJockey.isEmpty) {
        PreByJockey(None, None)
      } else {
        val earningMoney = byJockey.map(_._1).collect { case Some(em) => em }
        val wins = byJockey.map(_._2)

        PreByJockey(
          Some(earningMoney.sum / earningMoney.size),
          Some(wins.sum / wins.size)
        )
      }
    }
  }

  private def dto2model(result: WrappedResultSet): RaceResult = {
    RaceResult(
      result.int("race_id"),
      result.string("order_of_finish"),
      result.int("frame_number"),
      result.int("horse_number"),
      result.string("horse_id"),
      result.string("sex"),
      result.int("age"),
      result.int("basis_weight"),
      result.string("jockey_id"),
      result.string("finishing_time"),
      result.string("length"),
      result.intOpt("speed_figure"),
      result.string("pass"),
      result.doubleOpt("last_phase"),
      result.doubleOpt("odds"),
      result.intOpt("popularity"),
      result.string("horse_weight"),
      result.stringOpt("remark"),
      result.string("stable"),
      result.string("trainer_id"),
      result.string("owner_id"),
      result.doubleOpt("earning_money")
    )
  }

  private def dto2Pre(result: WrappedResultSet): Pre = {
    val raceId = result.int("race_id")
    val jockeyId = result.string("jockey_id")
    val speedFigure = result.intOpt("speed_figure")
    val oof = result.string("order_of_finish")
    val lateStart = result.stringOpt("remark").map(remark => remark == "出遅れ")
    val isLate = result.intOpt("is_late").getOrElse(0)
    val lastPhase = result.doubleOpt("last_phase")
    val margin = Util.margin(result.string("length"))
    val pass = result.string("pass")

    val distance = result.double("distance")
    val earningMoney = result.doubleOpt("earning_money")
    val isWin = result.int("is_win")
    val dsl = result.double("dsl")

    Pre(
      raceId,
      jockeyId,
      distance,
      earningMoney,
      speedFigure,
      dsl,
      oof,
      isWin,
      lateStart,
      isLate,
      lastPhase,
      margin,
      pass
    )
  }
}

case class Pre(
                raceId: Int,
                jockeyId: String,
                distance: Double,
                earningMoney: Option[Double],
                speedFigure: Option[Int],
                dsl: Double,
                oof: String,
                isWin: Int,
                lateStart: Option[Boolean],
                isLate: Int,
                lastPhase: Option[Double],
                margin: String,
                pass: String
              )

case class PreRating(
                      raceId: Int,
                      jockeyId: String,
                      disRoc: Option[Double],
                      eps: Option[Double],
                      sra: Option[Int],
                      dsl: Option[Double],
                      oof: String,
                      oof2: Option[String],
                      winRun: Option[Double],
                      lateStart: Option[Boolean],
                      lateStartPer: Double,
                      lastPhase: Option[Double],
                      margin: String,
                      runningStyle: Double
                    )

case class PreByJockey(
                        eps: Option[Double],
                        winPer: Option[Double]
                      )


class FeatureGenerator(
                        val race_id: Int,
                        val horse_number: Int
                      )(implicit s: DBSession) {
  assert(horse_number > 0)

  val info: RaceInfo = RaceInfoDao.getById(race_id)

  val grade: Int = Util.str2cls(info.race_class)

  val surfaceScore: Option[Int] = info.surface_score

  val order_of_finish: String = ""

  val horse_id: String = ""

  val jockey_id: String = ""

  val trainer_id: String = ""

  val owner_id: String = ""

  //Speed rating for the previous race in which the horse ran
  val preSRa: Option[Int] = Some(1)

  //The average of a horse’s speed rating in its last 4 races; value of zero when there is no past run
  val avgsr4: Option[Double] = Some(0)

  val avgWin4: Option[Double] = Some(0)

  //The average speed rating of the past runs of each horse at this distance; value of zero when no previous run
  val disavesr: Option[Double] = Some(0)

  val disRoc: Option[Double] = Some(0)

  //Total prize money earnings (finishing first, second or third) to date/Number of races entered
  val eps: Option[Double] = Some(0)

  val weight: Double = {
    sql"""
select
  basis_weight
from
  race_result
where
  race_id = ${race_id}
and
  horse_number = ${horse_number}
""".map(_.double("basis_weight")).
      single.
      apply.
      get
  }

  val hweight: Option[Int] = Some(0)

  val dhweight: Option[Int] = Some(0)

  val winRun: Option[Double] = Some(0)

  //The winning percentage of the trainer in career to date of race
  val twinper: Option[Double] = Some(0)

  //The winning percentage of the owner in career to date of race
  val owinper: Option[Double] = Some(0)

  //The winning percentage of the jockey in career to date of race
  val age: Double = 0

  val dsl: Option[Double] = Some(0)

  val surface: String = {
    Util.surface(info.surface)
  }

  val weather: String = {
    Util.weather(info.weather)
  }

  val sex: String = ""

  val enterTimes: Double = 0

  val odds: Option[Double] = Some(0)

  //Total prize money earnings (finishing first, second or third) to date/Number of races entered
  val jEps: Option[Double] = Some(0)

  val jAvgWin4: Option[Double] = Some(0)

  val month: String = info.date.split("-")(1)

  //The winning percentage of the jockey in career to date of race
  val jwinper: Option[Double] = jWinperOf(jockey_id)

  def jWinperOf(jockey_id: String)(implicit s: DBSession): Option[Double] = Some(0)

  val ridingStrongJockey: Option[Boolean] = Some(true)

  val preOOF: Option[String] = Some("")

  val pre2OOF: Option[String] = Some("")

  val runningStyle: Double = 0

  val preLateStart: Boolean = true

  val lateStartPer: Double = 0

  val preLastPhase: Option[Double] = Some(0)

  val course: String = Util.course(info.surface)

  val placeCode: String = ""

  val headCount: Double = 0

  private val preRaceIdOpt: Option[Int] = Some(1)

  // headCount by pre race id
  val preHeadCount: Option[Double] = preRaceIdOpt.map(_ => 0)

  val surfaceChanged: Option[Boolean] = Some(true)

  val gradeChanged: Option[Int] = Some(1)

  val preMargin: Option[String] = Some("")

  val femaleOnly: Boolean = info.race_class.contains("牝")
}

