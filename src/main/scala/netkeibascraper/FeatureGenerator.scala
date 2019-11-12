package netkeibascraper

import scalikejdbc._

import scala.concurrent.{ExecutionContext, Future}

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

  def preSRa(horseId: Int, date: String)(implicit ec: ExecutionContext, session: DBSession): Future[Option[Int]] = {
    Future {
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
        limit 1
      """.map(_.int("speed_figure")).single.apply()
    }
  }

  def avgsr4(horseId: Int, date: String)(implicit ec: ExecutionContext, session: DBSession): Future[Option[Double]] = {
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

  def avgWin4(horseId: Int, date: String)(implicit ec: ExecutionContext, session: DBSession): Future[Option[Double]] = {
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

  def disavesr(horseId: Int, date: String, distance: Int)(implicit ec: ExecutionContext, session: DBSession): Future[Option[Double]] = {
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

  def disRoc(horseId: Int, date: String, distance: Int)(implicit ec: ExecutionContext, session: DBSession): Future[Option[Double]] = {
    Future {
      val distances =
        sql"""
          select
            distance
          from
            race_result
          inner join
            race_info
          on race_result.race_id = race_info.id
          where
            horse_id = ${horseId}
          and
            race_info.date < ${date}
          order by date desc
          limit 100
        """.map(_.double("distance")).list.apply()

      if (distances.isEmpty)
        None
      else {
        val mean = distances.sum.toDouble / distances.size
        Some((distance - mean) / mean)
      }
    }
  }

  def eps(horseId: Int, date: String)(implicit ec: ExecutionContext, session: DBSession): Future[Option[Double]] = {
    Future {
      val earning_money =
        sql"""
          select
            earning_money
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
        """.map(_.doubleOpt("earning_money")).list.apply()

      if (earning_money.isEmpty)
        None
      else
        Some(earning_money.flatten.sum / earning_money.size)
    }
  }

  def winRun(horseId: Int, date: String)(implicit ec: ExecutionContext, session: DBSession): Future[Option[Double]] = {
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
            horse_id = ${horseId}
          and
            race_info.date < ${date}
          limit 100
        """.map(_.int("is_win")).list.apply()

      if (wins.isEmpty)
        None
      else
        Some(wins.sum.toDouble / wins.size)
    }
  }

  def twinper(horseId: Int, date: String, trainerId: String)(implicit ec: ExecutionContext, session: DBSession): Future[Option[Double]] = {
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

  def owinper(horseId: Int, date: String, ownerId: String)(implicit ec: ExecutionContext, session: DBSession): Future[Option[Double]] = {
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

  def dsl(horseId: Int, date: String, ownerId: String)(implicit ec: ExecutionContext, session: DBSession): Future[Option[Double]] = {
    Future {
      sql"""
        select
          (julianday(${date}) - julianday(date)) as dsl
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
        limit 1
      """.map(_.double("dsl")).single.apply()
    }
  }

  def enterTimes(horseId: Int, date: String, ownerId: String)(implicit ec: ExecutionContext, session: DBSession): Future[Double] = {
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

  def jEps(date: String, jockeyId: String)(implicit ec: ExecutionContext, session: DBSession): Future[Option[Double]] = {
    Future {
      val earning_money =
        sql"""
          select
            earning_money
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
        """.map(_.doubleOpt("earning_money")).list.apply()

      if (earning_money.isEmpty)
        None
      else
        Some(earning_money.flatten.sum / earning_money.size)
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

  def jWinperOf(date: String, jockeyId: String)(implicit ec: ExecutionContext, session: DBSession): Future[Option[Double]] = {
    Future {
      val wins =
        sql"""
          select
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
        """.map(_.double("is_win")).list.apply()

      if (wins.nonEmpty) Some(wins.sum / wins.size)
      else None
    }
  }

  def preJockeyIdOpt(horseId: String, date: String, jockeyId: String)(implicit ec: ExecutionContext, session: DBSession): Future[Option[String]] = {
    Future {
      sql"""
        select
          jockey_id
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
        limit 1
      """.map(_.string("jockey_id")).single.apply()
    }
  }

  def preOOF(horseId: String, date: String)(implicit ec: ExecutionContext, session: DBSession): Future[Option[String]] = {
    Future {
      sql"""
        select
          order_of_finish
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
        limit 1
      """.map(_.string("order_of_finish")).single.apply()
    }
  }

  def pre2OOF(horseId: String, date: String)(implicit ec: ExecutionContext, session: DBSession): Future[Option[String]] = {
    Future {
      val orders =
        sql"""
          select
            order_of_finish
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
          limit 2
        """.map(_.string("order_of_finish")).list.apply()

      if (orders.size == 2) orders.lastOption
      else None
    }
  }

  def preLateStart(horseId: String, date: String)(implicit ec: ExecutionContext, session: DBSession): Future[Boolean] = {
    Future {
      val preRemark =
        sql"""
          select
            remark
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
          limit 1
        """.map(_.stringOpt("remark")).single.apply().flatten

      preRemark.nonEmpty && preRemark.get == "出遅れ"
    }
  }

  def lateStartPer(horseId: String, date: String)(implicit ec: ExecutionContext, session: DBSession): Future[Double] = {
    Future {
      val lateList =
        sql"""
          select
            (remark = '出遅れ') as is_late
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
        """.map(_.intOpt("is_late").getOrElse(0)).list.apply()

      lateList.sum.toDouble / lateList.size
    }
  }

  def preLastPhase(horseId: String, date: String)(implicit ec: ExecutionContext, session: DBSession): Future[Option[Double]] = {
    Future {
      sql"""
        select
          last_phase
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
        limit 1
      """.map(_.doubleOpt("last_phase")).single.apply().flatten
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

  def preRaceIdOpt(horseId: String, date: String)(implicit ec: ExecutionContext, session: DBSession): Future[Option[Int]] = {
    Future {
      sql"""
        select
          race_id
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
        limit 1
      """.map(_.int("race_id")).single.apply()
    }
  }

  def preMargin(horseId: String, date: String)(implicit ec: ExecutionContext, session: DBSession): Future[Option[String]] = {
    Future {
      sql"""
        select
          length
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
        limit 1
      """.map(_.stringOpt("length")).single.apply().flatten.map(Util.margin)
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
}

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

  private val preJockeyIdOpt: Option[String] = Some("")

  val ridingStrongJockey: Option[Boolean] = for {
    preJockeyId <- preJockeyIdOpt
    preWinper <- jWinperOf(preJockeyId)
    winper <- jwinper
  } yield preJockeyId != jockey_id && preWinper < winper

  val preOOF: Option[String] = Some("")

  val pre2OOF: Option[String] = Some("")

  val runningStyle: Double = {
    val orders =
      sql"""
select
  order_of_finish,
  pass
from
  race_result
inner join
  race_info
on
  race_result.race_id = race_info.id
where
  horse_id = ${horse_id}
and
  race_info.date < ${info.date}
order by date desc
limit 100
""".map(rs => (rs.string("order_of_finish"), rs.string("pass"))).
        list.
        apply

    val diff =
      orders.filter(_._1.forall(c => '0' < c && c < '9')).
        filterNot(_._2.isEmpty).
        map { case (order_of_finish, pass) =>
          val xs = pass.split("-")
          xs.map(_.toInt).sum.toDouble / xs.size - order_of_finish.toInt
        }

    diff.sum / diff.size
  }

  val preLateStart: Boolean = true

  val lateStartPer: Double = 0

  // ここまでやった
  val preLastPhase: Option[Double] = Some(0)

  val course: String = Util.course(info.surface)

  val placeCode: String = ""

  val headCount: Double = 0

  private val preRaceIdOpt: Option[Int] = Some(1)

  // headCount by pre race id
  val preHeadCount: Option[Double] = preRaceIdOpt.map(_ => 0)

  val surfaceChanged: Option[Boolean] = preRaceIdOpt.map(preRaceId => {
    val info = RaceInfoDao.getById(preRaceId)
    surface != Util.surface(info.surface)
  })

  val gradeChanged: Option[Int] = preRaceIdOpt.map(preRaceId => {
    val info = RaceInfoDao.getById(preRaceId)
    Util.str2cls(info.race_class) - grade
  })

  val preMargin: Option[String] = Some("")

  val femaleOnly: Boolean = info.race_class.contains("牝")
}

