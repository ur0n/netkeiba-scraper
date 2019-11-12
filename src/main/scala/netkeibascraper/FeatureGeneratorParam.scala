package netkeibascraper

import io.github.hamsters.FutureOption
import scalikejdbc.DBSession

import scala.concurrent.{ExecutionContext, Future}

case class FeatureGeneratorParam(
                                  grade: Int,
                                  surfaceScore: Option[Int],
                                  order_of_finish: String,
                                  horse_id: String,
                                  jockey_id: String,
                                  trainer_id: String,
                                  owner_id: String,
                                  preSRa: Option[Int],
                                  avgsr4: Option[Double],
                                  avgWin4: Option[Double],
                                  disavesr: Option[Double],
                                  disRoc: Option[Double],
                                  eps: Option[Double],
                                  weight: Double,
                                  hweight: Option[Int],
                                  dhweight: Option[Int],
                                  winRun: Option[Double],
                                  twinper: Option[Double],
                                  owinper: Option[Double],
                                  age: Double,
                                  dsl: Option[Double],
                                  surface: String,
                                  weather: String,
                                  sex: String,
                                  enterTimes: Double,
                                  odds: Option[Double],
                                  jEps: Option[Double],
                                  jAvgWin4: Option[Double],
                                  month: String,
                                  jwinper: Option[Double],
                                  ridingStrongJockey: Option[Boolean],
                                  preOOF: Option[String],
                                  pre2OOF: Option[String],
                                  runningStyle: Double,
                                  preLateStart: Boolean,
                                  lateStartPer: Double,
                                  preLastPhase: Option[Double],
                                  course: String,
                                  placeCode: String,
                                  headCount: Double,
                                  preHeadCount: Option[Double],
                                  surfaceChanged: Option[Boolean],
                                  gradeChanged: Option[Int],
                                  preMargin: Option[String],
                                  femaleOnly: Boolean
                                )

object FeatureGeneratorParam {
  def create(raceId: Int, horseNumber: Int)(implicit ec: ExecutionContext, s: DBSession): Future[Option[FeatureGeneratorParam]] = {
    val info: RaceInfo = RaceInfoDao.getById(raceId)
    for {
      raceResult   <- FutureOption { RaceResultRepository.findByRaceIdAndHorseNumber(raceId, horseNumber).map(Some(_)) }
      preRating    <- FutureOption { RaceResultRepository.pre(raceResult.horse_id, info.date, info.distance).map(Some(_)) }
      byJockey     <- FutureOption { RaceResultRepository.preByJockey(preRating.jockeyId, info.date).map(Some(_)) }
      preByJockey  <- FutureOption { RaceResultRepository.preByJockey(raceResult.jockey_id, info.date).map(Some(_)) }
      avgsr4       <- FutureOption { RaceResultRepository.avgsr4(raceResult.horse_id, info.date) }
      avgWin4      <- FutureOption { RaceResultRepository.avgWin4(raceResult.horse_id, info.date) }
      disavesr     <- FutureOption { RaceResultRepository.disavesr(raceResult.horse_id, info.date, info.distance) }
      twinper      <- FutureOption { RaceResultRepository.twinper(raceResult.horse_id, info.date, raceResult.trainer_id) }
      owinper      <- FutureOption { RaceResultRepository.owinper(raceResult.horse_id, info.date, raceResult.owner_id) }
      enterTimes   <- FutureOption { RaceResultRepository.enterTimes(raceResult.horse_id, info.date).map(Some(_)) }
      jAvgWin4     <- FutureOption { RaceResultRepository.jAvgWin4(raceResult.horse_id, info.date) }
      headCount    <- FutureOption { RaceResultRepository.headCount(raceResult.race_id).map(Some(_)) }
      preHeadCount <- FutureOption { RaceResultRepository.headCount(preRating.raceId).map(Some(_)) }
    } yield {
      val grade: Int = Util.str2cls(info.race_class)
      val surfaceScore: Option[Int] = info.surface_score
      val orderOfFinish = raceResult.order_of_finish
      val horseId = raceResult.horse_id
      val jockeyId = raceResult.jockey_id
      val trainerId = raceResult.trainer_id
      val ownerId = raceResult.owner_id
      val preSRa = preRating.sra
      val disRoc = preRating.disRoc
      val eps = preRating.eps
      val weight = raceResult.basis_weight
      val hweight = raceResult.hweight
      val dhweight = raceResult.dhweight
      val winRun = preRating.winRun
      val age = raceResult.age
      val odds = raceResult.odds
      val jEps = preByJockey.eps
      val dsl = preRating.dsl

      val surface = Util.surface(info.surface)
      val weather = Util.weather(info.weather)
      val sex = raceResult.sex
      val month = info.date.split("-")(1)

      val jwinper = byJockey.winPer

      val ridingStrongJockey = for {
        preWinper <- preByJockey.winPer
        winper <- byJockey.winPer
      } yield {
        preRating.jockeyId != jockeyId && preWinper < winper
      }

      val preOOF = preRating.oof
      val pre2OOF = preRating.oof2
      val preLateStart = preRating.lateStart
      val lateStartPer = preRating.lateStartPer
      val preLastPhase = preRating.lastPhase
      val course = Util.course(info.surface)
      val placeCode = info.placeCode
      val preRaceId = preRating.raceId

      val preRaceInfo = RaceInfoDao.getById(preRaceId)
      val surfaceChanged = info.surface != Util.surface(preRaceInfo.surface)
      val gradeChanged = Util.str2cls(preRaceInfo.race_class) - grade
      val preMargin = preRating.margin
      val femaleOnly = info.race_class.contains("牝")
      val runningStyle = preRating.runningStyle

      FeatureGeneratorParam(
        grade,
        surfaceScore,
        orderOfFinish,
        horseId,
        jockeyId,
        trainerId,
        ownerId,
        preSRa,
        avgsr4,
        avgWin4,
        disavesr,
        disRoc,
        eps,
        weight,
        hweight,
        dhweight,
        winRun,
        twinper,
        owinper,
        age,
        dsl,
        surface,
        weather,
        sex,
        enterTimes,
        odds,
        jEps,
        jAvgWin4,
        month,
        jwinper,
        ridingStrongJockey,
        preOOF,
        pre2OOF,
        runningStyle,
        preLateStart,
        lateStartPer,
        preLastPhase,
        course,
        placeCode,
        headCount,
        preHeadCount,
        surfaceChanged,
        gradeChanged,
        preMargin,
        femaleOnly,
      )
    }
  }
}

