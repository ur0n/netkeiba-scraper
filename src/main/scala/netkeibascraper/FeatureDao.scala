package netkeibascraper

import scalikejdbc._

object FeatureDao {

  def createTable()(implicit s: DBSession) = {
    sql"""
create table if not exists feature (
  race_id integer not null,
  horse_number  integer not null,

  grade integer not null,
  order_of_finish integer,

  horse_id text not null,
  jockey_id text not null,
  trainer_id text not null,

  age real,
  avgsr4 real,
  avgWin4 real,
  dhweight real,
  disavesr real,
  disRoc real,
  distance real,
  dsl real,
  enterTimes real,
  eps real,
  hweight real,
  jwinper real,
  odds real,
  owinper real,
  preSRa real,
  sex text,
  surface text,
  surfaceScore real,
  twinper real,
  weather text,
  weight real,
  winRun real,

  jEps real,
  jAvgWin4 real,
  preOOF real,
  pre2OOF real,

  month real,
  ridingStrongJockey real,
  runningStyle real,
  preLateStart real,
  preLastPhase real,
  lateStartPer real,
  course text,
  placeCode text,

  headCount real,
  preHeadCount real,

  surfaceChanged real,
  gradeChanged real,

  preMargin real,
  femaleOnly real,

  primary key (race_id, horse_number)
);
""".execute.apply
  }

  def insert(fg: FeatureGenerator)(implicit s: DBSession) = {
    import Util.position2cls
    sql"""
insert or replace into feature (
  race_id,
  horse_number,
  grade,
  order_of_finish,

  horse_id,
  jockey_id,
  trainer_id,

  age,
  avgsr4,
  avgWin4,
  dhweight,
  disavesr,
  disRoc,
  distance,
  dsl,
  enterTimes,
  eps,
  hweight,
  jwinper,
  odds,
  owinper,
  preSRa,
  sex,
  surface,
  surfaceScore,
  twinper,
  weather,
  weight,
  winRun,

  jEps,
  jAvgWin4,
  preOOF,
  pre2OOF,

  month,
  ridingStrongJockey,
  runningStyle,
  preLateStart,
  preLastPhase,
  lateStartPer,
  course,
  placeCode,

  headCount,
  preHeadCount,

  surfaceChanged,
  gradeChanged,
  preMargin,
  femaleOnly

) values (
  ${fg.race_id},
  ${fg.horse_number},
  ${fg.grade},
  ${position2cls(fg.order_of_finish)._1},

  ${fg.horse_id},
  ${fg.jockey_id},
  ${fg.trainer_id},

  ${fg.age},
  ${fg.avgsr4},
  ${fg.avgWin4},
  ${fg.dhweight},
  ${fg.disavesr},
  ${fg.disRoc},
  ${fg.distance},
  ${fg.dsl},
  ${fg.enterTimes},
  ${fg.eps},
  ${fg.hweight},
  ${fg.jwinper},
  ${fg.odds},
  ${fg.owinper},
  ${fg.preSRa},
  ${fg.sex},
  ${fg.surface},
  ${fg.surfaceScore},
  ${fg.twinper},
  ${fg.weather},
  ${fg.weight},
  ${fg.winRun},
  ${fg.jEps},
  ${fg.jAvgWin4},
  ${fg.preOOF},

  ${fg.pre2OOF},
  ${fg.month},
  ${fg.ridingStrongJockey},
  ${fg.runningStyle},
  ${fg.preLateStart},
  ${fg.preLastPhase},
  ${fg.lateStartPer},
  ${fg.course},

  ${fg.placeCode},

  ${fg.headCount},
  ${fg.preHeadCount},

  ${fg.surfaceChanged},
  ${fg.gradeChanged},
  ${fg.preMargin},
  ${fg.femaleOnly}

)
""".update.apply
  }

  def rr2f() = {
    DB.readOnly { implicit s =>
      val fgs = FeatureGenerator.iterator()
      fgs.grouped(1000).foreach { fgs =>
        //1000回インサートする度にコミットする
        DB.localTx { implicit s =>
          fgs.foreach(FeatureDao.insert)
        }
      }
    }
  }

}

