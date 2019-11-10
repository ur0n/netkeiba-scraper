package netkeibascraper
import scalikejdbc._

case class RaceResult(
                       race_id: Int,
                       order_of_finish: String,
                       frame_number: Int,
                       horse_number: Int,
                       horse_id: String,
                       sex: String,
                       age: Int,
                       basis_weight: Double,
                       jockey_id: String,
                       finishing_time: String,
                       length: String,
                       speed_figure: Option[Int],
                       pass: String,
                       last_phase: Option[Double],
                       odds: Option[Double],
                       popularity: Option[Int],
                       horse_weight: String,
                       remark: Option[String],
                       stable: String,
                       trainer_id: String,
                       owner_id: String,
                       earning_money: Option[Double]
                     )

object RaceResultDao {

  def createTable()(implicit s: DBSession) = {
    sql"""
create table if not exists race_result (
  race_id integer not null,

  order_of_finish text    not null,
  frame_number       integer not null,
  horse_number       integer not null,
  horse_id           text    not null,
  sex                text    not null,
  age                integer not null,
  basis_weight       real    not null,
  jockey_id          text    not null,
  finishing_time     text    not null,
  length             text    not null,
  speed_figure       integer,
  pass               text    not null,
  last_phase         real,
  odds               real,
  popularity         integer,
  horse_weight       text    not null,
  remark             text,
  stable             text    not null,
  trainer_id         text    not null,
  owner_id           text    not null,
  earning_money      real,
  primary key (race_id, horse_number),
  foreign key (race_id) references race_info (id)
);""".execute.apply

    sql"""
create index
  race_id_idx
on
  race_result (race_id);
""".execute.apply

    sql"""
create index
  race_id_horse_id_idx
on
  race_result (race_id, horse_id);
""".execute.apply

    sql"""
create index
  race_id_jockey_id_idx
on
  race_result (race_id, jockey_id);
""".execute.apply

    sql"""
create index
  race_id_trainer_id_idx
on
  race_result (race_id, trainer_id);
""".execute.apply

    sql"""
create index
  race_id_owner_id_idx
on
  race_result (race_id, owner_id);
""".execute.apply
  }

  def insert(rr: RaceResult)(implicit s: DBSession) = {
    sql"""
insert or replace into race_result (
  race_id,

  order_of_finish,
  frame_number,
  horse_number,
  horse_id,
  sex,
  age,
  basis_weight,
  jockey_id,
  finishing_time,
  length,
  speed_figure,
  pass,
  last_phase,
  odds,
  popularity,
  horse_weight,
  remark,
  stable,
  trainer_id,
  owner_id,
  earning_money
) values (
  ${rr.race_id},

  ${rr.order_of_finish},
  ${rr.frame_number},
  ${rr.horse_number},
  ${rr.horse_id},
  ${rr.sex},
  ${rr.age},
  ${rr.basis_weight},
  ${rr.jockey_id},
  ${rr.finishing_time},
  ${rr.length},
  ${rr.speed_figure},
  ${rr.pass},
  ${rr.last_phase},
  ${rr.odds},
  ${rr.popularity},
  ${rr.horse_weight},
  ${rr.remark},
  ${rr.stable},
  ${rr.trainer_id},
  ${rr.owner_id},
  ${rr.earning_money}
)
""".update.apply()
  }

}



