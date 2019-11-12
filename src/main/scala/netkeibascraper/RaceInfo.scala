package netkeibascraper
import scalikejdbc._

case class RaceInfo(
                     race_name: String,
                     surface: String,
                     distance: Int,
                     weather: String,
                     surface_state: String,
                     race_start: String,
                     race_number: Int,
                     surface_score: Option[Int],
                     date: String,
                     place_detail: String,
                     race_class: String
                   ) {

  def placeCode: String = {
    place_detail.replaceAll("\\d+回([^\\d]+)\\d+日目", "$1")
  }
}

object RaceInfoDao {

  def createTable()(implicit s: DBSession) = {
    sql"""
create table if not exists race_info (
  id integer primary key autoincrement,

  race_name     text    not null,
  surface       text    not null,
  distance      integer not null,
  weather       text    not null,
  surface_state text    not null,

  race_start    text    not null,
  race_number   integer not null,

  surface_score integer,
  date          text    not null,
  place_detail  text    not null,
  race_class    text    not null
);""".execute.apply

    sql"""
create index
  date_idx
on
  race_info (date);
""".execute.apply

    sql"""
create index
  id_date_idx
on
  race_info (id, date);
""".execute.apply
  }

  def insert(ri: RaceInfo)(implicit s: DBSession) = {
    sql"""
insert or replace into race_info (
  race_name,
  surface,
  distance,
  weather,
  surface_state,
  race_start,
  race_number,
  surface_score,
  date,
  place_detail,
  race_class
) values (
  ${ri.race_name},
  ${ri.surface},
  ${ri.distance},
  ${ri.weather},
  ${ri.surface_state},
  ${ri.race_start},
  ${ri.race_number},
  ${ri.surface_score},
  ${ri.date},
  ${ri.place_detail},
  ${ri.race_class}
);
""".update.apply()
  }

  def lastRowId()(implicit s: DBSession): Int = {
    sql"""
select last_insert_rowid() as last_rowid
""".map(_.int("last_rowid")).single.apply().get
  }

  def getById(id: Int)(implicit s: DBSession): RaceInfo = {
    sql"""select * from race_info where id = ${id}""".
      map { x =>
        RaceInfo(
          race_name = x.string("race_name"),
          surface = x.string("surface"),
          distance = x.int("distance"),
          weather = x.string("weather"),
          surface_state = x.string("surface_state"),
          race_start = x.string("race_start"),
          race_number = x.int("race_number"),
          surface_score = x.intOpt("surface_score"),
          date = x.string("date"),
          place_detail = x.string("place_detail"),
          race_class = x.string("race_class")
        )
      }.single.apply().get
  }

}



