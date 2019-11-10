package netkeibascraper
import scalikejdbc._

case class Payoff(
                   race_id: Int,
                   ticket_type: Int,
                   horse_number: String,
                   payoff: Double,
                   popularity: Int
                 )

object PayoffDao {

  def createTable()(implicit s: DBSession) = {
    sql"""
create table if not exists payoff (
  race_id      integer not null,
  ticket_type  integer not null check(ticket_type between 0 and 7),
  horse_number text    not null,
  payoff       real    not null check(payoff >= 0),
  popularity   integer not null check(popularity >= 0),
  primary key (race_id, ticket_type, horse_number),
  foreign key (race_id) references race_info (id)
)
""".execute.apply
  }

  def insert(dto: Payoff)(implicit s: DBSession) = {
    sql"""
insert or replace into payoff (
  race_id,
  ticket_type,
  horse_number,
  payoff,
  popularity
) values (
  ${dto.race_id},
  ${dto.ticket_type},
  ${dto.horse_number},
  ${dto.payoff},
  ${dto.popularity}
)
""".update.apply()
  }

}


