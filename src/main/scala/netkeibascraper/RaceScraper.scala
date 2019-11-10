package netkeibascraper

import java.io.File

import org.apache.commons.io.FileUtils
import org.openqa.selenium.By
import org.openqa.selenium.htmlunit.HtmlUnitDriver

object RaceScraper {

  val mail: String = if (System.getenv("SCRAPER_LOGIN_MAIL") != null) {
    System.getenv("SCRAPER_LOGIN_MAIL")
  } else {
    sys.error("please set SCRAPER_LOGIN_MAIL")
  }

  val password: String = if (System.getenv("SCRAPER_LOGIN_PASSWORD") != null) {
    System.getenv("SCRAPER_LOGIN_PASSWORD")
  } else {
    sys.error("please set SCRAPER_LOGIN_PASSWORD")
  }

  def scrape() = {

    val driver = new HtmlUnitDriver(false)

    //login
    driver.get("https://account.netkeiba.com/?pid=login")
    driver.findElement(By.name("login_id")).sendKeys(mail)
    driver.findElement(By.name("pswd")).sendKeys(password + "\n")

    val re = """/race/(\d+)/""".r

    val nums =
      io.Source.fromFile("output/race_url.txt").getLines.toList.
        map { s => val re(x) = re.findFirstIn(s).get; x }

    val urls = nums.map(s => "https://db.netkeiba.com/race/" + s)

    val folder = new File("output/html")
    if (!folder.exists()) folder.mkdir()

    var i = 0
    val interval = if (System.getenv("SCRAPER_SCRAPE_INTERVAL") != null) {
      System.getenv("SCRAPER_SCRAPE_INTERVAL").toLong
    } else {
      500L
    }

    nums.zip(urls).map { case (num, url) =>
      i += 1
      println("" + i + ":downloading " + url)
      val file = new File(folder, num + ".html")
      if (!file.exists()) {
        driver.get(url)
        //↓ここあんまり短くしないでね！
        Thread.sleep(interval)
        val html = driver.getPageSource()
        FileUtils.writeStringToFile(file, html)
      }
    }
  }

}

