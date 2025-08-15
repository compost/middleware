package com.jada

import javax.inject.Inject
import scala.util.Using
import javax.sql.DataSource
import javax.enterprise.context.ApplicationScoped
import scala.util.Success
import scala.util.Failure
import org.eclipse.microprofile.config.inject.ConfigProperty
import java.util.Locale



import io.quarkus.runtime.QuarkusApplication
import io.quarkus.runtime.annotations.QuarkusMain
import io.quarkus.runtime.Quarkus

@QuarkusMain
object Referentials extends App {
  Quarkus.run(classOf[MyApp])
}

class MyApp(@Inject val referentials: RepositoryReferentials) extends QuarkusApplication {
  override def run(args: String*): Int = {
    println(referentials.currencies(List(136)))
    println(referentials.countries(List(136)))
    0
  }
}

object Countries {
  val countries = Locale
    .getISOCountries()
    .map(code => new Locale("", code))
    .map(l =>
      List(
        l.getCountry -> l.getDisplayCountry(),
        l.getISO3Country() -> l.getDisplayCountry()
      )
    )
    .flatten
    .toMap
  def apply(code: Option[String]): Option[String] = {
    code.map(c => countries.get(c)).flatten.orElse(code)
  }
}

@ApplicationScoped
class Referentials(
    @Inject rr: RepositoryReferentials,
    @ConfigProperty(name = "brand-ids") val brandIds: java.util.List[String]
) {
  import scala.jdk.CollectionConverters._
  val currencies = rr.currencies(brandIds.asScala.map(_.toInt).toList)
  val countries = rr.countries(brandIds.asScala.map(_.toInt).toList)
}

@ApplicationScoped
class RepositoryReferentials(
    @Inject val dataSource: DataSource
) {

  def currencies(brandIds: List[Int]): Map[String, Map[String, String]] = {
    referentials(
      "SELECT brand_id, currency_id, NVL(currency_description,'') FROM dbo.dim_currency WHERE brand_id in (#BRAND_IDS#)",
      brandIds
    )
  }

  def countries(brandIds: List[Int]): Map[String, Map[String, String]] = {
    referentials(
      "SELECT brand_id, country_id, country_description FROM dbo.dim_countries WHERE brand_id in (#BRAND_IDS#)",
      brandIds
    )
  }

  private def referentials(
      sqlReferentials: String,
      brandIds: List[Int]
  ): Map[String, Map[String, String]] = {
    Using.Manager { use =>
      val connection = use(dataSource.getConnection())

      // connection.createArrayOf is not supported
      val sql = use(
        connection.prepareStatement(
          sqlReferentials.replace("#BRAND_IDS#", brandIds.mkString(","))
        )
      )
      val query = use(sql.executeQuery())

      val results = scala.collection.mutable
        .Map[String, scala.collection.mutable.Map[String, String]]()
      while (query.next()) {
        val brandId = query.getString(1)
        val referentials = results.getOrElseUpdate(
          brandId,
          scala.collection.mutable.Map[String, String]()
        )
        referentials.put(query.getString(2), query.getString(3))
      }
      results.map(keyValue => keyValue._1 -> keyValue._2.toMap).toMap
    } match {
      case Success(value) => value
      // Youpi
      case Failure(e) => throw e
    }
  }
}
