package util

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.Suite

case class Project(
    id: String,
    dataModel: String,
) {
  val dataSourceConfig: String = {
    val config = ConnectorConfig.instance
    s"""
           |datasource test {
           |  provider = "${config.provider}"
           |  url = "${config.url}"
           |}
    """.stripMargin
      .replaceAllLiterally("$DB_FILE", s"${EnvVars.serverRoot}/db/$id.db")
      .replaceAllLiterally("$DB", id)
  }

  val dataModelWithDataSourceConfig = {
    dataSourceConfig + "\n" + dataModel
  }
}

trait Dsl {
  val testProjectId = "default@default"

  def fromStringWithId(id: String)(sdlString: String): Project = {
    Project(id = id, dataModel = sdlString)
  }

  def fromString(sdlString: String)(implicit suite: Suite): Project = {
    Project(id = projectId(suite), dataModel = sdlString.stripMargin)
  }

  // this exists only for backwards compatibility to ease test conversion
  def fromStringV11()(sdlString: String)(implicit suite: Suite): Project = {
    fromString(sdlString)
  }

  private def projectId(suite: Suite): String = {
    // GetFieldFromSQLUniqueException blows up if we generate longer names, since we then exceed the postgres limits for constraint names
    // todo: actually fix GetFieldFromSQLUniqueException instead
    val nameThatMightBeTooLong = suite.getClass.getSimpleName
    val validName              = nameThatMightBeTooLong.substring(0, Math.min(30, nameThatMightBeTooLong.length))
    val wasShortened           = validName.length != nameThatMightBeTooLong.length
    if (wasShortened) {
      validName + disambiguationCounter.incrementAndGet().toString
    } else {
      validName
    }
  }

  private val disambiguationCounter = new AtomicInteger(0)
}

object ProjectDsl extends Dsl
object SchemaDsl  extends Dsl // this exists only for backwards compatibility to ease test conversion
