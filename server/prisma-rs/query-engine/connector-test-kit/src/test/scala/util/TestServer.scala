package util

import java.io.{BufferedReader, InputStreamReader}
import java.net.{HttpURLConnection, URL}
import java.nio.charset.StandardCharsets
import java.util.Base64
import java.util.concurrent.atomic.AtomicInteger

import play.api.libs.json._

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Awaitable, Future}
import scala.util.Try

case class QueryEngineResponse(status: Int, body: String) {
  lazy val jsonBody: Try[JsValue] = Try(Json.parse(body))
}

case class TestServer() extends PlayJsonExtensions {
  import scala.concurrent.ExecutionContext.Implicits.global
  println(s"TestServers.nextPort: ${TestServers.nextPort.get()}")

  def query(
      query: String,
      project: Project,
      dataContains: String = ""
  ): JsValue = {
    awaitInfinitely { queryAsync(query, project, dataContains) }
  }

  def queryAsync(query: String, project: Project, dataContains: String = ""): Future[JsValue] = {
    val result = querySchemaAsync(
      query = query.stripMargin,
      project = project,
    )

    result.map { r =>
      r.assertSuccessfulResponse(dataContains)
      r
    }
  }

  def queryThatMustFail(
      query: String,
      project: Project,
      errorCode: Int,
      errorCount: Int = 1,
      errorContains: String = ""
  ): JsValue = {
    val result = awaitInfinitely {
      querySchemaAsync(
        query = query,
        project = project,
      )
    }

    // TODO: bring those error checks back
    // Ignore error codes for external tests (0) and containment checks ("")
    result.assertFailingResponse(0, errorCount, "")
    result
  }

  private def querySchemaAsync(
      query: String,
      project: Project
  ): Future[JsValue] = {
    val port = TestServers.getQueryEngine(project)
//    println(s"query engine started on port $port")
    println(s"Query: $query")
    Future {
      queryPrismaProcess(query, port)
    }.map(r => r.jsonBody.get)
      .transform { r =>
        println(s"Query result: $r")
//        queryEngineProcess.destroyForcibly().waitFor()
        r
      }
  }

//  val nextPort = new AtomicInteger(4000)
//
//  private def startQueryEngine(project: Project): (Int, java.lang.Process) = {
//    import java.lang.ProcessBuilder.Redirect
//
//    // TODO: discuss with Dom whether we want to keep the legacy mode
//    val pb         = new java.lang.ProcessBuilder(EnvVars.prismaBinaryPath, "--legacy")
//    val workingDir = new java.io.File(".")
//
//    val fullDataModel = project.dataModelWithDataSourceConfig
//    // Important: Rust requires UTF-8 encoding (encodeToString uses Latin-1)
//    val encoded = Base64.getEncoder.encode(fullDataModel.getBytes(StandardCharsets.UTF_8))
//    val envVar  = new String(encoded, StandardCharsets.UTF_8)
//    val port    = nextPort.incrementAndGet()
//
//    pb.environment.put("PRISMA_DML", envVar)
//    pb.environment.put("PORT", port.toString)
//
//    pb.directory(workingDir)
//    pb.redirectErrorStream(true)
//    pb.redirectOutput(Redirect.INHERIT)
//
//    val process = pb.start
//    Thread.sleep(100) // Offsets process startup latency
//    (port, process)
//  }

  private def queryPrismaProcess(query: String, port: Int): QueryEngineResponse = {
    val url = new URL(s"http://127.0.0.1:$port")
    val con = url.openConnection().asInstanceOf[HttpURLConnection]

    con.setDoOutput(true)
    con.setRequestMethod("POST")
    con.setRequestProperty("Content-Type", "application/json")

    val body = Json.obj("query" -> query, "variables" -> Json.obj()).toString()
    con.setRequestProperty("Content-Length", Integer.toString(body.length))
    con.getOutputStream.write(body.getBytes(StandardCharsets.UTF_8))

    try {
      val status = con.getResponseCode
      val streamReader = if (status > 299) {
        new InputStreamReader(con.getErrorStream, "utf8")
      } else {
        new InputStreamReader(con.getInputStream, "utf8")
      }

      val in     = new BufferedReader(streamReader)
      val buffer = new StringBuffer

      Stream.continually(in.readLine()).takeWhile(_ != null).foreach(buffer.append)
      QueryEngineResponse(status, buffer.toString)
    } catch {
      case e: Throwable => QueryEngineResponse(999, s"""{"errors": [{"message": "Connection error: $e"}]}""")
    } finally {
      con.disconnect()
    }
  }

  private def awaitInfinitely[T](awaitable: Awaitable[T]): T = Await.result(awaitable, Duration.Inf)
}

object TestServers {
  val cache    = Cache.unbounded[Project, (Int, java.lang.Process)]()
  val nextPort = new AtomicInteger(4000)

  def getQueryEngine(project: Project): Int = {
    val (port, _) = cache.getOrUpdate(project, () => {
      startQueryEngine(project)
    })
    port
  }

  private def startQueryEngine(project: Project): (Int, java.lang.Process) = {
    import java.lang.ProcessBuilder.Redirect

    // TODO: discuss with Dom whether we want to keep the legacy mode
    val pb         = new java.lang.ProcessBuilder(EnvVars.prismaBinaryPath, "--legacy")
    val workingDir = new java.io.File(".")

    val fullDataModel = project.dataModelWithDataSourceConfig
    // Important: Rust requires UTF-8 encoding (encodeToString uses Latin-1)
    val encoded = Base64.getEncoder.encode(fullDataModel.getBytes(StandardCharsets.UTF_8))
    val envVar  = new String(encoded, StandardCharsets.UTF_8)
    val port    = nextPort.incrementAndGet()

    pb.environment.put("PRISMA_DML", envVar)
    pb.environment.put("PORT", port.toString)

    pb.directory(workingDir)
    pb.redirectErrorStream(true)
    pb.redirectOutput(Redirect.INHERIT)

    val process = pb.start
    Thread.sleep(100) // Offsets process startup latency
    (port, process)
  }
}
