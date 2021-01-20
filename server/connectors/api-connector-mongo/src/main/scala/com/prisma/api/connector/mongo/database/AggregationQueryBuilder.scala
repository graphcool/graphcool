package com.prisma.api.connector.mongo.database

import com.prisma.api.connector._
import com.prisma.api.connector.mongo.extensions.FieldCombinators._
import com.prisma.api.connector.mongo.extensions.GCBisonTransformer.GCToBson
import com.prisma.api.connector.mongo.extensions.HackforTrue.hackForTrue
import com.prisma.api.connector.mongo.extensions.MongoResultReader
import com.prisma.api.helpers.LimitClauseHelper
import com.prisma.gc_values.{GCValue, IdGCValue, NullGCValue}
import com.prisma.shared.models.{Model, RelationField, ScalarField}
import org.mongodb.scala.MongoDatabase
import org.mongodb.scala.bson.conversions
import org.mongodb.scala.model.Filters._

import scala.concurrent.Future
trait AggregationQueryBuilder extends FilterConditionBuilder with ProjectionBuilder with MongoResultReader {
  import org.mongodb.scala.bson.collection.immutable.Document
  import org.mongodb.scala.bson.conversions.Bson
  import org.mongodb.scala.model.Aggregates._

  import scala.concurrent.ExecutionContext.Implicits.global

  def aggregationQuery(database: MongoDatabase,
                       model: Model,
                       queryArguments: QueryArguments,
                       selectedFields: SelectedFields,
                       rowValueOpt: Option[GCValue]): Future[Seq[Document]] = {
    aggregationQueryForId(database, model, queryArguments, rowValueOpt).flatMap { ids =>
      val bsonIds = ids.distinct.map(GCToBson(_))
      database.getCollection(model.dbName).find(in("_id", bsonIds: _*)).projection(projectSelected(selectedFields)).toFuture.map { seq =>
        bsonIds.map(id => seq.find(doc => doc.get("_id").get == id).get) //sort according to ids ordering
      }
    }
  }

  def aggregationQueryForId(database: MongoDatabase,
                            model: Model,
                            queryArguments: QueryArguments,
                            rowValueOpt: Option[GCValue] = None): Future[Seq[IdGCValue]] = {

    //--------------------------- Assemble Pipeline -----------------------------------------------------

    //-------------------------------- QueryArg Filter --------------------------------------------------
    val joinAndFilter = buildJoinStagesForFilter(queryArguments.filter)

    //-------------------------------- Order ------------------------------------------------------------
    val sort = Seq(OrderByClauseBuilder.sortStage(queryArguments))

    //-------------------------------- Skip & Limit -----------------------------------------------------
    val skipAndLimit = LimitClauseHelper.skipAndLimitValues(queryArguments)
    val skipStage    = Seq(skip(skipAndLimit.skip))
    val limitStage   = skipAndLimit.limit.map(limit)

    //-------------------------------- Project Result ---------------------------------------------------
    val projectStage = Seq(idProjectionStage)

    //--------------------------- Setup Query -----------------------------------------------------------
    val common = joinAndFilter ++ sort ++ skipStage ++ limitStage ++ projectStage

    //-------------------------------- Match on Cursor Condition ----------------------------------------
    val pipeline = rowValueOpt match {
      case None           => common
      case Some(rowValue) => `match`(CursorConditionBuilder.buildCursorCondition(model, queryArguments, rowValue)) +: common
    }

    database.getCollection(model.dbName).aggregate(pipeline).toFuture.map(_.map(readsId))
  }

  //-------------------------------------- Join And Filter ---------------------------------------------------
  //this splits up the matches and pulls them before the $lookup

  def buildJoinStagesForFilter(filter: Option[Filter]): Seq[conversions.Bson] = filter match {
    case Some(filter) => buildJoinStagesForFilter(Path.empty, filter)
    case None         => Seq.empty
  }

  private def buildJoinStagesForFilter(path: Path, filter: Filter): Seq[conversions.Bson] = {
    filter match {
      //-------------------------------RECURSION------------------------------------
      case NodeSubscriptionFilter => Seq(`match`(hackForTrue))
      case AndFilter(filters)     => sortFilters(filters).flatMap(f => buildJoinStagesForFilter(path, f))
      // The Mongo connector does not allow the usage of OR at the moment. Since we want to make use of Or for our filter
      // optimizations this handles the case where we internally transformed a filter to use OR.
      // We know that this will only occur to rewrite join relation filters at the moment
      // This will create a pipeline stage that combines scalarfilters into one stage.
      case OrFilter(filters) if filters.forall(f => f.isInstanceOf[ScalarFilter]) =>
        Seq(`match`(or(filters.map(f => buildConditionForScalarFilter(path.combinedNames, Some(f))): _*)))
      case NotFilter(_)      => sys.error("This is not implemented for the Mongo connector")
      case x: RelationFilter => relationFilterJoinStage(path, x)

      //--------------------------------ANCHORS------------------------------------
      case TrueFilter                                            => Seq(`match`(hackForTrue))
      case FalseFilter                                           => Seq(`match`(not(hackForTrue)))
      case ScalarFilter(scalarField, Contains(v))                => Seq(`match`(regex(nameHelper(path, scalarField), v.value.toString)))
      case ScalarFilter(scalarField, NotContains(v))             => Seq(`match`(not(regex(nameHelper(path, scalarField), v.value.toString))))
      case ScalarFilter(scalarField, StartsWith(v))              => Seq(`match`(regex(nameHelper(path, scalarField), "^" + v.value)))
      case ScalarFilter(scalarField, NotStartsWith(v))           => Seq(`match`(not(regex(nameHelper(path, scalarField), "^" + v.value))))
      case ScalarFilter(scalarField, EndsWith(value))            => Seq(`match`(regex(nameHelper(path, scalarField), value.value + "$")))
      case ScalarFilter(scalarField, NotEndsWith(v))             => Seq(`match`(not(regex(nameHelper(path, scalarField), v.value + "$"))))
      case ScalarFilter(scalarField, LessThan(v))                => Seq(`match`(lt(nameHelper(path, scalarField), GCToBson(v))))
      case ScalarFilter(scalarField, GreaterThan(v))             => Seq(`match`(gt(nameHelper(path, scalarField), GCToBson(v))))
      case ScalarFilter(scalarField, LessThanOrEquals(v))        => Seq(`match`(lte(nameHelper(path, scalarField), GCToBson(v))))
      case ScalarFilter(scalarField, GreaterThanOrEquals(v))     => Seq(`match`(gte(nameHelper(path, scalarField), GCToBson(v))))
      case ScalarFilter(scalarField, NotEquals(NullGCValue))     => Seq(`match`(notEqual(nameHelper(path, scalarField), null)))
      case ScalarFilter(scalarField, NotEquals(v))               => Seq(`match`(notEqual(nameHelper(path, scalarField), GCToBson(v))))
      case ScalarFilter(scalarField, Equals(NullGCValue))        => Seq(`match`(equal(nameHelper(path, scalarField), null)))
      case ScalarFilter(scalarField, Equals(v))                  => Seq(`match`(equal(nameHelper(path, scalarField), GCToBson(v))))
      case ScalarFilter(scalarField, In(Vector(NullGCValue)))    => Seq(`match`(in(nameHelper(path, scalarField), null)))
      case ScalarFilter(scalarField, NotIn(Vector(NullGCValue))) => Seq(`match`(not(in(nameHelper(path, scalarField), null))))
      case ScalarFilter(scalarField, In(values))                 => Seq(`match`(in(nameHelper(path, scalarField), values.map(GCToBson(_)): _*)))
      case ScalarFilter(scalarField, NotIn(values))              => Seq(`match`(not(in(nameHelper(path, scalarField), values.map(GCToBson(_)): _*))))
      case OneRelationIsNullFilter(field)                        => oneRelationNull(field, path)
      //Fixme test this thoroughly
      case ScalarListFilter(scalarListField, ListContains(value))       => Seq(`match`(all(nameHelper(path, scalarListField), GCToBson(value))))
      case ScalarListFilter(scalarListField, ListContainsEvery(values)) => Seq(`match`(all(nameHelper(path, scalarListField), values.map(GCToBson(_)): _*)))
      case ScalarListFilter(scalarListField, ListContainsSome(values)) =>
        Seq(`match`(or(values.map(value => all(nameHelper(path, scalarListField), GCToBson(value))): _*)))
      case x => sys.error(s"Not supported: $x")
    }
  }

  private def oneRelationNull(relationField: RelationField, path: Path): Seq[Bson] = {
    relationField.relatedModel_!.isEmbedded match {
      case true =>
        Seq(`match`(equal(dotPath(path.combinedNames, relationField), null)))

      case false =>
        relationField.relationIsInlinedInParent match {
          case true =>
            Seq(`match`(equal(combineTwo(path.combinedNames, relationField.relation.inlineManifestation.get.referencingColumn), null)))
          case false =>
            val mongoJoin = lookup(
              localField = dotPath(path.combinedNames, relationField.model.idField_!),
              from = relationField.relatedModel_!.dbName,
              foreignField = relationField.relatedField.dbName,
              as = dotPath(path.combinedNames, relationField)
            )
            val mongoMatch = `match`(size(dotPath(path.combinedNames, relationField), 0))
            Seq(mongoJoin, mongoMatch)
        }
    }
  }

  private def relationFilterJoinStage(path: Path, relationFilter: RelationFilter): Seq[conversions.Bson] = {

    val rf          = relationFilter.field
    val updatedPath = path.append(rf)

    val next = buildJoinStagesForFilter(updatedPath, relationFilter.nestedFilter)

    val current = rf.relatedModel_!.isEmbedded match {
      case true => //Fixme this currently always translates to a some we might want to allow _none, _every at some point
        Seq.empty

      case false =>
        val mongoLookup = rf.relationIsInlinedInParent match {
          case true =>
            lookup(
              localField = dotPath(path.combinedNames, rf),
              from = rf.relatedModel_!.dbName,
              foreignField = renameId(rf.relatedModel_!.idField_!),
              as = updatedPath.combinedNames
            )

          case false =>
            lookup(
              localField = dotPath(path.combinedNames, rf.model.idField_!),
              from = rf.relatedModel_!.dbName,
              foreignField = rf.relatedField.dbName,
              as = updatedPath.combinedNames
            )
        }

        val mongoUnwind = unwind(s"$$${updatedPath.combinedNames}")

        Seq(mongoLookup, mongoUnwind)
    }

    current ++ next
  }

  //-------------------------------------------------- Helpers ------------------------------------------------------
  private def nameHelper(path: Path, scalarField: ScalarField): String = dotPath(path.combinedNames, scalarField)

  //-------------------------------Determine if Aggregation is needed -----------------------------------
  def needsAggregation(filter: Option[Filter]): Boolean = filter match {
    case Some(filter) => needsAggregation(filter)
    case None         => false
  }

  private def needsAggregation(filter: Filter): Boolean = {
    filter match {
      //-------------------------------RECURSION------------------------------------
      case NodeSubscriptionFilter => false
      case AndFilter(filters)     => filters.exists(f => needsAggregation(f))
      case OrFilter(filters)      => filters.exists(f => needsAggregation(f))
      case NotFilter(filters)     => filters.exists(f => needsAggregation(f))
      case x: RelationFilter      => relationNeedsFilter(x)

      //--------------------------------ANCHORS------------------------------------
      case TrueFilter                 => false
      case FalseFilter                => false
      case ScalarFilter(_, _)         => false
      case ScalarListFilter(_, _)     => false
      case OneRelationIsNullFilter(_) => true
      case x                          => sys.error(s"Not supported: $x")
    }
  }

  private def relationNeedsFilter(relationFilter: RelationFilter): Boolean = {
    val next    = needsAggregation(relationFilter.nestedFilter)
    val current = !relationFilter.field.relatedModel_!.isEmbedded

    current || next
  }

  //------------------------------Sort Filters - Join Relations Last --------------------------------
  private def sortFilters(filters: Seq[Filter]): Seq[Filter] = {
    val withRelationFilter    = filters.collect { case x if needsAggregation(x)  => x }
    val withoutRelationFilter = filters.collect { case x if !needsAggregation(x) => x }

    withoutRelationFilter ++ withRelationFilter
  }
}
