package com.prisma.api.connector.jdbc.database

import com.prisma.api.connector._
import com.prisma.gc_values.NullGCValue
import com.prisma.shared.models.{RelationField, ScalarField}
import org.jooq._
import org.jooq.impl.DSL._

trait FilterConditionBuilder extends BuilderBase {
  def buildConditionForFilter(filter: Option[Filter]): Condition = filter match {
    case Some(filter) => buildConditionForFilter(filter, topLevelAlias)
    case None         => noCondition()
  }

  private def buildConditionForFilter(filter: Filter, alias: String): Condition = {

    def fieldFrom(scalarField: ScalarField) = field(name(alias, scalarField.dbName))
    def nonEmptyConditions(filters: Vector[Filter]): Vector[Condition] = {
      filters.map(buildConditionForFilter(_, alias)) match {
        case x if x.isEmpty => Vector(noCondition())
        case x              => x
      }
    }

    filter match {
      //-------------------------------RECURSION------------------------------------
      case NodeSubscriptionFilter => and(trueCondition())
      case AndFilter(filters)     => nonEmptyConditions(filters).reduceLeft(_ and _)
      case OrFilter(filters)      => nonEmptyConditions(filters).reduceLeft(_ or _)
      case NotFilter(filters)     => filters.map(buildConditionForFilter(_, alias)).foldLeft(and(trueCondition()))(_ andNot _)
      case relationFilter: RelationFilter =>
        inStatementForRelationCondition(
          jooqField = modelIdColumn(alias, relationFilter.field.model),
          condition = relationFilter.condition,
          subSelect = relationFilterSubSelect(alias, relationFilter)
        )

      //--------------------------------ANCHORS------------------------------------
      case TrueFilter                                            => trueCondition()
      case FalseFilter                                           => falseCondition()
      case ScalarFilter(scalarField, Contains(_))                => fieldFrom(scalarField).contains(placeHolder)
      case ScalarFilter(scalarField, NotContains(_))             => fieldFrom(scalarField).notContains(placeHolder)
      case ScalarFilter(scalarField, StartsWith(_))              => fieldFrom(scalarField).startsWith(placeHolder)
      case ScalarFilter(scalarField, NotStartsWith(_))           => fieldFrom(scalarField).startsWith(placeHolder).not()
      case ScalarFilter(scalarField, EndsWith(_))                => fieldFrom(scalarField).endsWith(placeHolder)
      case ScalarFilter(scalarField, NotEndsWith(_))             => fieldFrom(scalarField).endsWith(placeHolder).not()
      case ScalarFilter(scalarField, LessThan(_))                => fieldFrom(scalarField).lessThan(placeHolder)
      case ScalarFilter(scalarField, GreaterThan(_))             => fieldFrom(scalarField).greaterThan(placeHolder)
      case ScalarFilter(scalarField, LessThanOrEquals(_))        => fieldFrom(scalarField).lessOrEqual(placeHolder)
      case ScalarFilter(scalarField, GreaterThanOrEquals(_))     => fieldFrom(scalarField).greaterOrEqual(placeHolder)
      case ScalarFilter(scalarField, NotEquals(NullGCValue))     => fieldFrom(scalarField).isNotNull
      case ScalarFilter(scalarField, NotEquals(_))               => fieldFrom(scalarField).notEqual(placeHolder)
      case ScalarFilter(scalarField, Equals(NullGCValue))        => fieldFrom(scalarField).isNull
      case ScalarFilter(scalarField, Equals(_))                  => fieldFrom(scalarField).equal(placeHolder)
      case ScalarFilter(scalarField, In(Vector(NullGCValue)))    => fieldFrom(scalarField).isNull
      case ScalarFilter(scalarField, NotIn(Vector(NullGCValue))) => fieldFrom(scalarField).isNotNull
      case ScalarFilter(scalarField, In(values))                 => fieldFrom(scalarField).in(Vector.fill(values.length) { placeHolder }: _*)
      case ScalarFilter(scalarField, NotIn(values))              => fieldFrom(scalarField).notIn(Vector.fill(values.length) { placeHolder }: _*)
      case OneRelationIsNullFilter(field)                        => oneRelationIsNullFilter(field, alias)
      case x                                                     => sys.error(s"Not supported: $x")
    }
  }

  private def relationFilterSubSelect(alias: String, relationFilter: RelationFilter): SelectConditionStep[Record1[AnyRef]] = {
    // this skips intermediate tables when there is no condition on them. so the following will not join with the album table but join the artist-album relation with the album-track relation
    // artists(where:{albums_some:{tracks_some:{condition}}})
    //
    // the following query contains an implicit andFilter around the two nested ones and will not be improved at the moment
    // albums(where: {Tracks_some:{ MediaType:{Name_starts_with:""}, Genre:{Name_starts_with:""}}})
    // the same is true for explicit AND, OR, NOT with more than one nested relationfilter. they do not profit from skipping intermediate tables at the moment
    // these cases could be improved as well at the price of higher code complexity

    val relationField              = relationFilter.field
    val relation                   = relationField.relation
    val newAlias                   = relationField.relatedModel_!.dbName + "_" + alias
    val invertConditionOfSubSelect = relationFilter.condition == EveryRelatedNode

    relationFilter.nestedFilter match {
      case nested: RelationFilter =>
        val condition = inStatementForRelationCondition(
          jooqField = relationColumn(relationField.relatedField),
          condition = nested.condition,
          subSelect = relationFilterSubSelect(newAlias, nested)
        )
        sql
          .select(relationColumn(relationField))
          .from(relationTable(relation))
          .where(condition.invert(invertConditionOfSubSelect))

      case nested =>
        val condition = buildConditionForFilter(nested, newAlias)
        sql
          .select(relationColumn(relationField))
          .from(relationTable(relation))
          .innerJoin(modelTable(relationField.relatedModel_!).as(newAlias))
          .on(modelIdColumn(newAlias, relationField.relatedModel_!).eq(relationColumn(relationField.relatedField)))
          .where(condition.invert(invertConditionOfSubSelect))
    }
  }

  private def inStatementForRelationCondition(jooqField: Field[AnyRef], condition: RelationCondition, subSelect: SelectConditionStep[_]) = {
    condition match {
      case EveryRelatedNode      => jooqField.notIn(subSelect)
      case NoRelatedNode         => jooqField.notIn(subSelect)
      case AtLeastOneRelatedNode => jooqField.in(subSelect)
      case ToOneRelatedNode      => jooqField.in(subSelect)
    }
  }

  private def oneRelationIsNullFilter(relationField: RelationField, alias: String): Condition = {
    val relation = relationField.relation

    relationField.relationIsInlinedInParent match {
      case true =>
        field(name(alias, relationField.dbName)).isNull

      case false =>
        val select = sql
          .select(relationColumn(relationField))
          .from(relationTable(relation))
          .where(relationColumn(relationField).isNotNull)

        modelIdColumn(alias, relationField.relatedModel_!).notIn(select)
    }
  }
}
