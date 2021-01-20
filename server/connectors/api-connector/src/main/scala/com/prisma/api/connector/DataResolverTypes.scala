package com.prisma.api.connector

import com.prisma.gc_values.{GCValue, IdGCValue}
import com.prisma.shared.models._

case class ScalarListElement(nodeId: IdGCValue, position: Int, value: GCValue)

case class ResolverResult[T](
    nodes: Vector[T],
    hasNextPage: Boolean,
    hasPreviousPage: Boolean,
    parentModelId: Option[IdGCValue]
)

object ResolverResult {
  def apply[T](nodes: Vector[T]): ResolverResult[T] = {
    ResolverResult(nodes, hasNextPage = false, hasPreviousPage = false, parentModelId = None)
  }

  // If order is inverted we have to reverse the returned data items. We do this in-mem to keep the sql query simple.
  // Also, remove excess items from limit + 1 queries and set page info (hasNext, hasPrevious).
  def apply[T](queryArguments: QueryArguments, vector: Vector[T], parentModelId: Option[IdGCValue] = None): ResolverResult[T] = {
    val isReverseOrder = queryArguments.last.isDefined
    val items = isReverseOrder match {
      case true  => vector.reverse
      case false => vector
    }

    (queryArguments.first, queryArguments.last) match {
      case (Some(f), _) if items.size > f => ResolverResult(items.dropRight(1), hasPreviousPage = false, hasNextPage = true, parentModelId = parentModelId)
      case (_, Some(l)) if items.size > l => ResolverResult(items.tail, hasPreviousPage = true, hasNextPage = false, parentModelId = parentModelId)
      case _                              => ResolverResult(items, hasPreviousPage = false, hasNextPage = false, parentModelId = parentModelId)
    }
  }
}

case class QueryArguments(
    skip: Option[Int],
    after: Option[IdGCValue],
    first: Option[Int],
    before: Option[IdGCValue],
    last: Option[Int],
    filter: Option[Filter],
    orderBy: Option[OrderBy]
) {
  val isWithPagination = last.orElse(first).orElse(skip).isDefined
  val isEmpty          = skip.isEmpty && after.isEmpty && first.isEmpty && before.isEmpty && last.isEmpty && filter.isEmpty && orderBy.isEmpty
}

object QueryArguments {
  def empty                                              = QueryArguments(skip = None, after = None, first = None, before = None, last = None, filter = None, orderBy = None)
  def withFilter(filter: Filter): QueryArguments         = QueryArguments.empty.copy(filter = Some(filter))
  def withFilter(filter: Option[Filter]): QueryArguments = QueryArguments.empty.copy(filter = filter)
}

object SelectedFields {
  val empty                                                             = SelectedFields(Set.empty)
  def forRelationField(rf: RelationField)                               = SelectedFields(Set(SelectedRelationField.empty(rf)))
  def byFieldAndNodeAddress(field: RelationField, address: NodeAddress) = address.path.selectedFields(field)
  def allScalarAndFlatRelationFields(model: Model) =
    SelectedFields((model.scalarFields.map(SelectedScalarField) ++ model.relationFields.map(SelectedRelationField.empty)).toSet)
  def allScalarFields(model: Model) = SelectedFields(model.scalarFields.map(SelectedScalarField).toSet)
}

sealed trait SelectedField
case class SelectedScalarField(field: ScalarField)                                     extends SelectedField
case class SelectedRelationField(field: RelationField, selectedFields: SelectedFields) extends SelectedField

object SelectedRelationField {
  def empty(rf: RelationField) = SelectedRelationField(rf, SelectedFields.empty)
}

case class SelectedFields(fields: Set[SelectedField]) {
  val scalarFields: List[ScalarField] = fields.collect { case selected: SelectedScalarField                             => selected.field }.toList
  val scalarListFields                = fields.collect { case selected: SelectedScalarField if selected.field.isList    => selected.field }.toList
  val scalarNonListFields             = fields.collect { case selected: SelectedScalarField if !selected.field.isList   => selected.field }.toList
  val relationFields                  = fields.collect { case selected: SelectedRelationField                           => selected.field }.toList
  val relationListFields              = fields.collect { case selected: SelectedRelationField if selected.field.isList  => selected.field }.toList
  val relationNonListFields           = fields.collect { case selected: SelectedRelationField if !selected.field.isList => selected.field }.toList
  private val inlineRelationFields = relationFields.collect {
    case rf if !rf.isHidden && rf.relation.isInlineRelation && rf.relation.isSelfRelation && rf.relationSide == RelationSide.B                        => rf
    case rf if rf.relatedField.isHidden && rf.relation.isInlineRelation && rf.relation.isSelfRelation && rf.relationSide == RelationSide.A            => rf
    case rf if rf.relation.isInlineRelation && !rf.relation.isSelfRelation && rf.relation.inlineManifestation.get.inTableOfModelName == rf.model.name => rf
  }

  val scalarDbFields           = scalarNonListFields ++ inlineRelationFields.map(_.scalarCopy)
  val scalarSelectedFields     = fields.collect { case selected: SelectedScalarField => selected }
  val relationalSelectedFields = fields.collect { case selected: SelectedRelationField => selected }

  def ++(other: SelectedFields) = SelectedFields(fields ++ other.fields)

  def includeOrderBy(queryArguments: QueryArguments): SelectedFields = queryArguments.orderBy match {
    case None          => this
    case Some(orderBy) => this ++ SelectedFields(Set(SelectedScalarField(orderBy.field)))
  }

}

object SortOrder extends Enumeration {
  type SortOrder = Value
  val Asc: SortOrder.Value  = Value("asc")
  val Desc: SortOrder.Value = Value("desc")
}

case class OrderBy(
    field: ScalarField,
    sortOrder: SortOrder.Value
)

object LogicalKeyWords {
  val logicCombinators           = List("AND", "OR", "NOT")
  def isLogicFilter(key: String) = logicCombinators.contains(key)
}

object Filter {
  val empty = TrueFilter
}
sealed trait Filter

case class AndFilter(filters: Vector[Filter]) extends Filter
case class OrFilter(filters: Vector[Filter])  extends Filter
case class NotFilter(filters: Vector[Filter]) extends Filter

case class ScalarFilter(field: ScalarField, condition: ScalarCondition) extends Filter

sealed trait ScalarCondition {
  def sameAs(other: ScalarCondition) = this.getClass == other.getClass
}

case class Equals(value: GCValue)              extends ScalarCondition
case class NotEquals(value: GCValue)           extends ScalarCondition
case class Contains(value: GCValue)            extends ScalarCondition
case class NotContains(value: GCValue)         extends ScalarCondition
case class StartsWith(value: GCValue)          extends ScalarCondition
case class NotStartsWith(value: GCValue)       extends ScalarCondition
case class EndsWith(value: GCValue)            extends ScalarCondition
case class NotEndsWith(value: GCValue)         extends ScalarCondition
case class LessThan(value: GCValue)            extends ScalarCondition
case class LessThanOrEquals(value: GCValue)    extends ScalarCondition
case class GreaterThan(value: GCValue)         extends ScalarCondition
case class GreaterThanOrEquals(value: GCValue) extends ScalarCondition
case class In(values: Vector[GCValue])         extends ScalarCondition
case class NotIn(values: Vector[GCValue])      extends ScalarCondition

case class ScalarListFilter(field: ScalarField, condition: ScalarListCondition) extends Filter

sealed trait ScalarListCondition
case class ListContains(value: GCValue)              extends ScalarListCondition
case class ListContainsEvery(value: Vector[GCValue]) extends ScalarListCondition
case class ListContainsSome(value: Vector[GCValue])  extends ScalarListCondition

case class OneRelationIsNullFilter(field: RelationField) extends Filter

case class RelationFilter(field: RelationField, nestedFilter: Filter, condition: RelationCondition) extends Filter

sealed trait RelationCondition
object EveryRelatedNode      extends RelationCondition
object AtLeastOneRelatedNode extends RelationCondition
object NoRelatedNode         extends RelationCondition
object ToOneRelatedNode      extends RelationCondition

object NodeSubscriptionFilter extends Filter
object TrueFilter             extends Filter
object FalseFilter            extends Filter
