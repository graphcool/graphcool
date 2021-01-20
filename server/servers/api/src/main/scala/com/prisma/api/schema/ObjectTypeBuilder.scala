package com.prisma.api.schema

import com.prisma.api.connector.LogicalKeyWords._
import com.prisma.api.connector.{TrueFilter, _}
import com.prisma.api.mutations.BatchPayload
import com.prisma.api.resolver.DeferredTypes._
import com.prisma.api.resolver.{IdBasedConnection, IdBasedConnectionDefinition}
import com.prisma.api.schema.CustomScalarTypes.{DateTimeType, JsonType, UUIDType}
import com.prisma.gc_values._
import com.prisma.shared.models
import com.prisma.shared.models.ConnectorCapability.EmbeddedScalarListsCapability
import com.prisma.shared.models.{Field => _, _}
import com.prisma.util.coolArgs.GCAnyConverter
import sangria.schema.{Field => SangriaField, _}

import scala.concurrent.ExecutionContext

class ObjectTypeBuilder(
    project: models.Project,
    nodeInterface: Option[InterfaceType[ApiUserContext, PrismaNode]] = None,
    withRelations: Boolean = true,
    onlyId: Boolean = false,
    capabilities: ConnectorCapabilities
)(implicit ec: ExecutionContext)
    extends SangriaExtensions {

  val batchPayloadType: ObjectType[ApiUserContext, BatchPayload] = ObjectType(
    name = "BatchPayload",
    description = "",
    fieldsFn = () => {
      List(
        SangriaField(
          "count",
          fieldType = LongType,
          description = Some("The number of nodes that have been affected by the Batch operation."),
          resolve = (ctx: Context[ApiUserContext, BatchPayload]) => { ctx.value.count }
        )
      )
    }
  )

  val modelObjectTypes: Map[String, ObjectType[ApiUserContext, PrismaNode]] = project.models.map(model => (model.name, modelToObjectType(model))).toMap

  val modelConnectionTypes: Map[String, ObjectType[ApiUserContext, IdBasedConnection[PrismaNode]]] =
    project.models.map(model => (model.name, modelToConnectionType(model).connectionType)).toMap

  def modelToConnectionType(model: Model): IdBasedConnectionDefinition[ApiUserContext, IdBasedConnection[PrismaNode], PrismaNode] = {
    IdBasedConnection.definition[ApiUserContext, IdBasedConnection, PrismaNode](
      name = model.name,
      nodeType = modelObjectTypes(model.name),
      connectionFields = {
        List(
          SangriaField(
            "aggregate",
            aggregateTypeForModel(model),
            resolve = (ctx: Context[ApiUserContext, IdBasedConnection[PrismaNode]]) => ctx.value.parent.args
          )
        )
      }
    )
  }

  def aggregateTypeForModel(model: models.Model): ObjectType[ApiUserContext, QueryArguments] = {
    ObjectType(
      name = s"Aggregate${model.name}",
      fields = List(
        SangriaField(
          "count",
          IntType,
          resolve = (ctx: Context[ApiUserContext, QueryArguments]) => CountNodesDeferred(model, ctx.value)
        )
      )
    )
  }

  protected def modelToObjectType(model: models.Model): ObjectType[ApiUserContext, PrismaNode] = {
    new ObjectType(
      name = model.name,
      description = None,
      fieldsFn = () => {
        model.fields
          .filter(_.isVisible)
          .filter(field =>
            field.isScalar match {
              case true  => true
              case false => withRelations
          })
          .map(mapClientField(model))
      },
      interfaces = {
        val idFieldHasRightType = model.idField.exists(f =>
          f.name == ReservedFields.idFieldName && (f.typeIdentifier == TypeIdentifier.String || f.typeIdentifier == TypeIdentifier.Cuid))
        if (model.hasVisibleIdField && idFieldHasRightType) nodeInterface.toList else List.empty
      },
      instanceCheck = (value: Any, valClass: Class[_], tpe: ObjectType[ApiUserContext, _]) =>
        value match {
          case PrismaNode(_, _, Some(tpe.name)) => true
          case PrismaNode(_, _, Some(_))        => false
          case _                                => valClass.isAssignableFrom(value.getClass)
      },
      astDirectives = Vector.empty
    )
  }

  def mapClientField(model: models.Model)(field: models.Field): SangriaField[ApiUserContext, PrismaNode] = SangriaField(
    field.name,
    fieldType = mapToOutputType(Some(model), field),
    arguments = mapToListConnectionArguments(model, field),
    resolve = (ctx: Context[ApiUserContext, PrismaNode]) => mapToOutputResolve(model, field)(ctx),
    tags = List()
  )

  def mapToOutputType(model: Option[models.Model], field: models.Field): OutputType[Any] = {
    val outputType: OutputType[Any] = field match {
      case f: RelationField => resolveConnection(f)
      case f: ScalarField =>
        f.typeIdentifier match {
          case TypeIdentifier.String   => StringType
          case TypeIdentifier.Int      => IntType
          case TypeIdentifier.Float    => FloatType
          case TypeIdentifier.Boolean  => BooleanType
          case TypeIdentifier.Cuid     => IDType
          case TypeIdentifier.UUID     => UUIDType
          case TypeIdentifier.DateTime => DateTimeType
          case TypeIdentifier.Json     => JsonType
          case TypeIdentifier.Enum     => SchemaBuilderUtils.mapEnumFieldToInputType(f)
        }
    }

    if (field.isScalar && field.isList) {
      ListType(outputType)
    } else if (!field.isRequired) {
      OptionType(outputType)
    } else {
      outputType
    }
  }

  def resolveConnection(field: RelationField): OutputType[Any] = field.isList match {
    case true  => ListType(modelObjectTypes(field.relatedModel_!.name))
    case false => modelObjectTypes(field.relatedModel_!.name)
  }

  def mapToListConnectionArguments(model: models.Model, field: models.Field): List[Argument[Option[Any]]] = field match {
    case f if f.isHidden                                              => List.empty
    case _: ScalarField                                               => List.empty
    case f: RelationField if f.isList && !f.relatedModel_!.isEmbedded => mapToListConnectionArguments(f.relatedModel_!)
    case f: RelationField if f.isList && f.relatedModel_!.isEmbedded  => List.empty
    case f: RelationField if !f.isList                                => List.empty
  }

  def mapToListConnectionArguments(model: Model): List[Argument[Option[Any]]] = {
    import SangriaQueryArguments._
    val skipArgument = Argument("skip", OptionInputType(IntType))

    List(
      whereArgument(model, project, capabilities = capabilities),
      orderByArgument(model).asInstanceOf[Argument[Option[Any]]],
      skipArgument.asInstanceOf[Argument[Option[Any]]],
      IdBasedConnection.Args.After.asInstanceOf[Argument[Option[Any]]],
      IdBasedConnection.Args.Before.asInstanceOf[Argument[Option[Any]]],
      IdBasedConnection.Args.First.asInstanceOf[Argument[Option[Any]]],
      IdBasedConnection.Args.Last.asInstanceOf[Argument[Option[Any]]]
    )
  }

  def mapToUniqueArguments(model: models.Model): List[Argument[_]] = {
    import com.prisma.util.coolSangria.FromInputImplicit.DefaultScalaResultMarshaller

    model.scalarNonListFields
      .filter(_.isUnique)
      .map(field => Argument(field.name, SchemaBuilderUtils.mapToOptionalInputType(field)))
  }

  def extractQueryArgumentsFromContext(model: Model, ctx: Context[ApiUserContext, Unit]): QueryArguments = {
    extractQueryArgumentsFromContext(model, ctx, isSubscriptionFilter = false)
  }

  def extractQueryArgumentsFromContextForSubscription(model: Model, ctx: Context[_, Unit]): QueryArguments = {
    extractQueryArgumentsFromContext(model, ctx, isSubscriptionFilter = true)
  }

  private def extractQueryArgumentsFromContext(model: Model, ctx: Context[_, Unit], isSubscriptionFilter: Boolean): QueryArguments = {
    def convertCursorToGcValue(s: String) = model.idField_!.typeIdentifier match {
      case TypeIdentifier.Cuid => StringIdGCValue(s)
      case TypeIdentifier.UUID => UuidGCValue.parse_!(s)
      case TypeIdentifier.Int  => IntGCValue(s.toInt)
      case x                   => sys.error(s"This must not happen. $x is not a valid type identifier for an id field.")
    }

    val rawFilterOpt: Option[Map[String, Any]] = ctx.argOpt[Map[String, Any]]("where")
    val filterOpt                              = rawFilterOpt.map(FilterHelper.getFilterAst(_, model, isSubscriptionFilter))
    val skipOpt                                = ctx.argOpt[Int]("skip")
    val orderByOpt                             = ctx.argOpt[OrderBy]("orderBy")
    val afterOpt                               = ctx.argOpt[String](IdBasedConnection.Args.After.name).map(convertCursorToGcValue)
    val beforeOpt                              = ctx.argOpt[String](IdBasedConnection.Args.Before.name).map(convertCursorToGcValue)
    val firstOpt                               = ctx.argOpt[Int](IdBasedConnection.Args.First.name)
    val lastOpt                                = ctx.argOpt[Int](IdBasedConnection.Args.Last.name)

    QueryArguments(skipOpt, afterOpt, firstOpt, beforeOpt, lastOpt, filterOpt, orderByOpt)
  }

  def mapToOutputResolve[C <: ApiUserContext](model: models.Model, field: models.Field)(
      ctx: Context[C, PrismaNode]): sangria.schema.Action[ApiUserContext, _] = {

    val item: PrismaNode = unwrapDataItemFromContext(ctx)

    field match {
      case f: ScalarField if f.isList =>
        if (capabilities.has(EmbeddedScalarListsCapability)) item.data.map(field.name).value else ScalarListDeferred(model, f, item.id)

      case f: ScalarField if !f.isList =>
        item.data.map(field.name).value

      case f: RelationField if f.isList && f.relation.isInlineRelation =>
        val arguments = extractQueryArgumentsFromContext(f.relatedModel_!, ctx.asInstanceOf[Context[ApiUserContext, Unit]])

        f.relationIsInlinedInParent match {
          case true =>
            item.data.map.get(f.name) match {
              case Some(list: ListGCValue) =>
                val existingFilter: Filter = arguments.filter.getOrElse(Filter.empty)
                val newFilter              = AndFilter(Vector(ScalarFilter(f.relatedModel_!.idField_!, In(list.values)), existingFilter))
                val newQueryArguments      = arguments.copy(filter = Some(newFilter))
                DeferredValue(GetNodesDeferred(f.relatedModel_!, newQueryArguments, ctx.getSelectedFields(f.relatedModel_!))).map(_.toNodes)

              case _ => Vector.empty[PrismaNode]
            }
          case false =>
            DeferredValue(GetNodesByParentDeferred(f, item.id, arguments, ctx.getSelectedFields(f.relatedModel_!))).map(_.toNodes)
        }

      case f: RelationField if f.isList && f.relatedModel_!.isEmbedded =>
        item.data.map(f.name) match {
          case ListGCValue(values) => values.map(v => PrismaNode(v.asRoot.idFieldByName(f.relatedModel_!.idField_!.name), v.asRoot))
          case NullGCValue         => Vector.empty[PrismaNode]
          case x                   => sys.error("not handled yet" + x)
        }

      case f: RelationField if !f.isList && f.relation.isInlineRelation =>
        f.relationIsInlinedInParent match {
          case true =>
            item.data.map.get(f.name) match {
              case Some(id: IdGCValue) =>
                GetNodeDeferred(f.relatedModel_!, NodeSelector.forId(f.relatedModel_!, id), ctx.getSelectedFields(f.relatedModel_!))
              case _ => None
            }

          case false =>
            GetNodeByParentDeferred(f, item.id, QueryArguments.empty, ctx.getSelectedFields(f.relatedModel_!))
        }

      case f: RelationField if !f.isList && f.relatedModel_!.isEmbedded =>
        item.data.map(field.name) match {
          case NullGCValue => None
          case value       => Some(PrismaNode(value.asRoot.idFieldByName(f.relatedModel_!.idField_!.name), value.asRoot))
        }

      case f: RelationField if f.isList =>
        val arguments = extractQueryArgumentsFromContext(f.relatedModel_!, ctx.asInstanceOf[Context[ApiUserContext, Unit]])
        DeferredValue(GetNodesByParentDeferred(f, item.id, arguments, ctx.getSelectedFields(f.relatedModel_!))).map(_.toNodes)

      case f: RelationField if !f.isList =>
        val arguments = extractQueryArgumentsFromContext(f.relatedModel_!, ctx.asInstanceOf[Context[ApiUserContext, Unit]])
        GetNodeByParentDeferred(f, item.id, arguments, ctx.getSelectedFields(f.relatedModel_!))
    }
  }

  def unwrapDataItemFromContext[C <: ApiUserContext](ctx: Context[C, PrismaNode]) = {
    // note: ctx.value is sometimes of type Some[DataItem] at runtime even though the type is DataItem
    //metacounts of relations being required or not is one cause see RequiredRelationMetaQueriesSpec
    // todo: figure out why and fix issue at source
    ctx.value.asInstanceOf[Any] match {
      case Some(x: PrismaNode) => x
      case x: PrismaNode       => x
      case None                => throw new Exception("Resolved DataItem was None. This is unexpected - please investigate why and fix.")
    }
  }
}

object FilterHelper {

  def getFilterAst(input: Map[String, Any], model: Model, isSubscriptionFilter: Boolean = false): AndFilter = {
    val initial = generateFilterElement(input, model, isSubscriptionFilter)
    AndFilter(Vector(Optimizations.FilterOptimizer.optimize(initial)))
  }

  def generateFilterElement(input: Map[String, Any], model: Model, isSubscriptionFilter: Boolean = false): Filter = {
    val filterArguments = new FilterArguments(model, isSubscriptionFilter)

    val filters = input.map {
      case (key, value) =>
        val FieldFilterTuple(field, filter)                                    = filterArguments.lookup(key)
        lazy val asRelationField                                               = field.get.asInstanceOf[RelationField]
        lazy val asScalarField                                                 = field.get.asInstanceOf[ScalarField]
        def isScalarNonListFilter(filterName: String): Boolean                 = field.isDefined && field.get.isScalar && !field.get.isList && filter.name == filterName
        def isScalarListFilter(filterName: String): Boolean                    = field.isDefined && field.get.isScalar && field.get.isList && filter.name == filterName
        def isRelationFilter(filterName: String): Boolean                      = field.isDefined && field.get.isRelation && filter.name == filterName
        def isOneRelationFilter(filterName: String): Boolean                   = isRelationFilter(filterName) && !field.get.isList
        def isManyRelationFilter(filterName: String): Boolean                  = isRelationFilter(filterName) && field.get.isList
        def isFilterList(value: Seq[Any], filterName: String): Boolean         = value.nonEmpty && value.head.isInstanceOf[Map[_, _]] && filter.name == filterName
        def getGCValue(value: Any): GCValue                                    = GCAnyConverter(field.get.typeIdentifier, isList = false).toGCValue(unwrapSome(value)).get
        def scalarFilter(condition: ScalarCondition): ScalarFilter             = ScalarFilter(asScalarField, condition)
        def scalarListFilter(condition: ScalarListCondition): ScalarListFilter = ScalarListFilter(asScalarField, condition)
        def generateSubFilter(value: Map[_, _], model: Model): Filter          = generateFilterElement(value.asInstanceOf[Map[String, Any]], model, isSubscriptionFilter)
        def generateSubFilters(values: Seq[Any]): Vector[Filter]               = values.map(x => generateSubFilter(x.asInstanceOf[Map[String, Any]], model)).toVector
        def relationFilter(value: Map[_, _], condition: RelationCondition): RelationFilter =
          RelationFilter(asRelationField, generateSubFilter(value, asRelationField.relatedModel_!), condition)

        unwrapSome(value) match {
          //-------------------------RECURSION-----------------------------
          case value: Map[_, _] if isLogicFilter(key) || (isSubscriptionFilter && key == "node") => generateSubFilter(value, model)
          case value: Map[_, _] if isManyRelationFilter(filterName = "_every")                   => relationFilter(value, EveryRelatedNode)
          case value: Map[_, _] if isManyRelationFilter(filterName = "_some")                    => relationFilter(value, AtLeastOneRelatedNode)
          case value: Map[_, _] if isManyRelationFilter(filterName = "_none")                    => relationFilter(value, NoRelatedNode)
          case value: Map[_, _] if isRelationFilter(filterName = "")                             => relationFilter(value, ToOneRelatedNode)
          case Seq() if filter.name == "AND"                                                     => TrueFilter
          case value: Seq[Any] if isFilterList(value, filterName = "AND")                        => AndFilter(generateSubFilters(value))
          case Seq() if filter.name == "OR"                                                      => FalseFilter
          case value: Seq[Any] if isFilterList(value, filterName = "OR")                         => OrFilter(generateSubFilters(value))
          case Seq() if filter.name == "NOT"                                                     => TrueFilter
          case value: Seq[Any] if isFilterList(value, filterName = "NOT")                        => NotFilter(generateSubFilters(value))

          //--------------------------ANCHORS------------------------------
          case values: Seq[Any] if isScalarListFilter(filterName = "_contains_every") => scalarListFilter(ListContainsEvery(values.map(getGCValue).toVector))
          case values: Seq[Any] if isScalarListFilter(filterName = "_contains_some")  => scalarListFilter(ListContainsSome(values.map(getGCValue).toVector))
          case value if isScalarListFilter(filterName = "_contains")                  => scalarListFilter(ListContains(getGCValue(value)))
          case values: Seq[Any] if isScalarNonListFilter(filterName = "_in")          => scalarFilter(In(values.map(getGCValue).toVector))
          case null if isScalarNonListFilter(filterName = "_in")                      => scalarFilter(In(Vector(NullGCValue)))
          case values: Seq[Any] if isScalarNonListFilter(filterName = "_not_in")      => scalarFilter(NotIn(values.map(getGCValue).toVector))
          case null if isScalarNonListFilter(filterName = "_not_in")                  => scalarFilter(NotIn(Vector(NullGCValue)))
          case value if isScalarNonListFilter(filterName = "")                        => scalarFilter(Equals(getGCValue(value)))
          case value if isScalarNonListFilter(filterName = "_not")                    => scalarFilter(NotEquals(getGCValue(value)))
          case value if isScalarNonListFilter(filterName = "_contains")               => scalarFilter(Contains(getGCValue(value)))
          case value if isScalarNonListFilter(filterName = "_not_contains")           => scalarFilter(NotContains(getGCValue(value)))
          case value if isScalarNonListFilter(filterName = "_starts_with")            => scalarFilter(StartsWith(getGCValue(value)))
          case value if isScalarNonListFilter(filterName = "_not_starts_with")        => scalarFilter(NotStartsWith(getGCValue(value)))
          case value if isScalarNonListFilter(filterName = "_ends_with")              => scalarFilter(EndsWith(getGCValue(value)))
          case value if isScalarNonListFilter(filterName = "_not_ends_with")          => scalarFilter(NotEndsWith(getGCValue(value)))
          case value if isScalarNonListFilter(filterName = "_lt")                     => scalarFilter(LessThan(getGCValue(value)))
          case value if isScalarNonListFilter(filterName = "_lte")                    => scalarFilter(LessThanOrEquals(getGCValue(value)))
          case value if isScalarNonListFilter(filterName = "_gt")                     => scalarFilter(GreaterThan(getGCValue(value)))
          case value if isScalarNonListFilter(filterName = "_gte")                    => scalarFilter(GreaterThanOrEquals(getGCValue(value)))
          case _ if isOneRelationFilter(filterName = "")                              => OneRelationIsNullFilter(asRelationField)
          case value: Boolean if field.isEmpty && filter.name == "boolean"            => if (value) TrueFilter else FalseFilter
          case None if field.isDefined                                                => NodeSubscriptionFilter
          case null if field.isDefined && field.get.isList && field.get.isRelation    => throw APIErrors.FilterCannotBeNullOnToManyField(field.get.name)
          case x                                                                      => sys.error("Missing case " + x)
        }
    }
    AndFilter(filters.toVector)
  }

  def unwrapSome(value: Any): Any = value match {
    case Some(x) => x
    case x       => x
  }
}
