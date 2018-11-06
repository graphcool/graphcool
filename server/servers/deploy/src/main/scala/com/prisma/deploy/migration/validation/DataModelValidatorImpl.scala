package com.prisma.deploy.migration.validation
import com.prisma.deploy.connector.FieldRequirementsInterface
import com.prisma.deploy.gc_value.GCStringConverter
import com.prisma.deploy.migration.DataSchemaAstExtensions._
import com.prisma.shared.models.FieldBehaviour._
import com.prisma.shared.models.{ConnectorCapability, FieldBehaviour, RelationStrategy, TypeIdentifier}
import org.scalactic.{Bad, Good, Or}
import sangria.ast.{Argument => _, _}
import com.prisma.gc_values.GCValue
import com.prisma.shared.models.ApiConnectorCapability.{EmbeddedScalarListsCapability, NonEmbeddedScalarListCapability}
import com.prisma.shared.models.OnDelete.OnDelete
import com.prisma.shared.models.TypeIdentifier.ScalarTypeIdentifier
import com.prisma.utils.boolean.BooleanUtils

import scala.collection.immutable.Seq
import scala.util.{Failure, Success, Try}

object DataModelValidatorImpl extends DataModelValidator {
  override def validate(
      dataModel: String,
      fieldRequirements: FieldRequirementsInterface,
      capabilities: Set[ConnectorCapability]
  ): PrismaSdl Or Vector[DeployError] = {
    DataModelValidatorImpl(dataModel, fieldRequirements, capabilities).validate
  }
}

case class DataModelValidatorImpl(
    dataModel: String,
    fieldRequirements: FieldRequirementsInterface,
    capabilities: Set[ConnectorCapability]
) {
  import com.prisma.deploy.migration.DataSchemaAstExtensions._

  val result   = GraphQlSdlParser.parse(dataModel)
  lazy val doc = result.get

  def validate: PrismaSdl Or Vector[DeployError] = {
    val errors = validateSyntax
    if (errors.isEmpty) {
      Good(generateSDL)
    } else {
      Bad(errors.toVector)
    }
  }

  def generateSDL: PrismaSdl = {

//    val enumTypes: Vector[PrismaSdl => PrismaEnum] = doc.enumNames.map { name =>
//      val definition: EnumTypeDefinition = doc.enumType(name).get
//      val enumValues                     = definition.values.map(_.name)
//      PrismaEnum(name, values = enumValues)(_)
//    }
    val enumTypes = Vector.empty

    val prismaTypes: Vector[PrismaSdl => PrismaType] = doc.objectTypes.map { typeDef =>
      val prismaFields = typeDef.fields.map {
        case x if isRelationField(x) =>
          val relationDirective = RelationDirective.value(doc, typeDef, x, capabilities).get
          RelationalPrismaField(
            name = x.name,
            relationDbDirective = x.relationDBDirective,
            strategy = relationDirective.strategy,
            isList = x.isList,
            isRequired = x.isRequired,
            referencesType = x.typeName,
            relationName = relationDirective.name,
            cascade = relationDirective.onDelete
          )(_)

        case x if isEnumField(x) =>
          EnumPrismaField(
            name = x.name,
            columnName = x.dbName,
            isList = x.isList,
            isRequired = x.isRequired,
            isUnique = x.isUnique,
            enumName = x.typeName,
            defaultValue = DefaultDirective.value(doc, typeDef, x, capabilities)
          )(_)

        case x if isScalarField(x) =>
          ScalarPrismaField(
            name = x.name,
            columnName = x.dbName,
            isList = x.isList,
            isRequired = x.isRequired,
            isUnique = x.isUnique,
            typeIdentifier = doc.typeIdentifierForTypename(x.fieldType),
            defaultValue = DefaultDirective.value(doc, typeDef, x, capabilities),
            behaviour = FieldDirective.behaviour.flatMap(_.value(doc, typeDef, x, capabilities)).headOption
          )(_)
      }

      PrismaType(
        name = typeDef.name,
        tableName = typeDef.dbName,
        isEmbedded = typeDef.isEmbedded,
        isRelationTable = typeDef.isRelationTable,
        fieldFn = prismaFields
      )(_)
    }

    PrismaSdl(typesFn = prismaTypes, enumsFn = enumTypes)
  }

  def validateSyntax: Seq[DeployError] = result match {
    case Success(_) => validateInternal
    case Failure(e) => List(DeployError.global(s"There's a syntax error in the Schema Definition. ${e.getMessage}"))
  }

  lazy val allFieldAndTypes: Seq[FieldAndType] = for {
    objectType <- doc.objectTypes
    field      <- objectType.fields
  } yield FieldAndType(objectType, field)

  def validateInternal: Seq[DeployError] = {
    val reservedFieldsValidations    = tryValidation(validateTypes())
    val fieldDirectiveValidationsNew = tryValidation(validateFieldDirectives())
    val allValidations = Vector(
      reservedFieldsValidations,
      fieldDirectiveValidationsNew
    )

    val validationErrors: Vector[DeployError] = allValidations.collect { case Good(x) => x }.flatten
    val validationFailures: Vector[Throwable] = allValidations.collect { case Bad(e) => e }

    // We don't want to return unhelpful exception messages to the user if there are normal validation errors. It is likely that the exceptions won't occur if those get fixed first.
    val errors = if (validationErrors.nonEmpty) {
      validationErrors
    } else {
      validationFailures.map { throwable =>
        throwable.printStackTrace()
        DeployError.global(s"An unknown error happened: $throwable")
      }
    }

    errors.distinct
  }

  def tryValidation(block: => Seq[DeployError]): Or[Seq[DeployError], Throwable] = Or.from(Try(block))

  def validateTypes(): Seq[DeployError] = {
    doc.objectTypes.flatMap { objectType =>
      val hasIdDirective = objectType.fields.exists(_.hasDirective("id"))
      if (!hasIdDirective && !objectType.isRelationTable) {
        Some(DeployError.apply(objectType.name, s"One field of the type `${objectType.name}` must be marked as the id field with the `@id` directive."))
      } else {
        None
      }
    }
  }

  def validateFieldDirectives(): Seq[DeployError] = {
    for {
      fieldAndType <- allFieldAndTypes
      directive    <- fieldAndType.fieldDef.directives
      validator    <- FieldDirective.all
      if directive.name == validator.name
      argumentErrors  = validateDirectiveArguments(directive, validator, fieldAndType)
      validationError = validator.validate(doc, fieldAndType.objectType, fieldAndType.fieldDef, directive, capabilities)
      error           <- argumentErrors ++ validationError
    } yield {
      error
    }
  }

  def validateDirectiveArguments(directive: Directive, validator: FieldDirective[_], fieldAndType: FieldAndType): Vector[DeployError] = {
    val requiredArgErrors = for {
      argumentRequirement <- validator.requiredArgs
      schemaError <- directive.argument(argumentRequirement.name) match {
                      case None =>
                        Some(DeployErrors.directiveMissesRequiredArgument(fieldAndType, validator.name, argumentRequirement.name))
                      case Some(arg) =>
                        argumentRequirement.validate(arg.value).map(error => DeployError(fieldAndType, error))
                    }
    } yield schemaError

    val optionalArgErrors = for {
      argumentRequirement <- validator.optionalArgs
      argument            <- directive.argument(argumentRequirement.name)
      schemaError <- argumentRequirement.validate(argument.value).map { errorMsg =>
                      DeployError(fieldAndType, errorMsg)
                    }
    } yield schemaError

    requiredArgErrors ++ optionalArgErrors
  }

  private def isSelfRelation(fieldAndType: FieldAndType): Boolean  = fieldAndType.fieldDef.typeName == fieldAndType.objectType.name
  private def isRelationField(fieldAndType: FieldAndType): Boolean = isRelationField(fieldAndType.fieldDef)
  private def isRelationField(fieldDef: FieldDefinition): Boolean  = !isScalarField(fieldDef) && !isEnumField(fieldDef)

  private def isScalarField(fieldAndType: FieldAndType): Boolean = isScalarField(fieldAndType.fieldDef)
  private def isScalarField(fieldDef: FieldDefinition): Boolean  = fieldDef.hasScalarType

  private def isEnumField(fieldDef: FieldDefinition): Boolean = doc.isEnumType(fieldDef.typeName)
}

trait ValidatorShared {
  def validateString(value: sangria.ast.Value): Option[String] = {
    value match {
      case v: sangria.ast.StringValue => None
      case _                          => Some("This argument must be a String.")
    }
  }

  def validateEnum(validValues: Vector[String])(value: sangria.ast.Value): Option[String] = {
    if (validValues.contains(value.asString)) {
      None
    } else {
      Some(s"Valid values are: ${validValues.mkString(",")}.")
    }
  }
}

trait FieldDirective[T] extends BooleanUtils with ValidatorShared { // could introduce a new interface for type level directives
  def name: String
  def requiredArgs: Vector[ArgumentRequirement]
  def optionalArgs: Vector[ArgumentRequirement]

  // gets called if the directive was found. Can return an error message
  def validate(
      document: Document,
      typeDef: ObjectTypeDefinition,
      fieldDef: FieldDefinition,
      directive: sangria.ast.Directive,
      capabilities: Set[ConnectorCapability]
  ): Option[DeployError]

  def value(
      document: Document,
      typeDef: ObjectTypeDefinition,
      fieldDef: FieldDefinition,
      capabilities: Set[ConnectorCapability]
  ): Option[T]
}

object FieldDirective {
  val behaviour = Vector(IdDirective, CreatedAtDirective, UpdatedAtDirective, ScalarListDirective)
  val all       = Vector(DefaultDirective) ++ behaviour

}

case class ArgumentRequirement(name: String, validate: sangria.ast.Value => Option[String])

object DefaultDirective extends FieldDirective[GCValue] {
  val valueArg = "value"

  override def name         = "default"
  override def requiredArgs = Vector(ArgumentRequirement("value", _ => None))
  override def optionalArgs = Vector.empty

  override def validate(
      document: Document,
      typeDef: ObjectTypeDefinition,
      fieldDef: FieldDefinition,
      directive: Directive,
      capabilities: Set[ConnectorCapability]
  ): Option[DeployError] = {
    val placementIsInvalid = !document.isEnumType(fieldDef.typeName) && !fieldDef.isValidScalarNonListType
    if (placementIsInvalid) {
      return Some(DeployError(typeDef, fieldDef, "The `@default` directive must only be placed on scalar fields that are not lists."))
    }

    val value          = directive.argument_!(valueArg).value
    val typeIdentifier = document.typeIdentifierForTypename(fieldDef.fieldType).asInstanceOf[ScalarTypeIdentifier]
    (typeIdentifier, value) match {
      case (TypeIdentifier.String, _: StringValue)   => None
      case (TypeIdentifier.Float, _: FloatValue)     => None
      case (TypeIdentifier.Boolean, _: BooleanValue) => None
      case (TypeIdentifier.Json, _: StringValue)     => None
      case (TypeIdentifier.DateTime, _: StringValue) => None
      case (TypeIdentifier.Enum, v: EnumValue) => {
        val enumValues = document.enumType(fieldDef.typeName).get.values.map(_.name)
        if (enumValues.contains(v.asString)) {
          None
        } else {
          Some(DeployError(typeDef, fieldDef, s"The default value is invalid for this enum. Valid values are: ${enumValues.mkString(", ")}."))
        }
      }
      case (ti, v) => Some(DeployError(typeDef, fieldDef, s"The value ${v.renderPretty} is not a valid default for fields of type ${ti.code}."))
    }
  }

  override def value(
      document: Document,
      typeDef: ObjectTypeDefinition,
      fieldDef: FieldDefinition,
      capabilities: Set[ConnectorCapability]
  ): Option[GCValue] = {
    fieldDef.directive(name).map { directive =>
      val value          = directive.argument_!(valueArg).valueAsString
      val typeIdentifier = document.typeIdentifierForTypename(fieldDef.fieldType)
      GCStringConverter(typeIdentifier, fieldDef.isList).toGCValue(value).get
    }
  }
}

object IdDirective extends FieldDirective[IdBehaviour] {
  val autoValue           = "AUTO"
  val noneValue           = "NONE"
  val validStrategyValues = Set(autoValue, noneValue)

  override def name         = "id"
  override def requiredArgs = Vector.empty
  override def optionalArgs = Vector(ArgumentRequirement("strategy", isStrategyValueValid))

  private def isStrategyValueValid(value: sangria.ast.Value): Option[String] = {
    if (validStrategyValues.contains(value.asString)) {
      None
    } else {
      Some(s"Valid values for the strategy argument of `@$name` are: ${validStrategyValues.mkString(", ")}.")
    }
  }

  override def validate(
      document: Document,
      typeDef: ObjectTypeDefinition,
      fieldDef: FieldDefinition,
      directive: Directive,
      capabilities: Set[ConnectorCapability]
  ) = {
    if (typeDef.isEmbedded) {
      Some(DeployError(typeDef, fieldDef, s"The `@$name` directive is not allowed on embedded types."))
    } else {
      None
    }
  }

  override def value(document: Document, typeDef: ObjectTypeDefinition, fieldDef: FieldDefinition, capabilities: Set[ConnectorCapability]) = {
    fieldDef.directive(name).map { directive =>
      directive.argumentValueAsString("strategy").getOrElse(autoValue) match {
        case `autoValue` => IdBehaviour(FieldBehaviour.IdStrategy.Auto)
        case `noneValue` => IdBehaviour(FieldBehaviour.IdStrategy.None)
        case x           => sys.error(s"Encountered unknown strategy $x")
      }
    }
  }
}

object CreatedAtDirective extends FieldDirective[CreatedAtBehaviour.type] {
  override def name         = "createdAt"
  override def requiredArgs = Vector.empty
  override def optionalArgs = Vector.empty

  override def validate(
      document: Document,
      typeDef: ObjectTypeDefinition,
      fieldDef: FieldDefinition,
      directive: Directive,
      capabilities: Set[ConnectorCapability]
  ) = {
    if (fieldDef.typeName == "DateTime" && fieldDef.isRequired) {
      None
    } else {
      Some(DeployError(typeDef, fieldDef, s"Fields that are marked as @createdAt must be of type `DateTime!`."))
    }
  }

  override def value(document: Document, typeDef: ObjectTypeDefinition, fieldDef: FieldDefinition, capabilities: Set[ConnectorCapability]) = {
    fieldDef.directive(name).map { _ =>
      CreatedAtBehaviour
    }
  }
}

object UpdatedAtDirective extends FieldDirective[UpdatedAtBehaviour.type] {
  override def name         = "updatedAt"
  override def requiredArgs = Vector.empty
  override def optionalArgs = Vector.empty

  override def validate(
      document: Document,
      typeDef: ObjectTypeDefinition,
      fieldDef: FieldDefinition,
      directive: Directive,
      capabilities: Set[ConnectorCapability]
  ) = {
    if (fieldDef.typeName == "DateTime" && fieldDef.isRequired) {
      None
    } else {
      Some(DeployError(typeDef, fieldDef, s"Fields that are marked as @updatedAt must be of type `DateTime!`."))
    }
  }

  override def value(document: Document, typeDef: ObjectTypeDefinition, fieldDef: FieldDefinition, capabilities: Set[ConnectorCapability]) = {
    fieldDef.directive(name).map { _ =>
      UpdatedAtBehaviour
    }
  }
}

object ScalarListDirective extends FieldDirective[ScalarListBehaviour] {
  override def name         = "scalarList"
  override def requiredArgs = Vector.empty
  override def optionalArgs = Vector(ArgumentRequirement("strategy", isValidStrategyArgument))

  val embeddedValue       = "EMBEDDED"
  val relationValue       = "RELATION"
  val validStrategyValues = Set(embeddedValue, relationValue)

  private def isValidStrategyArgument(value: sangria.ast.Value): Option[String] = {
    if (validStrategyValues.contains(value.asString)) {
      None
    } else {
      Some(s"Valid values for the strategy argument of `@scalarList` are: ${validStrategyValues.mkString(", ")}.")
    }
  }

  override def validate(
      document: Document,
      typeDef: ObjectTypeDefinition,
      fieldDef: FieldDefinition,
      directive: Directive,
      capabilities: Set[ConnectorCapability]
  ) = {
    if (!fieldDef.isValidScalarListType) {
      Some(DeployError(typeDef, fieldDef, s"Fields that are marked as `@scalarList` must be either of type `[String!]` or `[String!]!`."))
    } else {
      None
    }
  }

  override def value(document: Document, typeDef: ObjectTypeDefinition, fieldDef: FieldDefinition, capabilities: Set[ConnectorCapability]) = {
    lazy val behaviour = fieldDef.directive(name) match {
      case Some(directive) =>
        directive.argument_!("strategy").valueAsString match {
          case `relationValue` => ScalarListBehaviour(FieldBehaviour.ScalarListStrategy.Relation)
          case `embeddedValue` => ScalarListBehaviour(FieldBehaviour.ScalarListStrategy.Embedded)
          case x               => sys.error(s"Unknown strategy $x")
        }
      case None =>
        if (capabilities.contains(EmbeddedScalarListsCapability)) {
          ScalarListBehaviour(ScalarListStrategy.Embedded)
        } else if (capabilities.contains(NonEmbeddedScalarListCapability)) {
          ScalarListBehaviour(ScalarListStrategy.Relation)
        } else {
          sys.error("should not happen")
        }
    }
    fieldDef.isValidScalarListType.toOption {
      behaviour
    }
  }
}

case class RelationDirectiveHolder(name: Option[String], onDelete: OnDelete, strategy: RelationStrategy)
object RelationDirective extends FieldDirective[RelationDirectiveHolder] {
  override def name = "relation"

  override def requiredArgs = Vector.empty

  override def optionalArgs = Vector(
    ArgumentRequirement("name", validateString),
    ArgumentRequirement("onDelete", validateEnum(Vector("ON_DELETE", "SET_NULL"))),
    ArgumentRequirement("strategy", validateEnum(Vector("AUTO", "EMBED", "RELATION_TABLE")))
  )

  override def validate(
      document: Document,
      typeDef: ObjectTypeDefinition,
      fieldDef: FieldDefinition,
      directive: Directive,
      capabilities: Set[ConnectorCapability]
  ) = {
    None
  }

  override def value(document: Document, typeDef: ObjectTypeDefinition, fieldDef: FieldDefinition, capabilities: Set[ConnectorCapability]) = {
    if (fieldDef.isRelationField(document)) {
      val strategy = fieldDef.directiveArgumentAsString(name, "strategy") match {
        case Some("AUTO")           => RelationStrategy.Auto
        case Some("EMBED")          => RelationStrategy.Embed
        case Some("RELATION_TABLE") => RelationStrategy.RelationTable
        case _                      => RelationStrategy.Auto
      }
      Some(RelationDirectiveHolder(fieldDef.relationName, fieldDef.onDelete, strategy))
    } else {
      None
    }
  }
}
