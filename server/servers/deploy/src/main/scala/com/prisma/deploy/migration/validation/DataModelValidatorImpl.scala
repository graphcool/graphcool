package com.prisma.deploy.migration.validation
import com.prisma.deploy.connector.FieldRequirementsInterface
import com.prisma.deploy.migration.DataSchemaAstExtensions._
import com.prisma.deploy.migration.validation.directives._
import com.prisma.deploy.validation.NameConstraints
import com.prisma.shared.models.ConnectorCapability.EmbeddedTypesCapability
import com.prisma.shared.models.FieldBehaviour.{IdBehaviour, IdStrategy}
import com.prisma.shared.models.{ConnectorCapabilities, ConnectorCapability, ReservedFields, TypeIdentifier}
import com.prisma.utils.boolean.BooleanUtils
import org.scalactic.{Bad, Good, Or}
import sangria.ast.{Argument => _, _}

import scala.collection.immutable.Seq
import scala.util.{Failure, Success, Try}

object DataModelValidatorImpl extends DataModelValidator {
  override def validate(
      dataModel: String,
      fieldRequirements: FieldRequirementsInterface,
      capabilities: ConnectorCapabilities
  ): PrismaSdl Or Vector[DeployError] = {
    DataModelValidatorImpl(dataModel, fieldRequirements, capabilities).validate
  }
}

case class DataModelValidatorImpl(
    dataModel: String,
    fieldRequirements: FieldRequirementsInterface,
    capabilities: ConnectorCapabilities
) {
  import com.prisma.deploy.migration.DataSchemaAstExtensions._
  import BooleanUtils._

  val result   = GraphQlSdlParser.parse(dataModel)
  lazy val doc = result.get

  def validate: PrismaSdl Or Vector[DeployError] = {
    val syntaxErrors = validateSyntax
    if (syntaxErrors.isEmpty) {
      val dataModel = generateSDL
      val semanticErrors = FieldDirective.all.flatMap(_.postValidate(dataModel, capabilities)).distinct ++
        TypeDirective.all.flatMap(_.postValidate(dataModel, capabilities)).distinct
      if (semanticErrors.isEmpty) {
        Good(dataModel)
      } else {
        Bad(semanticErrors)
      }
    } else {
      Bad(syntaxErrors.toVector)
    }
  }

  def generateSDL: PrismaSdl = {
    val enumTypes: Vector[PrismaSdl => PrismaEnum] = doc.enumTypes.map { definition =>
      val enumValues = definition.values.map(_.name)
      PrismaEnum(definition.name, values = enumValues)(_)
    }

    val prismaTypes: Vector[PrismaSdl => PrismaType] = doc.objectTypes.map { typeDef =>
      val prismaFields = typeDef.fields.map {
        case x if isRelationField(x) =>
          val relationDirective = RelationDirective.value(doc, typeDef, x, capabilities).get
          RelationalPrismaField(
            name = x.name,
            columnName = x.dbName,
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
            columnName = FieldDbDirective.value(doc, typeDef, x, capabilities),
            isList = x.isList,
            isRequired = x.isRequired,
            isUnique = x.isUnique,
            enumName = x.typeName,
            defaultValue = DefaultDirective.value(doc, typeDef, x, capabilities),
            behaviour = FieldDirective.behaviour.flatMap(_.value(doc, typeDef, x, capabilities)).headOption
          )(_)

        case x if isScalarField(x) =>
          ScalarPrismaField(
            name = x.name,
            columnName = FieldDbDirective.value(doc, typeDef, x, capabilities),
            isList = x.isList,
            isRequired = x.isRequired,
            isUnique = UniqueDirective.value(doc, typeDef, x, capabilities).getOrElse(false),
            typeIdentifier = doc.typeIdentifierForTypename(x.fieldType),
            defaultValue = DefaultDirective.value(doc, typeDef, x, capabilities),
            behaviour = FieldDirective.behaviour.flatMap(_.value(doc, typeDef, x, capabilities)).headOption
          )(_)
      }

      // FIXME: it should not be needed that embedded types have a hidden id field
      val extraField = typeDef.isEmbedded.toOption {
        ScalarPrismaField(
          name = ReservedFields.embeddedIdFieldName,
          columnName = None,
          isList = false,
          isRequired = false,
          isUnique = false,
          typeIdentifier = TypeIdentifier.Cuid,
          defaultValue = None,
          behaviour = Some(IdBehaviour(IdStrategy.Auto)),
          isHidden = true
        )(_)
      }
      PrismaType(
        name = typeDef.name,
        tableName = TypeDbDirective.value(doc, typeDef, capabilities),
        isEmbedded = EmbeddedDirective.value(doc, typeDef, capabilities).getOrElse(false),
        isRelationTable = typeDef.isRelationTable,
        fieldFn = prismaFields ++ extraField,
        indexes = IndexesDirective.value(doc, typeDef, capabilities).getOrElse(Vector())
      )(_)
    }

    PrismaSdl(typesFn = prismaTypes, enumsFn = enumTypes)
  }

  def validateSyntax: Seq[DeployError] = result match {
    case Success(_) => validateInternal
    case Failure(e) => List(DeployError.global(s"There's a syntax error in the data model. ${e.getMessage}"))
  }

  lazy val allFieldAndTypes: Seq[FieldAndType] = for {
    objectType <- doc.objectTypes
    field      <- objectType.fields
  } yield FieldAndType(objectType, field)

  def validateInternal: Seq[DeployError] = {
    val globalValidations         = tryValidation(GlobalValidations(doc).validate())
    val reservedFieldsValidations = tryValidation(validateTypes())
    val fieldDirectiveValidations = tryValidation(validateFieldDirectives())
    val typeDirectiveValidations  = tryValidation(validateTypeDirectives())
    val enumValidations           = tryValidation(EnumValidator(doc).validate())

    val allValidations = Vector(
      globalValidations,
      reservedFieldsValidations,
      fieldDirectiveValidations,
      enumValidations,
      typeDirectiveValidations
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
    doc.objectTypes.filter(!_.isRelationTable).flatMap { objectType =>
      ModelValidator(doc, objectType, capabilities).validate()
    }
  }

  def validateTypeDirectives(): Seq[DeployError] = {
    for {
      objectType   <- doc.objectTypes
      directive    <- objectType.directives
      validator    <- TypeDirective.all
      if directive.name == validator.name
      duplicateErrors  = validateDirectiveUniqueness(objectType)
      validationErrors = validator.validate(doc, objectType, directive, capabilities)
      error            <- duplicateErrors ++ validationErrors
    } yield error
  }

  def validateFieldDirectives(): Seq[DeployError] = {
    for {
      fieldAndType    <- allFieldAndTypes
      duplicateErrors = validateDirectiveUniqueness(fieldAndType)
      directive       <- fieldAndType.fieldDef.directives
      validator       <- FieldDirective.all
      if directive.name == validator.name
      argumentErrors   = validateDirectiveArguments(directive, validator, fieldAndType)
      validationErrors = validator.validate(doc, fieldAndType.objectType, fieldAndType.fieldDef, directive, capabilities)
      error            <- duplicateErrors ++ argumentErrors ++ validationErrors
    } yield {
      error
    }
  }

  def validateDirectiveArguments(directive: Directive, validator: FieldDirective[_], fieldAndType: FieldAndType): Vector[DeployError] = {
    val requiredArgErrors = for {
      argumentRequirement <- validator.requiredArgs(capabilities)
      schemaError <- directive.argument(argumentRequirement.name) match {
                      case None =>
                        Some(DeployErrors.directiveMissesRequiredArgument(fieldAndType, validator.name, argumentRequirement.name))
                      case Some(arg) =>
                        argumentRequirement.validate(arg.value).map(error => DeployError(fieldAndType, error))
                    }
    } yield schemaError

    val optionalArgErrors = for {
      argumentRequirement <- validator.optionalArgs(capabilities)
      argument            <- directive.argument(argumentRequirement.name)
      schemaError <- argumentRequirement.validate(argument.value).map { errorMsg =>
                      DeployError(fieldAndType, errorMsg)
                    }
    } yield schemaError

    requiredArgErrors ++ optionalArgErrors
  }

  def validateDirectiveUniqueness(fieldAndType: FieldAndType): Option[DeployError] = {
    val directives       = fieldAndType.fieldDef.directives
    val uniqueDirectives = directives.map(_.name).toSet
    if (uniqueDirectives.size != directives.size) {
      Some(DeployErrors.directivesMustAppearExactlyOnce(fieldAndType))
    } else {
      None
    }
  }

  def validateDirectiveUniqueness(objectType: ObjectTypeDefinition): Option[DeployError] = {
    val directives       = objectType.directives
    val uniqueDirectives = directives.map(_.name).toSet
    if (uniqueDirectives.size != directives.size) {
      Some(DeployErrors.directivesMustAppearExactlyOnce(objectType))
    } else {
      None
    }
  }

  private def isSelfRelation(fieldAndType: FieldAndType): Boolean  = fieldAndType.fieldDef.typeName == fieldAndType.objectType.name
  private def isRelationField(fieldAndType: FieldAndType): Boolean = isRelationField(fieldAndType.fieldDef)
  private def isRelationField(fieldDef: FieldDefinition): Boolean  = !isScalarField(fieldDef) && !isEnumField(fieldDef)

  private def isScalarField(fieldAndType: FieldAndType): Boolean = isScalarField(fieldAndType.fieldDef)
  private def isScalarField(fieldDef: FieldDefinition): Boolean  = fieldDef.hasScalarType

  private def isEnumField(fieldDef: FieldDefinition): Boolean = doc.isEnumType(fieldDef.typeName)
}

case class GlobalValidations(doc: Document) {
  def validate(): Vector[DeployError] = {
    validateDuplicateTypes()
  }

  def validateDuplicateTypes(): Vector[DeployError] = {
    val typeNames          = doc.objectTypes.map(_.name.toLowerCase)
    val duplicateTypeNames = typeNames.filter(name => typeNames.count(_ == name) > 1)

    for {
      duplicateTypeName <- duplicateTypeNames
      objectType        <- doc.objectTypes.find(_.name.equalsIgnoreCase(duplicateTypeName))
    } yield {
      DeployErrors.duplicateTypeName(objectType)
    }
  }
}

case class EnumValidator(doc: Document) {
  def validate(): Vector[DeployError] = {
    val duplicateNames = doc.enumNames.diff(doc.enumNames.distinct).distinct.flatMap(dupe => Some(DeployErrors.enumNamesMustBeUnique(doc.enumType(dupe).get)))

    val otherErrors = doc.enumTypes.flatMap { enumType =>
      val invalidEnumValues = enumType.valuesAsStrings.filter(!NameConstraints.isValidEnumValueName(_))

      if (enumType.values.exists(value => value.name.head.isLower)) {
        Some(DeployErrors.enumValuesMustBeginUppercase(enumType))
      } else if (invalidEnumValues.nonEmpty) {
        Some(DeployErrors.enumValuesMustBeValid(enumType, invalidEnumValues))
      } else {
        None
      }
    }

    duplicateNames ++ otherErrors
  }
}

case class ModelValidator(doc: Document, objectType: ObjectTypeDefinition, capabilities: ConnectorCapabilities) {
  def validate(): Vector[DeployError] = {
    val allValidations = Vector(
      tryValidation(validateMissingTypes),
      tryValidation(requiredIdDirectiveValidation.toVector),
      tryValidation(validateRelationFields),
      tryValidation(validateDuplicateFields)
    )

    val validationErrors: Vector[DeployError] = allValidations.collect { case Good(x) => x }.flatten
    val validationFailures: Vector[Throwable] = allValidations.collect { case Bad(e) => e }

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

  val requiredIdDirectiveValidation = {
    val hasIdDirective = objectType.fields.exists(_.hasDirective("id"))
    if (!hasIdDirective && !objectType.isRelationTable && !objectType.isEmbedded) {
      Some(DeployError.apply(objectType.name, s"One field of the type `${objectType.name}` must be marked as the id field with the `@id` directive."))
    } else {
      None
    }
  }

  val validateMissingTypes: Vector[DeployError] = {
    objectType.fields
      .filter(!_.hasScalarType)
      .collect { case fieldDef if !doc.isObjectOrEnumType(fieldDef.typeName) => DeployErrors.missingType(objectType, fieldDef) }
  }

  def validateDuplicateFields: Seq[DeployError] = {
    val fieldNames = objectType.fields.map(_.name.toLowerCase)
    for {
      field <- objectType.fields
      if fieldNames.count(_.equals(field.name.toLowerCase)) > 1
    } yield {
      DeployErrors.duplicateFieldName(FieldAndType(objectType, field))
    }
  }

  def validateRelationFields: Seq[DeployError] = {
    val ambiguousRelationFields: Vector[FieldAndType] = {
      val relationFields                                = objectType.relationFields(doc)
      val grouped: Map[String, Vector[FieldDefinition]] = relationFields.groupBy(_.typeName)
      val ambiguousFields                               = grouped.values.filter(_.size > 1).flatten.toVector
      ambiguousFields.map(field => FieldAndType(objectType, field))
    }

    val (schemaErrors, _) = partition(ambiguousRelationFields) {
      case fieldAndType if !fieldAndType.fieldDef.hasRelationDirectiveWithNameArg =>
        Left(DeployErrors.missingRelationDirective(fieldAndType))

      case fieldAndType if !fieldAndType.isSelfRelation && fieldAndType.relationCount(doc) > 2 =>
        Left(DeployErrors.relationDirectiveCannotAppearMoreThanTwice(fieldAndType))

      case fieldAndType if fieldAndType.isSelfRelation && fieldAndType.relationCount(doc) != 1 && fieldAndType.relationCount(doc) != 2 =>
        Left(DeployErrors.selfRelationMustAppearOneOrTwoTimes(fieldAndType))

      case fieldAndType =>
        Right(fieldAndType)
    }

    val relationFieldsWithRelationDirective = for {
      objectType <- doc.objectTypes
      field      <- objectType.fields
      if field.hasRelationDirectiveWithNameArg
      if field.isRelationField(doc)
    } yield FieldAndType(objectType, field)

    /**
      * Check that if a relation is not a self-relation and a relation-directive occurs only once that there is no
      * opposing field without a relationdirective on the other side.
      */
    val allowOnlyOneDirectiveOnlyWhenUnambiguous = relationFieldsWithRelationDirective.flatMap {
      case thisType if !thisType.isSelfRelation && thisType.relationCount(doc) == 1 =>
        val oppositeObjectType               = doc.objectType_!(thisType.fieldDef.typeName)
        val fieldsOnOppositeObjectType       = oppositeObjectType.fields.filter(_.typeName == thisType.objectType.name)
        val relationFieldsWithoutDirective   = fieldsOnOppositeObjectType.filter(f => !f.hasRelationDirectiveWithNameArg && f.isRelationField(doc))
        val relationFieldsPointingToThisType = relationFieldsWithoutDirective.filter(f => f.typeName == thisType.objectType.name)
        if (relationFieldsPointingToThisType.nonEmpty) Some(DeployErrors.relationDirectiveWithNameArgumentMustAppearTwice(thisType)) else None

      case _ =>
        None
    }

    /**
      * The validation below must be only applied to fields that specify the relation directive.
      * And it can only occur for relation that specify both sides of a relation.
      */
    val relationFieldsWithNonMatchingTypes = relationFieldsWithRelationDirective
      .groupBy(_.fieldDef.previousRelationName.get)
      .flatMap {
        case (_, fieldAndTypes) if fieldAndTypes.size > 1 =>
          val first  = fieldAndTypes.head
          val second = fieldAndTypes.last
          val firstError = if (first.fieldDef.typeName != second.objectType.name) {
            Option(DeployErrors.typesForOppositeRelationFieldsDoNotMatch(first, second))
          } else {
            None
          }
          val secondError = if (second.fieldDef.typeName != first.objectType.name) {
            Option(DeployErrors.typesForOppositeRelationFieldsDoNotMatch(second, first))
          } else {
            None
          }
          firstError ++ secondError
        case _ =>
          Iterable.empty
      }

    schemaErrors ++ relationFieldsWithNonMatchingTypes ++ allowOnlyOneDirectiveOnlyWhenUnambiguous
  }

  def partition[A, B, C](seq: Seq[A])(partitionFn: A => Either[B, C]): (Seq[B], Seq[C]) = {
    val mapped = seq.map(partitionFn)
    val lefts  = mapped.collect { case Left(x) => x }
    val rights = mapped.collect { case Right(x) => x }
    (lefts, rights)
  }
}
