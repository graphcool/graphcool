package com.prisma.deploy.migration.validation

import com.prisma.deploy.connector.{FieldRequirement, FieldRequirementsInterface}
import com.prisma.deploy.gc_value.GCStringConverter
import com.prisma.deploy.validation._
import com.prisma.shared.models.ConnectorCapability.{MigrationsCapability, ScalarListsCapability}
import com.prisma.shared.models.{ConnectorCapabilities, ConnectorCapability, RelationStrategy, TypeIdentifier}
import com.prisma.utils.or.OrExtensions
import org.scalactic.{Bad, Good, Or}
import sangria.ast.{Argument => _, _}

import scala.collection.immutable.Seq
import scala.util.{Failure, Success}
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

case class DirectiveRequirement(directiveName: String, requiredArguments: Seq[RequiredArg], optionalArguments: Seq[Argument])
case class RequiredArg(name: String, mustBeAString: Boolean)
case class Argument(name: String, isValid: sangria.ast.Value => Boolean)

case class FieldAndType(objectType: ObjectTypeDefinition, fieldDef: FieldDefinition) {
  import com.prisma.deploy.migration.DataSchemaAstExtensions._

  def isSelfRelation: Boolean = fieldDef.typeName == objectType.name

  def relationCount(doc: Document): Int = {
    def fieldsWithType(objectType: ObjectTypeDefinition, typeName: String): Seq[FieldDefinition] = objectType.fields.filter(_.typeName == typeName)

    val oppositeObjectType = doc.objectType_!(fieldDef.typeName)
    val fieldsOnTypeA      = fieldsWithType(objectType, fieldDef.typeName)
    val fieldsOnTypeB      = fieldsWithType(oppositeObjectType, objectType.name)

    isSelfRelation match {
      case true  => fieldsOnTypeB.count(_.relationName == fieldDef.relationName)
      case false => (fieldsOnTypeA ++ fieldsOnTypeB).count(_.relationName == fieldDef.relationName)
    }
  }
}

object FieldRequirementHelper {
  implicit class FieldRequirementExtensions(req: FieldRequirement) {
    import com.prisma.deploy.migration.DataSchemaAstExtensions._

    def isValid(field: FieldDefinition): Boolean = field.name == req.name match {
      case true  => req.validTypes.contains(field.typeName) && field.isRequired == req.required && field.isUnique == req.unique && field.isList == req.list
      case false => true
    }
  }
}

object LegacyDataModelValidator extends DataModelValidator {

  def validOnDeleteEnum(x: sangria.ast.Value): Boolean = {
    val enum = x.isInstanceOf[EnumValue]

    val valid = x.renderPretty match {
      case "CASCADE"  => true
      case "SET_NULL" => true
      case _          => false
    }
    enum && valid
  }

  def validOldName(x: sangria.ast.Value): Boolean = x.isInstanceOf[StringValue]

  val directiveRequirements = Seq(
    DirectiveRequirement(
      "relation",
      requiredArguments = Seq(RequiredArg("name", mustBeAString = true)),
      optionalArguments = Seq(Argument("onDelete", validOnDeleteEnum), Argument("oldName", validOldName))
    ),
    DirectiveRequirement("rename", requiredArguments = Seq(RequiredArg("oldName", mustBeAString = true)), optionalArguments = Seq.empty),
    DirectiveRequirement("default", requiredArguments = Seq(RequiredArg("value", mustBeAString = false)), optionalArguments = Seq.empty),
    DirectiveRequirement("unique", requiredArguments = Seq.empty, optionalArguments = Seq.empty),
    DirectiveRequirement("embedded", requiredArguments = Seq.empty, optionalArguments = Seq.empty)
  )

  def apply(schema: String, fieldRequirements: FieldRequirementsInterface, capabilities: ConnectorCapabilities): LegacyDataModelValidator = {
    LegacyDataModelValidator(
      schema = schema,
      directiveRequirements = directiveRequirements,
      fieldRequirements = fieldRequirements,
      capabilities = capabilities
    )
  }

  override def validate(dataModel: String, fieldRequirements: FieldRequirementsInterface, capabilities: ConnectorCapabilities) = {
    apply(dataModel, fieldRequirements, capabilities).validateSyntax
  }
}

case class LegacyDataModelValidator(
    schema: String,
    directiveRequirements: Seq[DirectiveRequirement],
    fieldRequirements: FieldRequirementsInterface,
    capabilities: ConnectorCapabilities
) {
  import com.prisma.deploy.migration.DataSchemaAstExtensions._

  val result   = GraphQlSdlParser.parse(schema)
  lazy val doc = result.get

  def validateSyntax: PrismaSdl Or Vector[DeployError] = {
    val errors = validate
    if (errors.isEmpty) {
      Good(generateSDL)
    } else {
      Bad(errors.toVector)
    }
  }

  def validate: Seq[DeployError] = result match {
    case Success(_) => validateInternal
    case Failure(e) => List(DeployError.global(s"There's a syntax error in the Schema Definition. ${e.getMessage}"))
  }

  def generateSDL: PrismaSdl = {

    val enumTypes: Vector[PrismaSdl => PrismaEnum] = doc.enumNames.map { name =>
      val definition: EnumTypeDefinition = doc.enumType(name).get
      val enumValues                     = definition.values.map(_.name)
      PrismaEnum(name, values = enumValues)(_)
    }

    val prismaTypes: Vector[PrismaSdl => PrismaType] = doc.objectTypes.map { definition =>
      val prismaFields = definition.fields.map {
        case x if isRelationField(x) =>
          RelationalPrismaField(
            name = x.name,
            columnName = None,
            relationDbDirective = x.relationDBDirective,
            strategy = None,
            isList = x.isList,
            isRequired = x.isRequired,
            referencesType = x.typeName,
            relationName = x.relationName,
            cascade = x.onDelete
          )(_)

        case x if isEnumField(x) =>
          EnumPrismaField(
            name = x.name,
            columnName = x.columnName,
            isList = x.isList,
            isRequired = x.isRequired,
            isUnique = x.isUnique,
            enumName = x.typeName,
            defaultValue = getDefaultValueFromField_!(x),
            behaviour = None
          )(_)

        case x if isScalarField(x) =>
          ScalarPrismaField(
            name = x.name,
            columnName = x.columnName,
            isList = x.isList,
            isRequired = x.isRequired,
            isUnique = x.isUnique,
            typeIdentifier = typeIdentifierForTypename(x.fieldType),
            defaultValue = getDefaultValueFromField_!(x),
            behaviour = None
          )(_)
      }

      PrismaType(
        name = definition.name,
        tableName = definition.tableNameDirective,
        isEmbedded = definition.isEmbedded,
        isRelationTable = false,
        fieldFn = prismaFields,
        indexes = Vector()
      )(_)
    }

    PrismaSdl(typesFn = prismaTypes, enumsFn = enumTypes)
  }

  def validateInternal: Seq[DeployError] = {

    val allFieldAndTypes: Seq[FieldAndType] = for {
      objectType <- doc.objectTypes
      field      <- objectType.fields
    } yield FieldAndType(objectType, field)

    val reservedFieldsValidations = tryValidation(validateReservedFields(allFieldAndTypes))
    val requiredFieldValidations  = tryValidation(validateRequiredReservedFields(doc.objectTypes))
    val duplicateTypeValidations  = tryValidation(validateDuplicateTypes(doc.objectTypes, allFieldAndTypes))
    val duplicateFieldValidations = tryValidation(validateDuplicateFields(allFieldAndTypes))
    val missingTypeValidations    = tryValidation(validateMissingTypes(allFieldAndTypes))
    val relationFieldValidations  = tryValidation(validateRelationFields(allFieldAndTypes))
    val scalarFieldValidations    = tryValidation(validateScalarFields(allFieldAndTypes))
    val fieldDirectiveValidations = tryValidation(allFieldAndTypes.flatMap(validateFieldDirectives))
    val enumValidations           = tryValidation(validateEnumTypes)

    val allValidations = Vector(
      reservedFieldsValidations,
      requiredFieldValidations,
      duplicateFieldValidations,
      duplicateTypeValidations,
      missingTypeValidations,
      relationFieldValidations,
      scalarFieldValidations,
      fieldDirectiveValidations,
      enumValidations
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

  def validateReservedFields(fieldAndTypes: Seq[FieldAndType]): Seq[DeployError] = {
    for {
      field        <- fieldAndTypes
      failedChecks = fieldRequirements.reservedFieldRequirements.filterNot { FieldRequirementHelper.FieldRequirementExtensions(_).isValid(field.fieldDef) }
      if failedChecks.nonEmpty
    } yield DeployErrors.malformedReservedField(field, failedChecks.head)
  }

  def validateRequiredReservedFields(objectTypes: Seq[ObjectTypeDefinition]): Seq[DeployError] = {
    for {
      objectType   <- objectTypes
      fieldNames   = objectType.fields.map(_.name)
      failedChecks = fieldRequirements.requiredReservedFields.filterNot(req => fieldNames.contains(req.name))
      if failedChecks.nonEmpty
    } yield DeployErrors.missingReservedField(objectType, failedChecks.head.name, failedChecks.head)
  }

  def validateDuplicateTypes(objectTypes: Seq[ObjectTypeDefinition], fieldAndTypes: Seq[FieldAndType]): Seq[DeployError] = {
    val typeNames          = objectTypes.map(_.name.toLowerCase)
    val duplicateTypeNames = typeNames.filter(name => typeNames.count(_ == name) > 1)

    for {
      duplicateTypeName <- duplicateTypeNames
      objectType        <- objectTypes.find(_.name.equalsIgnoreCase(duplicateTypeName))
    } yield {
      DeployErrors.duplicateTypeName(objectType)
    }
  }

  def validateDuplicateFields(fieldAndTypes: Seq[FieldAndType]): Seq[DeployError] = {
    for {
      objectType <- fieldAndTypes.map(_.objectType).distinct
      fieldNames = objectType.fields.map(_.name.toLowerCase)
      fieldName  <- fieldNames
      if fieldNames.count(_ == fieldName) > 1
    } yield {
      DeployErrors.duplicateFieldName(fieldAndTypes.find(ft => ft.objectType == objectType & ft.fieldDef.name.equalsIgnoreCase(fieldName)).get)
    }
  }

  def validateMissingTypes(fieldAndTypes: Seq[FieldAndType]): Seq[DeployError] = {
    fieldAndTypes
      .filter(!isScalarField(_))
      .collect { case fieldAndType if !doc.isObjectOrEnumType(fieldAndType.fieldDef.typeName) => DeployErrors.missingType(fieldAndType) }
  }

  def validateRelationFields(fieldAndTypes: Seq[FieldAndType]): Seq[DeployError] = {
    def ambiguousRelationFieldsForType(objectType: ObjectTypeDefinition): Vector[FieldAndType] = {
      val relationFields                                = objectType.fields.filter(isRelationField)
      val grouped: Map[String, Vector[FieldDefinition]] = relationFields.groupBy(_.typeName)
      val ambiguousFields                               = grouped.values.filter(_.size > 1).flatten.toVector
      ambiguousFields.map(field => FieldAndType(objectType, field))
    }

    val ambiguousRelationFields = doc.objectTypes.flatMap(ambiguousRelationFieldsForType)

    val (schemaErrors, _) = partition(ambiguousRelationFields) {
      case fieldAndType if !fieldAndType.fieldDef.hasRelationDirectiveWithNameArg =>
        Left(DeployErrors.missingRelationDirective(fieldAndType))

      case fieldAndType if !isSelfRelation(fieldAndType) && relationCount(fieldAndType) > 2 =>
        Left(DeployErrors.relationDirectiveCannotAppearMoreThanTwice(fieldAndType))

      case fieldAndType if isSelfRelation(fieldAndType) && relationCount(fieldAndType) != 1 && relationCount(fieldAndType) != 2 =>
        Left(DeployErrors.selfRelationMustAppearOneOrTwoTimes(fieldAndType))

      case fieldAndType =>
        Right(fieldAndType)
    }

    val relationFieldsWithRelationDirective = for {
      objectType <- doc.objectTypes
      field      <- objectType.fields
      if field.hasRelationDirectiveWithNameArg
      if isRelationField(field)
    } yield FieldAndType(objectType, field)

    /**
      * Check that if a relation is not a self-relation and a relation-directive occurs only once that there is no
      * opposing field without a relationdirective on the other side.
      */
    val allowOnlyOneDirectiveOnlyWhenUnambigous = relationFieldsWithRelationDirective.flatMap {
      case thisType if !isSelfRelation(thisType) && relationCount(thisType) == 1 =>
        val oppositeObjectType               = doc.objectType_!(thisType.fieldDef.typeName)
        val fieldsOnOppositeObjectType       = oppositeObjectType.fields.filter(_.typeName == thisType.objectType.name)
        val relationFieldsWithoutDirective   = fieldsOnOppositeObjectType.filter(f => !f.hasRelationDirectiveWithNameArg && isRelationField(f))
        val relationFieldsPointingToThisType = relationFieldsWithoutDirective.filter(f => f.typeName == thisType.objectType.name)
        if (relationFieldsPointingToThisType.nonEmpty) Some(DeployErrors.ambiguousRelationSinceThereIsOnlyOneRelationDirective(thisType)) else None

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

    schemaErrors ++ relationFieldsWithNonMatchingTypes ++ allowOnlyOneDirectiveOnlyWhenUnambigous
  }

  def validateScalarFields(fieldAndTypes: Seq[FieldAndType]): Seq[DeployError] = {
    val scalarFields = fieldAndTypes.filter(isScalarField)
    if (capabilities.supportsScalarLists) {
      scalarFields.collect {
        case fieldAndType if !fieldAndType.fieldDef.isValidScalarType => DeployErrors.invalidScalarListOrNonListType(fieldAndType)
      }
    } else {
      scalarFields.collect {
        case fieldAndType if !fieldAndType.fieldDef.isValidScalarNonListType => DeployErrors.invalidScalarNonListType(fieldAndType)
      }
    }
  }

  def validateFieldDirectives(fieldAndType: FieldAndType): Seq[DeployError] = {
    def validateDirectiveRequirements(directive: Directive): Seq[DeployError] = {
      val optionalArgs = for {
        requirement         <- directiveRequirements if requirement.directiveName == directive.name
        argumentRequirement <- requirement.optionalArguments
        argument            <- directive.argument(argumentRequirement.name)
        schemaError <- if (argumentRequirement.isValid(argument.value)) {
                        None
                      } else {
                        Some(DeployError(fieldAndType, s"not a valid value for ${argumentRequirement.name}"))
                      }
      } yield schemaError

      val requiredArgs = for {
        requirement <- directiveRequirements if requirement.directiveName == directive.name
        requiredArg <- requirement.requiredArguments
        schemaError <- if (!directive.containsArgument(requiredArg.name, requiredArg.mustBeAString)) {
                        Some(DeployErrors.directiveMissesRequiredArgument(fieldAndType, requirement.directiveName, requiredArg.name))
                      } else {
                        None
                      }
      } yield schemaError

      requiredArgs ++ optionalArgs
    }

    def ensureDirectivesAreUnique(fieldAndType: FieldAndType): Option[DeployError] = {
      val directives       = fieldAndType.fieldDef.directives
      val uniqueDirectives = directives.map(_.name).toSet
      if (uniqueDirectives.size != directives.size) {
        Some(DeployErrors.directivesMustAppearExactlyOnce(fieldAndType))
      } else {
        None
      }
    }

    def ensureRelationDirectivesArePlacedCorrectly(fieldAndType: FieldAndType): Option[DeployError] = {
      if (!isRelationField(fieldAndType.fieldDef) && fieldAndType.fieldDef.hasRelationDirectiveWithNameArg) {
        Some(DeployErrors.relationDirectiveNotAllowedOnScalarFields(fieldAndType))
      } else {
        None
      }
    }

    def ensureNoOldDefaultValueDirectives(fieldAndTypes: FieldAndType): Option[DeployError] = {
      if (fieldAndType.fieldDef.hasOldDefaultValueDirective) {
        Some(DeployErrors.invalidSyntaxForDefaultValue(fieldAndType))
      } else {
        None
      }
    }

    def ensureNoDefaultValuesOnListFields(fieldAndTypes: FieldAndType): Option[DeployError] = {
      if (fieldAndType.fieldDef.isList && fieldAndType.fieldDef.hasDefaultValueDirective) {
        Some(DeployErrors.listFieldsCantHaveDefaultValues(fieldAndType))
      } else {
        None
      }
    }

    def ensureNoInvalidEnumValuesInDefaultValues(fieldAndTypes: FieldAndType): Option[DeployError] = {
      def containsInvalidEnumValue: Boolean = {
        val enumValues = doc.enumType(fieldAndType.fieldDef.typeName).get.values.map(_.name)
        !enumValues.contains(fieldAndType.fieldDef.directiveArgumentAsString("default", "value").get)
      }

      fieldAndType.fieldDef.hasDefaultValueDirective && isEnumField(fieldAndType.fieldDef) match {
        case true if containsInvalidEnumValue => Some(DeployErrors.invalidEnumValueInDefaultValue(fieldAndType))
        case _                                => None
      }
    }

    def ensureDefaultValuesHaveCorrectType(fieldAndTypes: FieldAndType): Option[DeployError] = {
      def hasInvalidType: Boolean = {
        getDefaultValueFromField(fieldAndType.fieldDef) match {
          case Some(Good(_)) => false
          case None          => false
          case Some(Bad(_))  => true
        }
      }

      def isNotList = !fieldAndTypes.fieldDef.isList

      fieldAndType.fieldDef.hasDefaultValueDirective match {
        case true if isNotList && hasInvalidType => Some(DeployErrors.invalidTypeForDefaultValue(fieldAndType))
        case _                                   => None
      }
    }

    fieldAndType.fieldDef.directives.flatMap(validateDirectiveRequirements) ++
      ensureDirectivesAreUnique(fieldAndType) ++
      ensureNoOldDefaultValueDirectives(fieldAndType) ++
      ensureRelationDirectivesArePlacedCorrectly(fieldAndType) ++
      ensureNoDefaultValuesOnListFields(fieldAndType) ++
      ensureNoInvalidEnumValuesInDefaultValues(fieldAndType) ++
      ensureDefaultValuesHaveCorrectType(fieldAndType)
  }

  def getDefaultValueFromField(fieldDef: FieldDefinition) = {
    val defaultValue   = fieldDef.directiveArgumentAsString("default", "value")
    val typeIdentifier = typeIdentifierForTypename(fieldDef.fieldType)
    defaultValue.map(value => GCStringConverter(typeIdentifier, fieldDef.isList).toGCValue(value))
  }

  def getDefaultValueFromField_!(fieldDef: FieldDefinition) = getDefaultValueFromField(fieldDef).map(_.get)

  def validateEnumTypes: Seq[DeployError] = {
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

  def relationCount(fieldAndType: FieldAndType): Int = {
    def fieldsWithType(objectType: ObjectTypeDefinition, typeName: String): Seq[FieldDefinition] = objectType.fields.filter(_.typeName == typeName)

    val oppositeObjectType = doc.objectType_!(fieldAndType.fieldDef.typeName)
    val fieldsOnTypeA      = fieldsWithType(fieldAndType.objectType, fieldAndType.fieldDef.typeName)
    val fieldsOnTypeB      = fieldsWithType(oppositeObjectType, fieldAndType.objectType.name)

    isSelfRelation(fieldAndType) match {
      case true  => fieldsOnTypeB.count(_.relationName == fieldAndType.fieldDef.relationName)
      case false => (fieldsOnTypeA ++ fieldsOnTypeB).count(_.relationName == fieldAndType.fieldDef.relationName)
    }
  }

  def isSelfRelation(fieldAndType: FieldAndType): Boolean  = fieldAndType.fieldDef.typeName == fieldAndType.objectType.name
  def isRelationField(fieldAndType: FieldAndType): Boolean = isRelationField(fieldAndType.fieldDef)
  def isRelationField(fieldDef: FieldDefinition): Boolean  = !isScalarField(fieldDef) && !isEnumField(fieldDef)

  def isScalarField(fieldAndType: FieldAndType): Boolean = isScalarField(fieldAndType.fieldDef)
  def isScalarField(fieldDef: FieldDefinition): Boolean  = fieldDef.hasScalarType

  def isEnumField(fieldDef: FieldDefinition): Boolean = doc.enumType(fieldDef.typeName).isDefined

  def partition[A, B, C](seq: Seq[A])(partitionFn: A => Either[B, C]): (Seq[B], Seq[C]) = {
    val mapped = seq.map(partitionFn)
    val lefts  = mapped.collect { case Left(x) => x }
    val rights = mapped.collect { case Right(x) => x }
    (lefts, rights)
  }

  def typeIdentifierForTypename(fieldType: Type): TypeIdentifier.Value = {
    val typeName = fieldType.namedType.name

    if (doc.objectType(typeName).isDefined) {
      TypeIdentifier.Relation
    } else if (doc.enumType(typeName).isDefined) {
      TypeIdentifier.Enum
    } else {
      TypeIdentifier.withName(typeName)
    }
  }
}
