package com.prisma.deploy.migration.validation

import com.prisma.deploy.connector.FieldRequirement
import sangria.ast.{EnumTypeDefinition, FieldDefinition, ObjectTypeDefinition, TypeDefinition}

object DeployWarnings {
  def dataLossModel(`type`: String): DeployWarning = {
    DeployWarning(`type`, "You already have nodes for this model. This change will result in data loss.", None)
  }

  def dataLossRelation(`type`: String): DeployWarning = {
    DeployWarning(`type`, "You already have nodes for this relation. This change will result in data loss.", None)
  }

  def dataLossField(`type`: String, field: String): DeployWarning = {
    DeployWarning(`type`, "You already have nodes for this model. This change may result in data loss.", Some(field))
  }
}

object DeployErrors {
  def indexesAreNotSupported(objectType: ObjectTypeDefinition): DeployError = {
    error(objectType, "Indexes are not supported for the connector.")
  }

  def indexDefinitionMissesRequiredArgument(typeDefinition: TypeDefinition, field: String): DeployError = {
    error(typeDefinition, s"An index definition in `$typeDefinition` misses a required field `$field`.")
  }

  import com.prisma.deploy.migration.DataSchemaAstExtensions._

  def missingIdField(typeDefinition: TypeDefinition): DeployError = {
    error(typeDefinition, "All models must specify the `id` field: `id: ID! @unique`")
  }

  def missingUniqueDirective(fieldAndType: FieldAndType): DeployError = {
    error(fieldAndType, s"""All id fields must specify the `@unique` directive.""")
  }

  def uniqueDisallowedOnEmbeddedTyps(objectType: ObjectTypeDefinition, fieldDef: FieldDefinition): DeployError = {
    error(objectType, fieldDef, s"The field `${fieldDef.name}` is marked as unique but its type `${objectType.name}` is embedded. This is disallowed.")
  }

  def missingRelationDirective(fieldAndType: FieldAndType): DeployError = {
    error(fieldAndType, s"""The relation field `${fieldAndType.fieldDef.name}` must specify a `@relation` directive: `@relation(name: "MyRelation")`""")
  }

  def missingRelationStrategy(relationField: RelationalPrismaField, validModes: Vector[String]): DeployError = {
    DeployError(
      relationField.tpe.name,
      relationField.name,
      s"The field `${relationField.name}` must provide a relation link mode. Either specify it on this field or the opposite field. Valid values are: ${validModes
        .mkString(",")}"
    )
  }
  def moreThanOneRelationStrategy(relationField: RelationalPrismaField): DeployError = {
    DeployError(
      relationField.tpe.name,
      relationField.name,
      s"The `link` argument must be specified only on one side of a relation. The field `${relationField.name}` provides a link mode and the opposite field `${relationField.relatedField.get.name}` as well.}"
    )
  }

  def missingBackRelationField(tpe: PrismaType, relationField: RelationalPrismaField): DeployError = {
    DeployError(
      tpe.name,
      s"The type `${tpe.name}` does not specify a back relation field. It is referenced from the type `${relationField.tpe.name}` in the field `${relationField.name}`."
    )
  }

  def disallowedBackRelationFieldOnEmbeddedType(relationField: RelationalPrismaField): DeployError = {
    DeployError(
      relationField.tpe.name,
      relationField.name,
      s"The type `${relationField.tpe.name}` specifies the back relation field `${relationField.name}`, which is disallowed for embedded types."
    )
  }

  def relationDirectiveNotAllowedOnScalarFields(fieldAndType: FieldAndType): DeployError = {
    error(fieldAndType, s"""The field `${fieldAndType.fieldDef.name}` is a scalar field and cannot specify the `@relation` directive.""")
  }

  def ambiguousRelationSinceThereIsOnlyOneRelationDirective(fieldAndType: FieldAndType): DeployError = {
    val relationName = fieldAndType.fieldDef.previousRelationName.get
    val nameA        = fieldAndType.objectType.name
    val nameB        = fieldAndType.fieldDef.fieldType.namedType.name
    error(
      fieldAndType,
      s"You are trying to set the relation '$relationName' from `$nameA` to `$nameB` and are only providing a relation directive on `$nameA`. " +
        s"Since there is also a relation field without a relation directive on `$nameB` pointing towards `$nameA` that is ambiguous. " +
        s"Please provide the same relation directive on `$nameB` if this is supposed to be the same relation. " +
        s"If you meant to create two separate relations without backrelations please provide a relation directive with a different name on `$nameB`."
    )
  }

  def relationDirectiveWithNameArgumentMustAppearTwice(fieldAndType: FieldAndType): DeployError = {
    val relationName = fieldAndType.fieldDef.previousRelationName.get
    val nameA        = fieldAndType.objectType.name
    val nameB        = fieldAndType.fieldDef.fieldType.namedType.name
    error(
      fieldAndType,
      s"You are trying to set the relation '$relationName' from `$nameA` to `$nameB` and are only providing a relation directive with a name on `$nameA`. " +
        s"Please also provide the same named relation directive on the relation field on `$nameB` pointing towards `$nameA`. "
    )
  }

  def relationDirectiveCannotAppearMoreThanTwice(fieldAndType: FieldAndType): DeployError = {
    val relationName = fieldAndType.fieldDef.previousRelationName.get
    error(fieldAndType, s"A relation directive cannot appear more than twice. Relation name: '$relationName'")
  }

  def selfRelationMustAppearOneOrTwoTimes(fieldAndType: FieldAndType): DeployError = {
    val relationName = fieldAndType.fieldDef.previousRelationName.get
    error(fieldAndType, s"A relation directive for a self relation must appear either 1 or 2 times. Relation name: '$relationName'")
  }

  def typesForOppositeRelationFieldsDoNotMatch(fieldAndType: FieldAndType, other: FieldAndType): DeployError = {
    error(
      fieldAndType,
      s"The relation field `${fieldAndType.fieldDef.name}` has the type `${fieldAndType.fieldDef.typeString}`. But the other directive for this relation appeared on the type `${other.objectType.name}`"
    )
  }

  def missingType(fieldAndType: FieldAndType): DeployError = missingType(fieldAndType.objectType, fieldAndType.fieldDef)
  def missingType(objectType: ObjectTypeDefinition, fieldDef: FieldDefinition): DeployError = {
    error(
      objectType,
      fieldDef,
      s"The field `${fieldDef.name}` has the type `${fieldDef.typeString}` but there's no type or enum declaration with that name."
    )
  }

  def malformedReservedField(fieldAndType: FieldAndType, requirement: FieldRequirement): DeployError = {
    error(
      fieldAndType,
      s"The field `${fieldAndType.fieldDef.name}` is reserved and has to have the format: ${requirementMessage(requirement)}."
    )
  }

  def missingReservedField(objectType: ObjectTypeDefinition, fieldName: String, requirement: FieldRequirement): DeployError = {
    DeployError(
      objectType.name,
      fieldName,
      s"The required field `$fieldName` is missing and has to have the format: ${requirementMessage(requirement)}."
    )
  }

  // Brain kaputt, todo find a better solution
  def requirementMessage(requirement: FieldRequirement): String = {
    val requiredTypeMessages = requirement.validTypes.map { typeName =>
      requirement match {
        case x @ FieldRequirement(name, _, true, false, false)  => s"$name: $typeName!"
        case x @ FieldRequirement(name, _, true, true, false)   => s"$name: $typeName! @unique"
        case x @ FieldRequirement(name, _, true, true, true)    => s"$name: [$typeName!]! @unique" // is that even possible? Prob. not.
        case x @ FieldRequirement(name, _, true, false, true)   => s"$name: [$typeName!]!"
        case x @ FieldRequirement(name, _, false, true, false)  => s"$name: $typeName @unique"
        case x @ FieldRequirement(name, _, false, true, true)   => s"$name: [$typeName!] @unique"
        case x @ FieldRequirement(name, _, false, false, true)  => s"$name: [$typeName!]"
        case x @ FieldRequirement(name, _, false, false, false) => s"$name: $typeName"
      }
    }
    requiredTypeMessages.mkString(" or ")
  }

  def atNodeIsDeprecated(fieldAndType: FieldAndType) = {
    error(
      fieldAndType,
      s"The model `${fieldAndType.objectType.name}` has the implements Node annotation. This is deprecated, please do not use an annotation."
    )
  }

  def duplicateFieldName(fieldAndType: FieldAndType) = {
    error(
      fieldAndType,
      s"The type `${fieldAndType.objectType.name}` has a duplicate fieldName. The detection of duplicates is performed case insensitive. "
    )
  }

  def duplicateTypeName(objectTypeDefinition: ObjectTypeDefinition) = {
    error(
      objectTypeDefinition,
      s"The name of the type `${objectTypeDefinition.name}` occurs more than once. The detection of duplicates is performed case insensitive."
    )
  }

  def directiveMissesRequiredArgument(fieldAndType: FieldAndType, directive: String, argument: String) = {
    error(
      fieldAndType,
      s"The field `${fieldAndType.fieldDef.name}` specifies the directive `@$directive` but it's missing the required argument `$argument`."
    )
  }

  def directivesMustAppearExactlyOnce(fieldAndType: FieldAndType) = {
    error(fieldAndType, s"The field `${fieldAndType.fieldDef.name}` specifies a directive more than once. Directives must appear exactly once on a field.")
  }

  def directivesMustAppearExactlyOnce(objectType: ObjectTypeDefinition) = {
    error(objectType, s"The type `${objectType.name}` specifies a directive more than once. Directives must appear exactly once on a type.")
  }

  def manyRelationFieldsMustBeRequired(fieldAndType: FieldAndType) = {
    error(fieldAndType, s"Many relation fields must be marked as required.")
  }

  def listFieldsCantHaveDefaultValues(fieldAndType: FieldAndType) = {
    error(fieldAndType, s"List fields cannot have defaultValues.")
  }

  def invalidEnumValueInDefaultValue(fieldAndType: FieldAndType) = {
    error(fieldAndType, s"The defaultValue contains an invalid enumValue.")
  }

  def invalidTypeForDefaultValue(fieldAndType: FieldAndType) = {
    error(
      fieldAndType,
      s"Invalid value '${fieldAndType.fieldDef.directiveArgumentAsString("default", "value").get}' for type ${fieldAndType.fieldDef.fieldType.namedType.name}."
    )
  }

  def invalidSyntaxForDefaultValue(fieldAndType: FieldAndType) = {
    error(fieldAndType, s"""You are using a '@defaultValue' directive. Prisma uses '@default(value: "Value as String")' to declare default values.""")
  }

  def invalidScalarNonListType(fieldAndType: FieldAndType)       = invalidScalarType(fieldAndType, listTypesAllowed = false)
  def invalidScalarListOrNonListType(fieldAndType: FieldAndType) = invalidScalarType(fieldAndType, listTypesAllowed = true)

  private def invalidScalarType(fieldAndType: FieldAndType, listTypesAllowed: Boolean): DeployError = {
    val scalarType      = fieldAndType.fieldDef.fieldType.namedType.name
    val nonListFormats  = s"`$scalarType`, `$scalarType!`"
    val listFormats     = s", `[$scalarType!]` or `[$scalarType!]!"
    val possibleFormats = if (listTypesAllowed) nonListFormats + listFormats else nonListFormats
    error(
      fieldAndType,
      s"""The scalar field `${fieldAndType.fieldDef.name}` has the wrong format: `${fieldAndType.fieldDef.typeString}` Possible Formats: $possibleFormats"""
    )
  }

  def enumNamesMustBeUnique(enumType: EnumTypeDefinition) = {
    error(enumType, s"The enum type `${enumType.name}` is defined twice in the schema. Enum names must be unique.")
  }

  def enumValuesMustBeginUppercase(enumType: EnumTypeDefinition) = {
    error(enumType, s"The enum type `${enumType.name}` contains invalid enum values. The first character of each value must be an uppercase letter.")
  }

  def enumValuesMustBeValid(enumType: EnumTypeDefinition, enumValues: Seq[String]) = {
    error(enumType, s"The enum type `${enumType.name}` contains invalid enum values. Those are invalid: ${enumValues.map(v => s"`$v`").mkString(", ")}.")
  }

  def systemFieldCannotBeRemoved(theType: String, field: String) = {
    DeployError(theType, field, s"The field `$field` is a system field and cannot be removed.")
  }

  def schemaFileHeaderIsMissing() = {
    DeployError.global(s"""The schema must specify the project id and version as a front matter, e.g.:
                          |# projectId: your-project-id
                          |# version: 3
                          |type MyType {
                          |  myfield: String!
                          |}
       """.stripMargin)
  }

  def schemaFileHeaderIsReferencingWrongVersion(expected: Int) = {
    DeployError.global(s"The schema is referencing the wrong project version. Expected version $expected.")
  }

  def embeddedTypesAreNotSupported(typeName: String) = {
    DeployError(typeName, s"The type `$typeName` is marked as embedded but this connector does not support embedded types.")
  }

  def embeddedTypesMustNotSpecifyDbName(typeName: String) = {
    DeployError(typeName, s"The type `$typeName` is specifies the `@db` directive. Embedded types must not specify this directive.")
  }

  def relationFieldsMustNotSpecifyDbName(typeDef: ObjectTypeDefinition, fieldDef: FieldDefinition) = {
    DeployError(typeDef, fieldDef, s"The field `${fieldDef.name}` specifies the `@db` directive. Relation fields must not specify this directive.")
  }

  def sequenceDirectiveMisplaced(typeDef: ObjectTypeDefinition, fieldDef: FieldDefinition) = {
    DeployError(
      typeDef,
      fieldDef,
      s"The directive `@sequence` must only be specified for fields that are marked as id, are of type `Int` and use the sequence strategy. E.g. `id: Int! @id(strategy: SEQUENCE)`."
    )
  }

  def error(fieldAndType: FieldAndType, description: String): DeployError = {
    error(fieldAndType.objectType, fieldAndType.fieldDef, description)
  }

  def error(objectType: ObjectTypeDefinition, fieldDef: FieldDefinition, description: String): DeployError = {
    DeployError(objectType.name, fieldDef.name, description)
  }

  def error(typeDef: TypeDefinition, description: String): DeployError = {
    DeployError(typeDef.name, description)
  }

  // note: the cli relies on the string "destructive changes" being present in this error message. Ugly but effective
  def forceArgumentRequired: DeployError = {
    DeployError.global(
      "Your migration includes potentially destructive changes. Review using `graphcool deploy --dry-run` and continue using `graphcool deploy --force`.")
  }

  def invalidEnv(message: String) = {
    DeployError.global(s"""the environment file is invalid: $message""")
  }
}
