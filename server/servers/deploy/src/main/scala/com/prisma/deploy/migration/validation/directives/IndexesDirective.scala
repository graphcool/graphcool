package com.prisma.deploy.migration.validation.directives
import com.prisma.deploy.migration.validation.{DeployError, DeployErrors, PrismaIndex}
import com.prisma.shared.models.ConnectorCapabilities
import com.prisma.shared.models.ConnectorCapability.IndexesCapability
import com.prisma.deploy.migration.DataSchemaAstExtensions._
import sangria.ast.{Directive, Document, ObjectTypeDefinition, TypeDefinition}

object IndexesDirective extends TypeDirective[Vector[PrismaIndex]] {
  override def name: String = "indexes"

  override def validate(document: Document, typeDef: ObjectTypeDefinition, directive: Directive, capabilities: ConnectorCapabilities) = {
    val indexesAreSupported = capabilities.has(IndexesCapability)
    val errors  = (typeDef.hasIndexes && !indexesAreSupported).toOption(DeployErrors.indexesAreNotSupported(typeDef))

    val fieldsErrors = directive.argument("value").map(_.value) match {
      case Some(v: sangria.ast.ListValue) =>
        v.values.flatMap {
          case v: sangria.ast.ObjectValue => IndexDirective.validate(typeDef, v)
          case _ => Some(DeployError(typeDef, "The `value` argument is malformed."))
        }
      case _ => Vector(DeployError(typeDef, "The argument `value` is invalid in @indexes."))
    }

    errors.toVector ++ fieldsErrors
  }

  override def value(document: Document, typeDef: ObjectTypeDefinition, capabilities: ConnectorCapabilities) : Option[Vector[PrismaIndex]] = {
    val result = for {
      indexes <- typeDef.directive(name).toVector
      values  <- indexes.argument("value").map(_.value.asInstanceOf[sangria.ast.ListValue]).toVector
      value   <- values.values
      index   <- IndexDirective.value(value.asInstanceOf[sangria.ast.ObjectValue])
    } yield index

    Some(result)
  }
}

object IndexDirective extends SharedDirectiveValidation {
  val nameArg = DirectiveArgument("name", validateStringValue, _.asString)
  val fieldsArg = DirectiveArgument("fields", validateStringVectorValue, _.asStringVector)

  val requiredArgs = Vector(nameArg, fieldsArg)
  val optionalArgs = Vector.empty

  def validate(typeDef: ObjectTypeDefinition, value: sangria.ast.ObjectValue): Vector[DeployError] = {
    for {
      argumentRequirement <- requiredArgs
      deployError <- value.fieldsByName.get(argumentRequirement.name) match {
        case None =>
          Some(DeployErrors.indexDefinitionMissesRequiredArgument(typeDef, argumentRequirement.name))
        case Some(arg) =>
          argumentRequirement.validate(arg.value).map(e => DeployError(typeDef.name, argumentRequirement.name, e))
      }
    } yield deployError
  }


  def value(value: sangria.ast.ObjectValue): Option[PrismaIndex] = {
    for {
      nameVal <- value.fieldsByName.get("name")
      fieldsVal <- value.fieldsByName.get("fields")
      name <- Some(nameArg.value(nameVal))
      fields <- Some(fieldsArg.value(fieldsVal))
    } yield PrismaIndex(fields, name)
  }
}
