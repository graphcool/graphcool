package com.prisma.deploy.migration.validation

import sangria.ast.{FieldDefinition, ObjectTypeDefinition}

sealed trait DeployResult
case class DeployError(`type`: String, description: String, field: Option[String])   extends DeployResult
case class DeployWarning(`type`: String, description: String, field: Option[String]) extends DeployResult

object DeployError {
  def apply(`type`: String, field: String, description: String): DeployError = {
    DeployError(`type`, description, Some(field))
  }

  def apply(`type`: String, description: String): DeployError = {
    DeployError(`type`, description, None)
  }

  def apply(typeDef: ObjectTypeDefinition, description: String): DeployError = {
    DeployError(typeDef.name, description, None)
  }

  def apply(fieldAndType: FieldAndType, description: String): DeployError = {
    apply(fieldAndType.objectType, fieldAndType.fieldDef, description)
  }

  def apply(objectDef: ObjectTypeDefinition, fieldDef: FieldDefinition, description: String): DeployError = {
    apply(objectDef.name, fieldDef.name, description)
  }

  def global(description: String): DeployError = {
    DeployError("Global", description, None)
  }
}
