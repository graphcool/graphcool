package com.prisma.deploy.connector.mysql.database

import com.prisma.deploy.connector.{FieldRequirement, FieldRequirementsInterface}
import com.prisma.shared.models.FieldTemplate

case class MySqlFieldRequirement(
    isActive: Boolean
) extends FieldRequirementsInterface {
  val idFieldRequirementForPassiveConnectors = Vector(FieldRequirement("id", Vector("ID", "UUID"), required = true, unique = true, list = false))
  val idFieldRequirementForActiveConnectors  = Vector(FieldRequirement("id", Vector("ID", "UUID"), required = true, unique = true, list = false))

  val baseReservedFieldsRequirements = Vector(
    FieldRequirement("updatedAt", "DateTime", required = true, unique = false, list = false),
    FieldRequirement("createdAt", "DateTime", required = true, unique = false, list = false)
  )

  val reservedFieldsRequirementsForActiveConnectors  = baseReservedFieldsRequirements ++ idFieldRequirementForActiveConnectors
  val reservedFieldsRequirementsForPassiveConnectors = baseReservedFieldsRequirements ++ idFieldRequirementForPassiveConnectors

  val reservedFieldRequirements: Vector[FieldRequirement] =
    if (isActive) reservedFieldsRequirementsForActiveConnectors else reservedFieldsRequirementsForPassiveConnectors
  val requiredReservedFields: Vector[FieldRequirement] = Vector.empty
  val hiddenReservedField: Vector[FieldTemplate]       = Vector.empty
  val isAutogenerated: Boolean                         = false
}
