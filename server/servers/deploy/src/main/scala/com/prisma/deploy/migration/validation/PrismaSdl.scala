package com.prisma.deploy.migration.validation

import com.prisma.deploy.migration.DirectiveTypes.RelationDBDirective
import com.prisma.gc_values.GCValue
import com.prisma.shared.models.OnDelete.OnDelete
import com.prisma.shared.models.{FieldBehaviour, RelationStrategy, TypeIdentifier}
import com.prisma.shared.models.TypeIdentifier.TypeIdentifier

case class PrismaSdl(
    typesFn: Vector[PrismaSdl => PrismaType],
    enumsFn: Vector[PrismaSdl => PrismaEnum]
) {
  private val types: Vector[PrismaType]  = typesFn.map(_.apply(this))
  val enums: Vector[PrismaEnum]          = enumsFn.map(_.apply(this))
  val relationTables: Vector[PrismaType] = types.filter(_.isRelationTable)
  val modelTypes: Vector[PrismaType]     = types.filter(!_.isRelationTable)

  def type_!(name: String)          = types.find(_.name == name).get
  def enum_!(name: String)          = enums.find(_.name == name).get
  def modelType_!(name: String)     = modelType(name).get
  def modelType(name: String)       = modelTypes.find(_.name == name)
  def relationTable_!(name: String) = relationTable(name).get
  def relationTable(name: String)   = relationTables.find(_.name == name)
}

case class PrismaType(
    name: String,
    tableName: Option[String],
    isEmbedded: Boolean,
    isRelationTable: Boolean,
    fieldFn: Vector[PrismaType => PrismaField],
    indexes: Vector[PrismaIndex]
)(val sdl: PrismaSdl) {
  val fields: Vector[PrismaField] = fieldFn.map(_.apply(this))

  val relationFields = fields.collect { case x: RelationalPrismaField => x }
  val nonRelationFields = fields.collect {
    case x: EnumPrismaField   => x
    case y: ScalarPrismaField => y
  }

  def finalTableName = tableName.getOrElse(name)

  def scalarField_!(name: String)   = field_!(name).asInstanceOf[ScalarPrismaField]
  def enumField_!(name: String)     = field_!(name).asInstanceOf[EnumPrismaField]
  def relationField_!(name: String) = field_!(name).asInstanceOf[RelationalPrismaField]
  def field_!(name: String)         = fields.find(_.name == name).get
  def isNotEmbedded                 = !isEmbedded
}

sealed trait PrismaField {
  def name: String
  def typeIdentifier: TypeIdentifier
  def isList: Boolean
  def isRequired: Boolean
  def tpe: PrismaType
}

case class PrismaIndex(fields: Vector[String], name: String)

case class ScalarPrismaField(
    name: String,
    columnName: Option[String],
    isList: Boolean,
    isRequired: Boolean,
    isUnique: Boolean,
    typeIdentifier: TypeIdentifier,
    defaultValue: Option[GCValue],
    behaviour: Option[FieldBehaviour],
    isHidden: Boolean = false
)(val tpe: PrismaType)
    extends PrismaField

case class EnumPrismaField(
    name: String,
    columnName: Option[String],
    isList: Boolean,
    isRequired: Boolean,
    isUnique: Boolean,
    enumName: String,
    defaultValue: Option[GCValue],
    behaviour: Option[FieldBehaviour]
)(val tpe: PrismaType)
    extends PrismaField {
  override def typeIdentifier: TypeIdentifier = TypeIdentifier.Enum
}

case class RelationalPrismaField(
    name: String,
    columnName: Option[String],
    relationDbDirective: Option[RelationDBDirective],
    strategy: Option[RelationStrategy],
    isList: Boolean,
    isRequired: Boolean,
    referencesType: String,
    relationName: Option[String],
    cascade: OnDelete
)(val tpe: PrismaType)
    extends PrismaField {
  def finalDbName = columnName.getOrElse(name)

  override def typeIdentifier: TypeIdentifier = TypeIdentifier.Relation

  def relatedField: Option[RelationalPrismaField] = {
    val otherFieldsOnOppositeModel = tpe.sdl.modelType_!(referencesType) match {
      case sameModel if sameModel.name == tpe.name => sameModel.relationFields.filter(_.referencesType == tpe.name).filter(_.name != name)
      case otherModel                              => otherModel.relationFields.filter(_.referencesType == tpe.name)
    }

    relationName match {
      case Some(relationName) => otherFieldsOnOppositeModel.find(field => field.relationName.contains(relationName))
      case None               => otherFieldsOnOppositeModel.headOption
    }
  }

  def relatedType: PrismaType = tpe.sdl.modelType_!(referencesType)

  def hasManyToManyRelation: Boolean = isList && relatedField.forall(_.isList)
  def hasOneToManyRelation: Boolean  = (isList && relatedField.forall(_.isOne)) || (isOne && relatedField.forall(_.isList))
  def hasOneToOneRelation: Boolean   = isOne && relatedField.exists(_.isOne)
  def isOne: Boolean                 = !isList
  def oneRelationField               = if (isOne) Some(this) else relatedField
}

case class PrismaEnum(name: String, values: Vector[String])(sdl: PrismaSdl)
