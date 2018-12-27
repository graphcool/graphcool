package com.prisma.shared.models

import com.prisma.shared.models.Manifestations.{FieldManifestation, ModelManifestation}

import scala.language.implicitConversions

case class ModelTemplate(
    name: String,
    stableIdentifier: String,
    isEmbedded: Boolean,
    fieldTemplates: List[FieldTemplate],
    manifestation: Option[ModelManifestation],
    indexes: Vector[Index]
) {
  def build(schema: Schema): Model = new Model(this, schema)
}

object Model {
  implicit def asModelTemplate(model: Model): ModelTemplate = model.template

  val empty: Model = new Model(
    template = ModelTemplate(
      name = "",
      stableIdentifier = "",
      isEmbedded = false,
      fieldTemplates = List.empty,
      manifestation = None,
      indexes = Vector.empty,
    ),
    schema = Schema.empty
  )
}
class Model(
    val template: ModelTemplate,
    val schema: Schema
) {
  import template._
  val isLegacy = schema.isLegacy

  val dbName: String                                     = manifestation.map(_.dbName).getOrElse(name)
  lazy val fields: List[Field]                           = fieldTemplates.map(_.build(this))
  lazy val scalarFields: List[ScalarField]               = fields.collect { case f: ScalarField => f }
  lazy val scalarListFields: List[ScalarField]           = scalarFields.filter(_.isList)
  lazy val scalarNonListFields: List[ScalarField]        = scalarFields.filter(!_.isList)
  lazy val visibleScalarNonListFields: List[ScalarField] = scalarNonListFields.filter(_.isVisible)
  lazy val relationFields: List[RelationField]           = fields.collect { case f: RelationField => f }
  lazy val relationListFields: List[RelationField]       = relationFields.filter(_.isList)
  lazy val relationNonListFields: List[RelationField]    = relationFields.filter(!_.isList)
  lazy val visibleRelationFields: List[RelationField]    = relationFields.filter(_.isVisible)
  lazy val nonListFields                                 = fields.filter(!_.isList)
  lazy val idField                                       = scalarFields.find(_.isId)
  lazy val createdAtField                                = scalarFields.find(_.isCreatedAt)
  lazy val updatedAtField                                = scalarFields.find(_.isUpdatedAt)
  lazy val idField_!                                     = idField.getOrElse(sys.error(s"The model $name has no id field!"))
  lazy val dbNameOfIdField_!                             = idField_!.dbName
  lazy val hasUpdatedAtField                             = scalarFields.exists(_.isUpdatedAt)
  lazy val hasCreatedAtField                             = scalarFields.exists(_.isCreatedAt)
  lazy val hasVisibleIdField: Boolean                    = idField.exists(_.isVisible)
  def dummyField(rf: RelationField): ScalarField =
    idField_!.copy(name = rf.name,
                   isList = rf.isList,
                   manifestation = Some(FieldManifestation(rf.dbName)),
                   template = idField_!.template.copy(behaviour = None))

  lazy val cascadingRelationFields: List[RelationField] = relationFields.collect {
    case field if field.relationSide == RelationSide.A && field.relation.template.modelAOnDelete == OnDelete.Cascade => field
    case field if field.relationSide == RelationSide.B && field.relation.template.modelBOnDelete == OnDelete.Cascade => field
  }

  lazy val inlineFields = relationFields.collect {
    case rf if rf.relation.isInlineRelation && rf.relation.inlineManifestation.get.inTableOfModelName == this.name => rf
  }

  def filterScalarFields(fn: ScalarField => Boolean): Model = {
    val newFields         = this.scalarFields.filter(fn).map(_.template)
    val newModel          = copy(fieldTemplates = newFields)
    val newModelsInSchema = schema.models.filter(_.name != name).map(_.template) :+ newModel
    schema.copy(modelTemplates = newModelsInSchema).getModelByName_!(name)
  }

  def getRelationFieldByName_!(name: String): RelationField   = getFieldByName_!(name).asInstanceOf[RelationField]
  def getScalarFieldByName_!(name: String): ScalarField       = getFieldByName_!(name).asInstanceOf[ScalarField]
  def getScalarFieldByName(name: String): Option[ScalarField] = getFieldByName(name).map(_.asInstanceOf[ScalarField])
  def getFieldByName_!(name: String): Field                   = getFieldByName(name).getOrElse(sys.error(s"field $name is not part of the model ${this.name}"))
  def getFieldByName(name: String): Option[Field]             = fields.find(_.name == name)
  def getFieldByDBName_!(name: String): Field                 = getFieldByDBName(name).getOrElse(sys.error(s"field $name is not part of the model ${this.name}"))
  def getFieldByDBName(name: String): Option[Field]           = fields.find(_.dbName == name)
  def getIndexByName(name: String): Option[Index]             = indexes.find(_.name == name)
  def getIndexByName_!(name: String): Index                   = getIndexByName(name).getOrElse(sys.error(s"field $name is not part of the model ${this.name}"))
}
