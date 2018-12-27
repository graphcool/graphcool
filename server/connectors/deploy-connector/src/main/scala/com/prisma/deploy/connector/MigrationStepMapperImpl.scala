package com.prisma.deploy.connector

import com.prisma.shared.models.ConnectorCapability.RelationLinkListCapability
import com.prisma.shared.models.Manifestations.{EmbeddedRelationLink, RelationTable}
import com.prisma.shared.models._

case class MigrationStepMapperImpl(projectId: String) extends MigrationStepMapper {
  def mutactionFor(previousSchema: Schema, nextSchema: Schema, step: MigrationStep): Vector[DeployMutaction] = step match {
    case x: CreateModel =>
      val model = nextSchema.getModelByName_!(x.name)
      Vector(CreateModelTable(projectId, model))

    case x: DeleteModel =>
      val model                = previousSchema.getModelByName_!(x.name)
      val scalarListFieldNames = model.scalarListFields.map(_.name).toVector
      Vector(DeleteModelTable(projectId, model, model.dbNameOfIdField_!, scalarListFieldNames))

    case x: UpdateModel =>
      val model                = nextSchema.getModelByName(x.newName).getOrElse(nextSchema.getModelByName_!(x.newName.substring(2)))
      val scalarListFieldNames = model.scalarListFields.map(_.name).toVector
      Vector(RenameTable(projectId = projectId, previousName = x.name, nextName = x.newName, scalarListFieldsNames = scalarListFieldNames))

    case x: CreateField =>
      val model = nextSchema.getModelByName_!(x.model)
      val field = model.getFieldByName_!(x.name)

      field match {
        case _ if ReservedFields.idFieldName == field.name => Vector.empty
        case _: RelationField                              => Vector.empty
        case f: ScalarField if field.isScalarList          => Vector(CreateScalarListTable(projectId, model, f))
        case f: ScalarField if field.isScalarNonList       => Vector(CreateColumn(projectId, model, f))
      }

    case x: DeleteField =>
      val model = previousSchema.getModelByName_!(x.model)
      val field = model.getFieldByName_!(x.name)

      field match {
        case _ if field.isRelation                   => Vector.empty
        case f: ScalarField if field.isScalarList    => Vector(DeleteScalarListTable(projectId, model, f))
        case f: ScalarField if field.isScalarNonList => Vector(DeleteColumn(projectId, model, f))
      }

    case x: UpdateField =>
      val oldModel           = previousSchema.getModelByName_!(x.model)
      val newModel           = nextSchema.getModelByName_!(x.newModel)
      val next               = nextSchema.getFieldByName_!(x.newModel, x.finalName)
      val previous           = previousSchema.getFieldByName_!(x.model, x.name)
      lazy val temporaryNext = next.asScalarField_!.copy(name = next.name + "_prisma_tmp", manifestation = None)

      lazy val createColumn          = CreateColumn(projectId, oldModel, next.asScalarField_!)
      lazy val updateColumn          = UpdateColumn(projectId, oldModel, previous.asScalarField_!, next.asScalarField_!)
      lazy val deleteColumn          = DeleteColumn(projectId, oldModel, previous.asScalarField_!)
      lazy val createScalarListTable = CreateScalarListTable(projectId, oldModel, next.asScalarField_!)
      lazy val deleteScalarListTable = DeleteScalarListTable(projectId, oldModel, previous.asScalarField_!)
      lazy val updateScalarListTable = UpdateScalarListTable(projectId, oldModel, newModel, previous.asScalarField_!, next.asScalarField_!)
      lazy val createTemporaryColumn = createColumn.copy(field = temporaryNext)
      lazy val renameTemporaryColumn = UpdateColumn(projectId, oldModel, temporaryNext, next.asScalarField_!)

      // TODO: replace that with a pattern match based on the subtypes of `models.Field`
      () match {
        case _ if previous.isRelation && next.isRelation                                                             => Vector.empty
        case _ if previous.isRelation && next.isScalarNonList                                                        => Vector(createColumn)
        case _ if previous.isRelation && next.isScalarList                                                           => Vector(createScalarListTable)
        case _ if previous.isScalarList && next.isScalarNonList                                                      => Vector(createColumn, deleteScalarListTable)
        case _ if previous.isScalarList && next.isRelation                                                           => Vector(deleteScalarListTable)
        case _ if previous.isScalarNonList && next.isScalarList                                                      => Vector(createScalarListTable, deleteColumn)
        case _ if previous.isScalarNonList && next.isRelation                                                        => Vector(deleteColumn)
        case _ if previous.isScalarNonList && next.isScalarNonList && previous.typeIdentifier == next.typeIdentifier => Vector(updateColumn)
        case _ if previous.isScalarList && next.isScalarList && previous.typeIdentifier == next.typeIdentifier       => Vector(updateScalarListTable)
        case _ if previous.isScalarList && next.isScalarList                                                         => Vector(deleteScalarListTable, createScalarListTable)
        case _ if previous.isScalarNonList && next.isScalarNonList =>
          val isIdTypeChange = previous.asScalarField_!.isId && next.asScalarField_!.isId && previous.asScalarField_!.typeIdentifier != next.asScalarField_!.typeIdentifier
          val common         = Vector(createTemporaryColumn, deleteColumn, renameTemporaryColumn)
          if (isIdTypeChange) {
            val deleteRelations = previousSchema.relations.filter(_.containsTheModel(previous.model)).map(deleteRelation).toVector
            val recreateRelations = nextSchema.relations
              .filter(r => r.containsTheModel(next.model))
              .filter(r => previousSchema.relations.exists(_.name == r.name))
              .map(createRelation)
              .toVector

            deleteRelations ++ common ++ recreateRelations
          } else {
            common
          }
      }

    case x: CreateRelation =>
      val relation = nextSchema.getRelationByName_!(x.name)
      Vector(createRelation(relation))

    case x: DeleteRelation =>
      val relation = previousSchema.getRelationByName_!(x.name)
      Vector(deleteRelation(relation))

    case x: CreateIndex => {
      val model = nextSchema.getModelByName_!(x.modelName)
      val index = model.getIndexByName_!(x.indexName)
      Vector(CreateTableIndex(projectId, model, index))
    }

    case x: AlterIndex => {
      val model = nextSchema.getModelByName_!(x.modelName)
      Vector(AlterTableIndex(projectId, model, x.oldName, x.newName))
    }

    case x: DeleteIndex => {
      val model = previousSchema.getModelByName_!(x.modelName)
      val index = model.getIndexByName_!(x.indexName)
      Vector(DeleteTableIndex(projectId, model, index))
    }

    case x: UpdateRelation =>
      val previousRelation      = previousSchema.getRelationByName_!(x.name)
      val nextRelation          = nextSchema.getRelationByName_!(x.finalName)
      val previousManifestation = previousRelation.manifestation
      val nextManifestation     = nextRelation.manifestation

      val manifestationChange = (previousManifestation, nextManifestation) match {
        case (Some(_: EmbeddedRelationLink), Some(_: RelationTable)) =>
          Vector(
            deleteRelation(previousRelation),
            createRelation(nextRelation)
          )
        case (Some(_: RelationTable), Some(_: EmbeddedRelationLink)) =>
          Vector(
            deleteRelation(previousRelation),
            createRelation(nextRelation)
          )

        case (Some(_: EmbeddedRelationLink), Some(_: EmbeddedRelationLink)) =>
          Vector(
            deleteRelation(previousRelation),
            createRelation(nextRelation)
          )
        case (Some(_: RelationTable), Some(_: RelationTable)) =>
          Vector(
            UpdateRelationTable(projectId, previousRelation, nextRelation)
          )
        case (None, None) => // this is the case for the legacy data model where we always have relation tables
          Vector(
            UpdateRelationTable(projectId, previousRelation, nextRelation)
          )

        case (p, n) =>
          sys.error(s"Combination $p, $n not supported here") // this can only happen for None + Some combination that must not be possible
      }

      manifestationChange

    case x: EnumMigrationStep =>
      Vector.empty

    case x: UpdateSecrets =>
      Vector.empty
  }

  def deleteRelation(relation: Relation): DeployMutaction = {
    relation.manifestation match {
      case Some(m: EmbeddedRelationLink) =>
        val modelA              = relation.modelA
        val modelB              = relation.modelB
        val (model, references) = if (m.inTableOfModelName == modelA.name) (modelA, modelB) else (modelB, modelA)

        DeleteInlineRelation(projectId, model, references, m.referencingColumn)
      case _ =>
        DeleteRelationTable(projectId, relation)
    }
  }

  def createRelation(relation: Relation): DeployMutaction = {
    relation.manifestation match {
      case Some(m: EmbeddedRelationLink) =>
        val modelA              = relation.modelA
        val modelB              = relation.modelB
        val (model, references) = if (m.inTableOfModelName == modelA.name) (modelA, modelB) else (modelB, modelA)

        CreateInlineRelation(projectId, model, references, m.referencingColumn)
      case _ =>
        CreateRelationTable(projectId, relation = relation)
    }
  }
}
