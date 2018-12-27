package com.prisma.deploy.migration.inference

import com.prisma.deploy.connector.{InferredTables, MissingBackRelations}
import com.prisma.deploy.migration.DirectiveTypes.{MongoInlineRelationDirective, PGInlineRelationDirective, RelationTableDirective}
import com.prisma.deploy.migration.validation._
import com.prisma.deploy.schema.InvalidRelationName
import com.prisma.deploy.validation.NameConstraints
import com.prisma.shared.models.ConnectorCapability.{LegacyDataModelCapability, MigrationsCapability, RelationLinkListCapability}
import com.prisma.shared.models.FieldBehaviour.{IdBehaviour, IdStrategy}
import com.prisma.shared.models.Manifestations._
import com.prisma.shared.models.{OnDelete, RelationSide, ReservedFields, _}
import com.prisma.utils.await.AwaitUtils
import cool.graph.cuid.Cuid
import sangria.ast.{Field => _}

trait SchemaInferrer {
  def infer(baseSchema: Schema, schemaMapping: SchemaMapping, graphQlSdl: PrismaSdl, inferredTables: InferredTables): Schema
}

object SchemaInferrer {
  def apply(capabilities: ConnectorCapabilities) = new SchemaInferrer {
    override def infer(baseSchema: Schema, schemaMapping: SchemaMapping, graphQlSdl: PrismaSdl, inferredTables: InferredTables) =
      SchemaInferrerImpl(
        baseSchema = baseSchema,
        schemaMapping = schemaMapping,
        prismaSdl = graphQlSdl,
        capabilities = capabilities,
        inferredTables = inferredTables
      ).infer()
  }
}

case class SchemaInferrerImpl(
    baseSchema: Schema,
    schemaMapping: SchemaMapping,
    prismaSdl: PrismaSdl,
    capabilities: ConnectorCapabilities,
    inferredTables: InferredTables
) extends AwaitUtils {

  val isLegacy      = capabilities.has(LegacyDataModelCapability)
  val hasMigrations = capabilities.has(MigrationsCapability)
  val isMongo       = capabilities.has(RelationLinkListCapability)
  val isSql         = !isMongo

  def infer(): Schema = {
    val schemaWithOutOptionalBackrelations = Schema(
      modelTemplates = nextModels.toList,
      relationTemplates = nextRelations.toList,
      enums = nextEnums.toList,
      version = if (isLegacy) None else Some("v2")
    )
    val schemaWithOptionalBackRelations = MissingBackRelations.add(schemaWithOutOptionalBackrelations)
    schemaWithOptionalBackRelations
  }

  lazy val nextModels: Vector[ModelTemplate] = {
    prismaSdl.modelTypes.map { prismaType =>
      val fieldNames = prismaType.fields.map(_.name)
      // TODO: this can be removed after the unification of active and passive SQL
      val hiddenReservedFields = if (isSql) {
        if (hasMigrations && isLegacy) {
          val missingReservedFields = ReservedFields.reservedFieldNames.filterNot(fieldNames.contains)
          missingReservedFields.map(ReservedFields.reservedFieldFor)
        } else {
          Vector.empty
        }
      } else if (isMongo) { // MONGO
        if (isLegacy) { // this is the case in tests
          Vector(ReservedFields.embeddedIdField)
        } else {
          Vector.empty
        }
      } else {
        sys.error("Will not happen.")
      }

      val manifestation = prismaType.tableName.map(ModelManifestation)

      val stableIdentifier = baseSchema.getModelByName(schemaMapping.getPreviousModelName(prismaType.name)) match {
        case Some(existingModel) => existingModel.stableIdentifier
        case None                => Cuid.createCuid()
      }

      ModelTemplate(
        name = prismaType.name,
        stableIdentifier = stableIdentifier,
        isEmbedded = prismaType.isEmbedded,
        fieldTemplates = fieldsForType(prismaType).toList ++ hiddenReservedFields,
        manifestation = manifestation,
        indexes = prismaType.indexes.map(i => Index(name = i.name, fields = i.fields))
      )
    }
  }

  def fieldsForType(prismaType: PrismaType): Vector[FieldTemplate] = {

    val fields: Vector[FieldTemplate] = prismaType.fields.flatMap { prismaField =>
      def relationFromRelationField(x: RelationalPrismaField) = {
        x.relationName match {
          case Some(name) =>
            nextRelations.find(_.name == name)

          case None =>
            val relationsThatConnectBothModels = nextRelations.filter(relation => relation.connectsTheModels(prismaType.name, x.referencesType))
            if (relationsThatConnectBothModels.size > 1) {
              None
            } else {
              relationsThatConnectBothModels.headOption
            }
        }
      }

      //For self relations we were inferring the relationSide A for both sides, this now assigns A to the lexicographically lower field name and B to the other
      //If in the previous schema both relationSides are A we reassign the relationsides otherwise we keep the one from the previous schema.
      def inferRelationSide(relation: Option[RelationTemplate]) = {
        def oldRelationSidesNotBothEqual(oldField: RelationField) = oldField.relationSide != oldField.relatedField.relationSide

        relation.map { relation =>
          if (relation.isSelfRelation) {
            val oldFieldName = schemaMapping.getPreviousFieldName(prismaType.name, prismaField.name)
            val oldModelName = schemaMapping.getPreviousModelName(prismaType.name)
            val oldField     = baseSchema.getFieldByName(oldModelName, oldFieldName)

            oldField match {
              case Some(field: RelationField) if oldRelationSidesNotBothEqual(field) =>
                field.relationSide

              case _ =>
                val relationFieldNames = prismaType.relationFields.filter(f => f.relationName.contains(relation.name)).map(_.name)
                if (relationFieldNames.exists(name => name < prismaField.name)) RelationSide.B else RelationSide.A //Fixme here the side is implemented for the field
            }
          } else {
            if (relation.modelAName == prismaType.name) RelationSide.A else RelationSide.B
          }
        }
      }

      prismaField match {
        case scalarField: ScalarPrismaField =>
          val isAutoGeneratedByDb = if (isLegacy) {
            capabilities.hasNot(MigrationsCapability) && scalarField.typeIdentifier == TypeIdentifier.Int && scalarField.name == "id"
          } else {
            scalarField.behaviour.contains(IdBehaviour(IdStrategy.Auto)) && scalarField.typeIdentifier == TypeIdentifier.Int
          }
          Some(
            FieldTemplate(
              name = scalarField.name,
              typeIdentifier = scalarField.typeIdentifier,
              isRequired = scalarField.isRequired,
              isList = scalarField.isList,
              isUnique = scalarField.isUnique,
              isAutoGeneratedByDb = isAutoGeneratedByDb,
              enum = None,
              defaultValue = scalarField.defaultValue,
              relationName = None,
              relationSide = None,
              manifestation = scalarField.columnName.map(FieldManifestation),
              behaviour = scalarField.behaviour,
              isHidden = scalarField.isHidden
            ))

        case enumField: EnumPrismaField =>
          Some(
            FieldTemplate(
              name = enumField.name,
              typeIdentifier = enumField.typeIdentifier,
              isRequired = enumField.isRequired,
              isList = enumField.isList,
              isUnique = enumField.isUnique,
              enum = nextEnums.find(_.name == enumField.enumName),
              defaultValue = enumField.defaultValue,
              relationName = None,
              relationSide = None,
              manifestation = enumField.columnName.map(FieldManifestation),
              behaviour = enumField.behaviour
            ))
        case relationField: RelationalPrismaField =>
          val relation = relationFromRelationField(relationField)

          Some(
            FieldTemplate(
              name = relationField.name,
              typeIdentifier = relationField.typeIdentifier,
              isRequired = relationField.isRequired,
              isList = relationField.isList,
              isUnique = false,
              enum = None,
              defaultValue = None,
              relationName = relation.map(_.name),
              relationSide = inferRelationSide(relation),
              manifestation = None,
              behaviour = None
            ))
      }
    }

    fields
  }

  lazy val nextRelations: Set[RelationTemplate] = {
    val tmp = for {
      prismaType    <- prismaSdl.modelTypes
      relationField <- prismaType.relationFields
    } yield {
      val model1       = prismaType.name
      val model2       = relationField.referencesType
      val relatedField = relationField.relatedField

      val model1OnDelete: OnDelete.Value = relationField.cascade
      val model2OnDelete: OnDelete.Value = relatedField.map(_.cascade).getOrElse(OnDelete.SetNull)

      val (modelA, modelAOnDelete, modelB, modelBOnDelete) = () match {
        case _ if model1 < model2                                                                            => (model1, model1OnDelete, model2, model2OnDelete)
        case _ if model1 > model2                                                                            => (model2, model2OnDelete, model1, model1OnDelete)
        case _ if (model1 == model2) && relatedField.isDefined && relationField.name < relatedField.get.name => (model1, model1OnDelete, model2, model2OnDelete)
        case _ if (model1 == model2) && relatedField.isDefined && relationField.name > relatedField.get.name => (model2, model2OnDelete, model1, model1OnDelete)
        case _ if model1 == model2                                                                           => (model1, model1OnDelete, model2, model2OnDelete)

      }

      /**
        * 1: has relation directive. use that one.
        * 2: has no relation directive but there's a related field with directive. Use name of the related field.
        * 3: use auto generated name else
        */
      def generateRelationName: String = {
        def concat(modelName: String, otherModelName: String): String = {
          val concatenatedString = s"${modelName}To${otherModelName}"

          !NameConstraints.isValidRelationName(concatenatedString) match {
            case true if otherModelName.length > modelName.length => concat(modelName, otherModelName.substring(0, otherModelName.length - 1))
            case true                                             => concat(modelName.substring(0, modelName.length - 1), otherModelName)
            case false                                            => concatenatedString
          }
        }
        concat(modelA, modelB)
      }

      val relationNameOnRelatedField: Option[String] = relatedField.flatMap(_.relationName)
      val relationName = (relationField.relationName, relationNameOnRelatedField) match {
        case (Some(name), _) if !NameConstraints.isValidRelationName(name)    => throw InvalidRelationName(name)
        case (None, Some(name)) if !NameConstraints.isValidRelationName(name) => throw InvalidRelationName(name)
        case (Some(name), _)                                                  => name
        case (None, Some(name))                                               => name
        case (None, None)                                                     => generateRelationName
      }
      val previousModelAName = schemaMapping.getPreviousModelName(modelA)
      val previousModelBName = schemaMapping.getPreviousModelName(modelB)

      val oldEquivalentRelationByName =
        relationField.relationName.flatMap(baseSchema.getRelationByName).filter(rel => rel.connectsTheModels(previousModelAName, previousModelBName))

      val oldEquivalentRelation = oldEquivalentRelationByName.orElse {
        UnambiguousRelation.unambiguousRelationThatConnectsModels(baseSchema, previousModelAName, previousModelBName)
      }
      val relationManifestation = relationManifestationOnFieldOrRelatedField(prismaType, relationField, relationName)

      val nextRelation = RelationTemplate(
        name = relationName,
        modelAName = modelA,
        modelBName = modelB,
        modelAOnDelete = modelAOnDelete,
        modelBOnDelete = modelBOnDelete,
        manifestation = relationManifestation
      )

      oldEquivalentRelation match {
        case Some(relation) =>
          val nextModelAId = if (previousModelAName == relation.modelAName) modelA else modelB
          val nextModelBId = if (previousModelBName == relation.modelBName) modelB else modelA
          nextRelation.copy(modelAName = nextModelAId, modelBName = nextModelBId)

        case None => nextRelation
      }
    }
    tmp.groupBy(_.name).values.flatMap(_.headOption).toSet
  }

  def relationManifestationOnFieldOrRelatedField(
      prismaType: PrismaType,
      relationField: RelationalPrismaField,
      relationName: String
  ): Option[RelationLinkManifestation] = {
    if (!isLegacy) { //new
      import RelationStrategy._

      relationField.relatedField match {
        case Some(relatedField) =>
          (relationField.strategy, relatedField.strategy) match {
            case (None, None) if relationField.isOne  => manifestationForField(prismaType, relationField, relationName)
            case (None, None) if relationField.isList => manifestationForField(relatedField.tpe, relatedField, relationName)
            case (_, None)                            => manifestationForField(prismaType, relationField, relationName)
            case (None, _)                            => manifestationForField(relatedField.tpe, relatedField, relationName)
            case (_, _)                               => sys.error("must not happen")
          }

        case None =>
          manifestationForField(prismaType, relationField, relationName)
      }
    } else if (capabilities.hasNot(MigrationsCapability) || capabilities.has(RelationLinkListCapability)) { //passive or mongo
      val manifestationOnThisField = legacyRelationManifestationOnField(prismaType, relationField)
      val manifestationOnRelatedField = relationField.relatedField.flatMap { relatedField =>
        val relatedType = prismaSdl.modelType_!(relationField.referencesType)
        legacyRelationManifestationOnField(relatedType, relatedField)
      }

      manifestationOnThisField.orElse(manifestationOnRelatedField)
    } else { // active sql
      None
    }
  }

  private def manifestationForField(prismaType: PrismaType, relationField: RelationalPrismaField, relationName: String): Option[RelationLinkManifestation] = {
    val activeStrategy = relationField.strategy match {
      case Some(strat) => strat
      case None =>
        if (capabilities.has(RelationLinkListCapability)) {
          RelationStrategy.Inline
        } else if (relationField.hasManyToManyRelation) {
          RelationStrategy.Table
        } else if (relationField.hasOneToManyRelation && relationField.isOne) {
          RelationStrategy.Inline
        } else {
          sys.error("One to one relations must not have the AUTO strategy")
        }
    }

    activeStrategy match {
      case RelationStrategy.Inline =>
        if (relationField.relatedType.isEmbedded) {
          None
        } else if (capabilities.has(RelationLinkListCapability)) {
          Some(EmbeddedRelationLink(inTableOfModelName = prismaType.name, referencingColumn = relationField.finalDbName))
        } else {
          // this can be only one to many in SQL
          val oneRelationField = relationField.oneRelationField.get
          Some(EmbeddedRelationLink(inTableOfModelName = oneRelationField.tpe.name, referencingColumn = oneRelationField.finalDbName))
        }

      case RelationStrategy.Table =>
        prismaSdl.relationTable(relationName) match {
          case Some(relationTable) =>
            // FIXME: this is a duplication of the name logic in `nextRelations`
            val (modelX, modelY) = (prismaType.name, relationField.referencesType)
            val (modelA, modelB) = if (modelX < modelY) (modelX, modelY) else (modelY, modelX)
            val modelAColumn     = relationTable.relationFields.find(_.referencesType == modelA).get
            val modelBColumn     = relationTable.relationFields.find(_.referencesType == modelB).get

            Some(RelationTable(table = relationTable.finalTableName, modelAColumn = modelAColumn.finalDbName, modelBColumn = modelBColumn.finalDbName))
          case None =>
            Some(RelationTable(table = relationName, modelAColumn = "A", modelBColumn = "B"))
        }
    }
  }

  def legacyRelationManifestationOnField(prismaType: PrismaType, relationField: RelationalPrismaField): Option[RelationLinkManifestation] = {
    val relatedType         = relationField.relatedType
    val tableForThisType    = prismaType.finalTableName
    val tableForRelatedType = relatedType.finalTableName
    val isThisModelA        = isModelA(prismaType.name, relationField.referencesType)
    relationField.relationDbDirective match {
      case Some(inlineDirective: MongoInlineRelationDirective) =>
        Some(EmbeddedRelationLink(inTableOfModelName = prismaType.name, referencingColumn = inlineDirective.field))

      case Some(inlineDirective: PGInlineRelationDirective) =>
        Some(EmbeddedRelationLink(inTableOfModelName = prismaType.name, referencingColumn = inlineDirective.column))

      case Some(tableDirective: RelationTableDirective) =>
        val inferredTable        = inferredTables.relationTables.find(_.name == tableDirective.table)
        def columnForThisType    = tableDirective.thisColumn.orElse(inferredTable.flatMap(table => table.columnForTable(tableForThisType)))
        def columnForRelatedType = tableDirective.otherColumn.orElse(inferredTable.flatMap(table => table.columnForTable(tableForRelatedType)))

        for {
          modelAColumn <- if (isThisModelA) columnForThisType else columnForRelatedType
          modelBColumn <- if (isThisModelA) columnForRelatedType else columnForThisType
        } yield {
          RelationTable(
            table = tableDirective.table,
            modelAColumn = modelAColumn,
            modelBColumn = modelBColumn
          )
        }

      case None =>
        inferredTables.relationTables
          .find { relationTable =>
            relationTable.referencesTheTables(tableForThisType, tableForRelatedType)
          }
          .flatMap { inferredTable =>
            val columnForThisType    = inferredTable.columnForTable(tableForThisType)
            val columnForRelatedType = inferredTable.columnForTable(tableForRelatedType)

            for {
              modelAColumn <- if (isThisModelA) columnForThisType else columnForRelatedType
              modelBColumn <- if (isThisModelA) columnForRelatedType else columnForThisType
            } yield {
              RelationTable(
                table = inferredTable.name,
                modelAColumn = modelAColumn,
                modelBColumn = modelBColumn
              )
            }
          }
          .orElse {
            for {
              referencedType <- prismaSdl.modelType(relationField.referencesType)
              modelTable     <- inferredTables.modelTables.find(_.name == prismaType.finalTableName)
              column         <- modelTable.columnNameForReferencedTable(referencedType.tableName.getOrElse(referencedType.name))
            } yield {
              EmbeddedRelationLink(prismaType.name, column)
            }
          }
    }
  }

  lazy val nextEnums: Vector[Enum] = prismaSdl.enums.map(enumType => Enum(name = enumType.name, values = enumType.values))

  def isModelA(model1: String, model2: String): Boolean = model1 < model2
}
