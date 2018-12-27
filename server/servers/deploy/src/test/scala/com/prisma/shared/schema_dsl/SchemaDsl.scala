package com.prisma.shared.schema_dsl

import com.prisma.deploy.connector.{DeployConnector, InferredTables, MissingBackRelations}
import com.prisma.deploy.migration.inference.{SchemaInferrer, SchemaMapping}
import com.prisma.deploy.migration.validation.{DataModelValidatorImpl, LegacyDataModelValidator}
import com.prisma.gc_values.GCValue
import com.prisma.shared.models.ConnectorCapability.{LegacyDataModelCapability, RelationLinkListCapability}
import com.prisma.shared.models.IdType.Id
import com.prisma.shared.models.Manifestations.{FieldManifestation, EmbeddedRelationLink, ModelManifestation}
import com.prisma.shared.models._
import com.prisma.utils.await.AwaitUtils
import cool.graph.cuid.Cuid
import org.scalactic.{Bad, Good}
import org.scalatest.Suite

object SchemaDsl extends AwaitUtils {

  import scala.collection.mutable.Buffer

  def fromBuilder(fn: SchemaBuilder => Unit)(implicit deployConnector: DeployConnector, suite: Suite) = {
    val schemaBuilder = SchemaBuilder()
    fn(schemaBuilder)
    val project = schemaBuilder.build(id = projectId(suite))

    if (!deployConnector.isActive) {
      addIdFields(addManifestations(project, deployConnector))
    } else {
      addIdFields(project)
    }
  }

  def fromString(id: String = TestIds.testProjectId)(sdlString: String)(implicit deployConnector: DeployConnector, suite: Suite): Project = {
    val project = fromString(id = projectId(suite), InferredTables.empty, deployConnector)(sdlString.stripMargin)

    if (!deployConnector.isActive || deployConnector.capabilities.has(RelationLinkListCapability)) {
      addManifestations(project, deployConnector)
    } else {
      project
    }
  }

  private def projectId(suite: Suite): String = {
    // GetFieldFromSQLUniqueException blows up if we generate longer names, since we then exceed the postgres limits for constraint names
    // todo: actually fix GetFieldFromSQLUniqueException instead
    val nameThatMightBeTooLong = suite.getClass.getSimpleName
    nameThatMightBeTooLong.substring(0, Math.min(32, nameThatMightBeTooLong.length))
  }

  def fromPassiveConnectorSdl(
      deployConnector: DeployConnector,
      id: String = TestIds.testProjectId
  )(sdlString: String): Project = {
    val inferredTables = deployConnector.databaseIntrospectionInferrer(id).infer().await()
    fromString(id, inferredTables, deployConnector)(sdlString)
  }

  private def fromString(
      id: String,
      inferredTables: InferredTables,
      deployConnector: DeployConnector
  )(sdlString: String): Project = {
    val emptyBaseSchema    = Schema()
    val emptySchemaMapping = SchemaMapping.empty
    val validator          = LegacyDataModelValidator

    val prismaSdl = validator.validate(sdlString, deployConnector.fieldRequirements, deployConnector.capabilities) match {
      case Good(prismaSdl) =>
        prismaSdl
      case Bad(errors) =>
        sys.error(
          s"""Encountered the following errors during schema validation. Please fix:
           |${errors.mkString("\n")}
         """.stripMargin
        )
    }

    val schema                 = SchemaInferrer(deployConnector.capabilities).infer(emptyBaseSchema, emptySchemaMapping, prismaSdl, inferredTables)
    val withBackRelationsAdded = MissingBackRelations.add(schema)
    TestProject().copy(id = id, schema = withBackRelationsAdded)
  }

  private def addManifestations(project: Project, deployConnector: DeployConnector): Project = {
    val schema = project.schema
    val newRelations = project.relations.map { relation =>
      if ((relation.isManyToMany && deployConnector.capabilities.hasNot(RelationLinkListCapability)) || relation.modelA.isEmbedded || relation.modelB.isEmbedded) {
        relation.template
      } else {
        val relationFields = Vector(relation.modelAField, relation.modelBField)
        val fieldToRepresentAsInlineRelation = relationFields.find(_.isList) match {
          case Some(field) => field
          case None        => relationFields.head // happens for one to one relations
        }
        val modelToLinkTo          = fieldToRepresentAsInlineRelation.model
        val modelToPutRelationInto = fieldToRepresentAsInlineRelation.relatedModel_!
        val manifestation = EmbeddedRelationLink(
          inTableOfModelName = modelToPutRelationInto.name,
          referencingColumn = s"${relation.name}_${modelToLinkTo.name.toLowerCase}_id"
        )
        relation.template.copy(manifestation = Some(manifestation))
      }
    }
    val newModels = project.models.map { model =>
      val newFields = model.fields.map { field =>
        val newRelation = field.relationOpt.flatMap(relation => newRelations.find(_.name == relation.name))
        field.template.copy(relationName = newRelation.map(_.name), manifestation = Some(FieldManifestation(field.name + "_column")))
      }

      model.copy(fieldTemplates = newFields, manifestation = Some(ModelManifestation(model.name + "_Table")))
    }
    project.copy(schema = schema.copy(relationTemplates = newRelations, modelTemplates = newModels))
  }

  private def addIdFields(project: Project): Project = {
    val newModels = project.models.map { model =>
      val modelContainsAlreadyAnIdField = model.idField.isDefined
      if (modelContainsAlreadyAnIdField) {
        model.copy()
      } else {
        val newFields = model.fields.map(_.template) :+ cuidField
        model.copy(fieldTemplates = newFields)
      }
    }
    project.copy(schema = project.schema.copy(modelTemplates = newModels))
  }

  case class SchemaBuilder(
      modelBuilders: Buffer[ModelBuilder] = Buffer.empty,
      enums: Buffer[Enum] = Buffer.empty,
      functions: Buffer[com.prisma.shared.models.Function] = Buffer.empty
  ) {

    def apply(fn: SchemaBuilder => Unit): Project = {
      fn(this)
      this.build(TestIds.testProjectId)
    }

    def model(name: String): ModelBuilder = {
      modelBuilders.find(_.name == name).getOrElse {
        val newModelBuilder = ModelBuilder(name)
        modelBuilders += newModelBuilder
        newModelBuilder
      }
    }

    def enum(name: String, values: Vector[String]): Enum = {
      val newEnum = Enum(name, values)
      enums += newEnum
      newEnum
    }

    private[schema_dsl] def build(id: String): Project = {
      val models    = modelBuilders.map(_.build())
      val relations = modelBuilders.flatMap(_.relations)
      TestProject().copy(
        id = id,
        schema = Schema(
          modelTemplates = models.toList,
          relationTemplates = relations.toList,
          enums = enums.toList
        ),
        functions = functions.toList
      )
    }
  }

  case class ModelBuilder(
      name: String,
      fields: Buffer[FieldTemplate] = Buffer.empty,
      var withPermissions: Boolean = true,
      relations: Buffer[RelationTemplate] = Buffer.empty,
      isEmbedded: Boolean = false
  ) {
    val id = name

    def field(name: String,
              theType: TypeIdentifier.type => TypeIdentifier.Value,
              enum: Option[Enum] = None,
              isList: Boolean = false,
              isUnique: Boolean = false,
              isHidden: Boolean = false,
              defaultValue: Option[GCValue] = None,
              constraints: List[FieldConstraint] = List.empty): ModelBuilder = {

      val newField = plainField(
        name,
        this,
        theType(TypeIdentifier),
        isRequired = false,
        isUnique = isUnique,
        isHidden = isHidden,
        enum = enum,
        isList = isList,
        defaultValue = defaultValue,
        constraints = constraints
      )

      fields += newField
      this
    }

    def field_!(name: String,
                theType: TypeIdentifier.type => TypeIdentifier.Value,
                enumValues: List[String] = List.empty,
                enum: Option[Enum] = None,
                isList: Boolean = false,
                isUnique: Boolean = false,
                isHidden: Boolean = false,
                defaultValue: Option[GCValue] = None): ModelBuilder = {
      val newField = plainField(
        name,
        this,
        theType(TypeIdentifier),
        isRequired = true,
        isUnique = isUnique,
        isHidden = isHidden,
        enum = enum,
        isList = isList,
        defaultValue = defaultValue
      )
      fields += newField
      this
    }

    def withTimeStamps: ModelBuilder = {
      fields += createdAtField += updatedAtField
      this
    }

    def oneToOneRelation(
        fieldAName: String,
        fieldBName: String,
        modelB: ModelBuilder,
        relationName: Option[String] = None,
        includeFieldB: Boolean = true,
        modelAOnDelete: OnDelete.Value = OnDelete.SetNull,
        modelBOnDelete: OnDelete.Value = OnDelete.SetNull
    ): ModelBuilder = {
      val relation = RelationTemplate(
        name = relationName.getOrElse(s"${this.name}To${modelB.name}"),
        modelAName = this.id,
        modelBName = modelB.id,
        modelAOnDelete = modelAOnDelete,
        modelBOnDelete = modelBOnDelete,
        manifestation = None
      )
      val newField = relationField(fieldAName, from = this, modelB, relation, isList = false, isBackward = false, isRequired = false, includeInSchema = true)
      fields += newField

      val newBField = relationField(fieldBName, modelB, to = this, relation, isList = false, isBackward = true, isRequired = false, includeFieldB)
      modelB.fields += newBField

      this.relations += relation

      this
    }

    def oneToOneRelation_!(
        fieldAName: String,
        fieldBName: String,
        modelB: ModelBuilder,
        relationName: Option[String] = None,
        isRequiredOnFieldB: Boolean = true,
        includeFieldB: Boolean = true,
        modelAOnDelete: OnDelete.Value = OnDelete.SetNull,
        modelBOnDelete: OnDelete.Value = OnDelete.SetNull
    ): ModelBuilder = {
      val relation = RelationTemplate(
        name = relationName.getOrElse(s"${this.name}To${modelB.name}"),
        modelAName = this.id,
        modelBName = modelB.id,
        modelAOnDelete = modelAOnDelete,
        modelBOnDelete = modelBOnDelete,
        manifestation = None
      )

      val newField = relationField(fieldAName, this, modelB, relation, isList = false, isBackward = false, isRequired = true, true)
      fields += newField

      val newBField = relationField(fieldBName, modelB, this, relation, isList = false, isBackward = true, isRequired = isRequiredOnFieldB, includeFieldB)
      modelB.fields += newBField
      this.relations += relation

      this
    }

    def oneToManyRelation_!(
        fieldAName: String,
        fieldBName: String,
        modelB: ModelBuilder,
        relationName: Option[String] = None,
        includeFieldB: Boolean = true,
        modelAOnDelete: OnDelete.Value = OnDelete.SetNull,
        modelBOnDelete: OnDelete.Value = OnDelete.SetNull
    ): ModelBuilder = {
      val relation = RelationTemplate(
        name = relationName.getOrElse(s"${this.name}To${modelB.name}"),
        modelAName = this.id,
        modelBName = modelB.id,
        modelAOnDelete = modelAOnDelete,
        modelBOnDelete = modelBOnDelete,
        manifestation = None
      )

      val newField = relationField(fieldAName, this, modelB, relation, isList = true, isBackward = false, isRequired = false, true)
      fields += newField

      val newBField = relationField(fieldBName, modelB, this, relation, isList = false, isBackward = true, isRequired = true, includeFieldB)
      modelB.fields += newBField
      this.relations += relation

      this
    }

    def oneToManyRelation(
        fieldAName: String,
        fieldBName: String,
        modelB: ModelBuilder,
        relationName: Option[String] = None,
        includeFieldB: Boolean = true,
        modelAOnDelete: OnDelete.Value = OnDelete.SetNull,
        modelBOnDelete: OnDelete.Value = OnDelete.SetNull
    ): ModelBuilder = {
      val relation = RelationTemplate(
        name = relationName.getOrElse(s"${this.name}To${modelB.name}"),
        modelAName = this.id,
        modelBName = modelB.id,
        modelAOnDelete = modelAOnDelete,
        modelBOnDelete = modelBOnDelete,
        manifestation = None
      )
      val newField = relationField(fieldAName, this, modelB, relation, isList = true, isBackward = false, false, true)
      fields += newField

      val newBField = relationField(fieldBName, modelB, this, relation, isList = false, isBackward = true, false, includeFieldB)
      modelB.fields += newBField
      this.relations += relation

      this
    }

    def manyToOneRelation(
        fieldAName: String,
        fieldBName: String,
        modelB: ModelBuilder,
        relationName: Option[String] = None,
        includeFieldB: Boolean = true,
        modelAOnDelete: OnDelete.Value = OnDelete.SetNull,
        modelBOnDelete: OnDelete.Value = OnDelete.SetNull
    ): ModelBuilder = {
      val relation = RelationTemplate(
        name = relationName.getOrElse(s"${this.name}To${modelB.name}"),
        modelAName = this.id,
        modelBName = modelB.id,
        modelAOnDelete = modelAOnDelete,
        modelBOnDelete = modelBOnDelete,
        manifestation = None
      )
      val newField = relationField(fieldAName, this, modelB, relation, isList = false, isBackward = false, false, true)
      fields += newField

      val newBField = relationField(fieldBName, modelB, this, relation, isList = true, isBackward = true, false, includeFieldB)
      modelB.fields += newBField
      this.relations += relation

      this
    }

    def manyToManyRelation(
        fieldAName: String,
        fieldBName: String,
        modelB: ModelBuilder,
        relationName: Option[String] = None,
        includeFieldBInSchema: Boolean = true,
        modelAOnDelete: OnDelete.Value = OnDelete.SetNull,
        modelBOnDelete: OnDelete.Value = OnDelete.SetNull
    ): ModelBuilder = {
      val relation = RelationTemplate(
        name = relationName.getOrElse(s"${this.name}To${modelB.name}"),
        modelAName = this.id,
        modelBName = modelB.id,
        modelAOnDelete = modelAOnDelete,
        modelBOnDelete = modelBOnDelete,
        manifestation = None
      )
      val newField = relationField(fieldAName, from = this, to = modelB, relation, isList = true, isBackward = false, false, true)
      fields += newField

      val newBField = relationField(fieldBName, from = modelB, to = this, relation, isList = true, isBackward = true, false, includeFieldBInSchema)
      modelB.fields += newBField
      this.relations += relation

      this
    }

    def build(): ModelTemplate = {
      ModelTemplate(
        name = name,
        stableIdentifier = Cuid.createCuid(),
        isEmbedded = isEmbedded,
        fieldTemplates = fields.toList,
        manifestation = None,
        indexes = Vector.empty,
      )
    }
  }

  def plainField(name: String,
                 model: ModelBuilder,
                 theType: TypeIdentifier.Value,
                 isRequired: Boolean,
                 isUnique: Boolean,
                 isHidden: Boolean,
                 enum: Option[Enum],
                 isList: Boolean,
                 defaultValue: Option[GCValue] = None,
                 constraints: List[FieldConstraint] = List.empty): FieldTemplate = {

    FieldTemplate(
      name = name,
      typeIdentifier = theType,
      isRequired = isRequired,
      enum = enum,
      defaultValue = defaultValue,
      // hardcoded values
      isList = isList,
      isUnique = isUnique,
      isReadonly = false,
      isHidden = isHidden,
      relationName = None,
      relationSide = None,
      manifestation = None,
      behaviour = None
    )
  }

  def relationField(name: String,
                    from: ModelBuilder,
                    to: ModelBuilder,
                    relation: RelationTemplate,
                    isList: Boolean,
                    isBackward: Boolean,
                    isRequired: Boolean = false,
                    includeInSchema: Boolean): FieldTemplate = {
    FieldTemplate(
      name = if (!includeInSchema) Field.magicalBackRelationPrefix + relation.name else name,
      isList = isList,
      relationSide = Some(if (!isBackward) RelationSide.A else RelationSide.B),
      relationName = Some(relation.name),
      // hardcoded values
      typeIdentifier = TypeIdentifier.Relation,
      isRequired = isRequired,
      isUnique = false,
      isHidden = !includeInSchema,
      isReadonly = false,
      defaultValue = None,
      enum = None,
      manifestation = None,
      behaviour = None
    )
  }

  def newId(): Id = Cuid.createCuid()

  private val cuidField = FieldTemplate(
    name = "id",
    typeIdentifier = TypeIdentifier.Cuid,
    isRequired = true,
    isList = false,
    isUnique = true,
    isReadonly = true,
    enum = None,
    defaultValue = None,
    relationName = None,
    relationSide = None,
    manifestation = None,
    behaviour = None
  )

  private val intIdField = FieldTemplate(
    name = "id",
    typeIdentifier = TypeIdentifier.Int,
    isRequired = true,
    isList = false,
    isUnique = true,
    isReadonly = true,
    isAutoGeneratedByDb = true,
    enum = None,
    defaultValue = None,
    relationName = None,
    relationSide = None,
    manifestation = None,
    behaviour = None
  )

  private val updatedAtField = FieldTemplate(
    name = "updatedAt",
    typeIdentifier = TypeIdentifier.DateTime,
    isRequired = true,
    isList = false,
    isUnique = false,
    isReadonly = true,
    enum = None,
    defaultValue = None,
    relationName = None,
    relationSide = None,
    manifestation = None,
    behaviour = None
  )

  private val createdAtField = FieldTemplate(
    name = "createdAt",
    typeIdentifier = TypeIdentifier.DateTime,
    isRequired = true,
    isList = false,
    isUnique = true,
    isReadonly = true,
    enum = None,
    defaultValue = None,
    relationName = None,
    relationSide = None,
    manifestation = None,
    behaviour = None
  )
}
