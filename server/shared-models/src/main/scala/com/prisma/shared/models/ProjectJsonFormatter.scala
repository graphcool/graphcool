package com.prisma.shared.models

import java.util.UUID

import com.prisma.gc_values._
import com.prisma.shared.models.FieldBehaviour._
import com.prisma.shared.models.FieldConstraintType.FieldConstraintType
import com.prisma.shared.models.Manifestations._
import com.prisma.shared.models.MigrationStatus.MigrationStatus
import com.prisma.shared.models.MigrationStepsJsonFormatter._
import com.prisma.utils.json.JsonUtils
import com.prisma.utils.json.JsonUtils._
import org.joda.time.format.ISODateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}
import play.api.libs.functional.syntax._
import play.api.libs.json._

object ProjectJsonFormatter {

  // ENUMS
  implicit lazy val relationSide        = enumFormat(RelationSide)
  implicit lazy val fieldConstraintType = enumFormat(FieldConstraintType)
  implicit lazy val modelMutationType   = enumFormat(ModelMutationType)

  implicit lazy val typeIdentifier = new Format[TypeIdentifier.TypeIdentifier] {
    override def reads(json: JsValue) = {
      json match {
        case JsString(str) => JsSuccess(TypeIdentifier.withName(str))
        case _             => JsError(s"$json is not a string and can therefore not be deserialized into an enum")
      }
    }

    override def writes(o: TypeIdentifier.TypeIdentifier) = JsString(o.code)
  }

  // MODELS
  implicit lazy val numberConstraint  = Json.format[NumberConstraint]
  implicit lazy val booleanConstraint = Json.format[BooleanConstraint]
  implicit lazy val stringConstraint  = Json.format[StringConstraint]
  implicit lazy val listConstraint    = Json.format[ListConstraint]
  implicit lazy val fieldConstraint = new Format[FieldConstraint] {
    val discriminatorField = "constraintType"

    override def reads(json: JsValue) = {
      for {
        constraintType <- (json \ discriminatorField).validate[FieldConstraintType]
      } yield {
        constraintType match {
          case FieldConstraintType.STRING  => json.as[StringConstraint]
          case FieldConstraintType.NUMBER  => json.as[NumberConstraint]
          case FieldConstraintType.BOOLEAN => json.as[BooleanConstraint]
          case FieldConstraintType.LIST    => json.as[ListConstraint]
          case unknown @ _                 => sys.error(s"Unmarshalling issue for FieldConstraintType with $unknown")
        }
      }
    }

    override def writes(o: FieldConstraint) = o match {
      case constraint: NumberConstraint  => addTypeDiscriminator(numberConstraint.writes(constraint), FieldConstraintType.NUMBER)
      case constraint: BooleanConstraint => addTypeDiscriminator(booleanConstraint.writes(constraint), FieldConstraintType.BOOLEAN)
      case constraint: StringConstraint  => addTypeDiscriminator(stringConstraint.writes(constraint), FieldConstraintType.STRING)
      case constraint: ListConstraint    => addTypeDiscriminator(listConstraint.writes(constraint), FieldConstraintType.LIST)
    }

    private def addTypeDiscriminator(jsObject: JsObject, constraintType: FieldConstraintType): JsValue = {
      jsObject + (discriminatorField -> fieldConstraintType.writes(constraintType))
    }
  }

  implicit lazy val gcValueFormat = new Format[GCValue] {
    val discriminatorField = "gcValueType"
    val isListField        = "isList"
    val valueField         = "value"
    val nullType           = "null"
    val stringType         = "string"
    val passwordType       = "password"
    val enumType           = "enum"
    val graphQlIdType      = "graphQlId"
    val uuidType           = "uuid"
    val dateTimeType       = "datetime"
    val intType            = "int"
    val floatType          = "float"
    val booleanType        = "bool"
    val jsonType           = "json"
    val listType           = "list"
    val rootType           = "root"

    override def reads(json: JsValue): JsResult[GCValue] = {
      for {
        discriminator <- (json \ discriminatorField).validate[String]
        value         <- (json \ valueField).validate[JsValue]
        isList        <- (json \ isListField).validate[Boolean]
        converted     <- createGcValue(discriminator, value, isList)
      } yield converted
    }

    private def createGcValue(discriminator: String, value: JsValue, isList: Boolean): JsResult[GCValue] = (discriminator, value) match {
      case (`nullType`, _)                  => JsSuccess(NullGCValue)
      case (`stringType`, JsString(str))    => JsSuccess(StringGCValue(str))
      case (`enumType`, JsString(str))      => JsSuccess(EnumGCValue(str))
      case (`graphQlIdType`, JsString(str)) => JsSuccess(StringIdGCValue(str))
      case (`uuidType`, JsString(str))      => JsSuccess(UuidGCValue(UUID.fromString(str)))
      case (`dateTimeType`, JsString(str))  => JsSuccess(DateTimeGCValue(new DateTime(str, DateTimeZone.UTC)))
      case (`intType`, JsNumber(x))         => JsSuccess(IntGCValue(x.toInt))
      case (`floatType`, JsNumber(x))       => JsSuccess(FloatGCValue(x.toDouble))
      case (`booleanType`, JsBoolean(x))    => JsSuccess(BooleanGCValue(x))
      case (`jsonType`, json)               => JsSuccess(JsonGCValue(json))
      case (_, JsArray(elements)) if isList =>
        val gcValues = elements.map(element => this.reads(element))
        gcValues.find(_.isError) match {
          case Some(error) => error
          case None        => JsSuccess(ListGCValue(gcValues.map(_.get).toVector))
        }
      case _ => JsError(s"invalid discriminator and value combination: $discriminator and value $value")
    }

    override def writes(gcValue: GCValue): JsValue = {
      val formatter = ISODateTimeFormat.dateHourMinuteSecondFraction()

      gcValue match {
        case NullGCValue        => json(nullType, JsNull)
        case x: StringGCValue   => json(stringType, JsString(x.value))
        case x: EnumGCValue     => json(enumType, JsString(x.value))
        case x: StringIdGCValue => json(graphQlIdType, JsString(x.value))
        case x: UuidGCValue     => json(uuidType, JsString(x.value.toString))
        case x: DateTimeGCValue => json(dateTimeType, JsString(formatter.print(x.value)))
        case x: IntGCValue      => json(intType, JsNumber(x.value))
        case x: FloatGCValue    => json(floatType, JsNumber(x.value))
        case x: BooleanGCValue  => json(booleanType, JsBoolean(x.value))
        case x: JsonGCValue     => json(jsonType, x.value)
        case x: ListGCValue     => json(listType, JsArray(x.values.map(this.writes)), isList = true)
        case x: RootGCValue     => json(rootType, JsObject(x.map.mapValues(this.writes)))
      }
    }

    private def json(discriminator: String, valueAsJson: JsValue, isList: Boolean = false): JsObject = {
      Json.obj(
        discriminatorField -> discriminator,
        isListField        -> isList,
        valueField         -> valueAsJson
      )
    }
  }

  implicit lazy val webhookDelivery = Json.format[WebhookDelivery]
  implicit lazy val functionDelivery = new OFormat[FunctionDelivery] {
    val discriminatorField = "type"

    override def reads(json: JsValue) = {
      (json \ discriminatorField).validate[String].map(FunctionDeliveryType.withName).flatMap {
        case FunctionDeliveryType.WebhookDelivery => webhookDelivery.reads(json)
      }
    }

    override def writes(delivery: FunctionDelivery) = {
      val objectJson = delivery match {
        case x: WebhookDelivery => webhookDelivery.writes(x)
      }
      addDiscriminator(objectJson, delivery)
    }

    private def addDiscriminator(json: JsObject, delivery: FunctionDelivery) = json ++ Json.obj(discriminatorField -> delivery.typeCode.toString)
  }

  implicit lazy val sssFn = Json.format[ServerSideSubscriptionFunction]
  implicit lazy val function = new OFormat[Function] {
    val discriminatorField = "type"

    override def reads(json: JsValue): JsResult[ServerSideSubscriptionFunction] = {
      (json \ discriminatorField).validate[String].map(FunctionType.withName).flatMap {
        case FunctionType.ServerSideSubscription => sssFn.reads(json)
      }
    }

    override def writes(fn: Function): JsObject = {
      val objectJson = fn match {
        case x: ServerSideSubscriptionFunction => sssFn.writes(x)
      }
      addDiscriminator(objectJson, fn)
    }

    private def addDiscriminator(json: JsObject, fn: Function) = json ++ Json.obj(discriminatorField -> fn.typeCode.toString)
  }

  val inlineRelationManifestationFormat: OFormat[EmbeddedRelationLink] = (
    (JsPath \ "inTableOfModelId").format[String] and
      (JsPath \ "referencingColumn").format[String]
  )(EmbeddedRelationLink.apply, unlift(EmbeddedRelationLink.unapply))

  val relationTableManifestationFormat: OFormat[RelationTable] = (
    (JsPath \ "table").format[String] and
      (JsPath \ "modelAColumn").format[String] and
      (JsPath \ "modelBColumn").format[String]
  )(RelationTable.apply, unlift(RelationTable.unapply))

  implicit lazy val relationManifestation = new Format[RelationLinkManifestation] {
    val discriminatorField = "relationManifestationType"
    val inlineRelationType = "inline"
    val relationTableType  = "relation_table"

    override def reads(json: JsValue) = (json \ discriminatorField).validate[String].flatMap {
      case `inlineRelationType` => inlineRelationManifestationFormat.reads(json)
      case `relationTableType`  => relationTableManifestationFormat.reads(json)
    }

    override def writes(mani: RelationLinkManifestation) = {
      mani match {
        case x: EmbeddedRelationLink => inlineRelationManifestationFormat.writes(x) ++ Json.obj(discriminatorField -> inlineRelationType)
        case x: RelationTable        => relationTableManifestationFormat.writes(x) ++ Json.obj(discriminatorField  -> relationTableType)
      }
    }
  }

  implicit val relationWrites: Writes[RelationTemplate] = (
    (JsPath \ "name").write[String] and
      (JsPath \ "modelAId").write[String] and
      (JsPath \ "modelBId").write[String] and
      (JsPath \ "modelAOnDelete").write[OnDelete.Value] and
      (JsPath \ "modelBOnDelete").write[OnDelete.Value] and
      (JsPath \ "manifestation").writeNullable[RelationLinkManifestation]
  )(unlift(RelationTemplate.unapply))

  implicit val relationReads: Reads[RelationTemplate] = (
    (JsPath \ "name").read[String] and
      (JsPath \ "modelAId").read[String] and
      (JsPath \ "modelBId").read[String] and
      (JsPath \ "modelAOnDelete").readNullable[OnDelete.Value].map(_.getOrElse(OnDelete.SetNull)) and
      (JsPath \ "modelBOnDelete").readNullable[OnDelete.Value].map(_.getOrElse(OnDelete.SetNull)) and
      (JsPath \ "manifestation").readNullable[RelationLinkManifestation]
  )(RelationTemplate.apply _)

  implicit val modelManifestationWrites: Writes[ModelManifestation] = Writes(manifestation => Json.obj("dbName" -> manifestation.dbName))
  implicit val modelManifestationReads: Reads[ModelManifestation]   = (JsPath \ "dbName").read[String].map(ModelManifestation)
  implicit val fieldManifestationWrites: Writes[FieldManifestation] = Writes(manifestation => Json.obj("dbName" -> manifestation.dbName))
  implicit val fieldManifestationReads: Reads[FieldManifestation]   = (JsPath \ "dbName").read[String].map(FieldManifestation)
  implicit lazy val enum                                            = Json.format[Enum]

  implicit val sequenceFormat = (
    (JsPath \ "name").format[String] and
      (JsPath \ "initialValue").format[Int] and
      (JsPath \ "allocationSize").format[Int]
  )(Sequence.apply, unlift(Sequence.unapply))

  implicit val fieldBehaviourFormat = new OFormat[FieldBehaviour] {
    val discriminatorField = "type"
    val strategyField      = "strategy"
    val sequenceField      = "sequence"
    val createdAtType      = "createdAt"
    val updatedAtType      = "updatedAt"
    val idType             = "id"
    val scalarListType     = "scalarList"

    override def writes(behaviour: FieldBehaviour) = {
      behaviour match {
        case CreatedAtBehaviour            => Json.obj(discriminatorField -> createdAtType)
        case UpdatedAtBehaviour            => Json.obj(discriminatorField -> updatedAtType)
        case ScalarListBehaviour(strategy) => Json.obj(discriminatorField -> scalarListType, strategyField -> strategy.entryName)
        case IdBehaviour(strategy, sequence) =>
          Json.obj(
            discriminatorField -> idType,
            strategyField      -> strategy.entryName,
            sequenceField      -> Json.toJson(sequence)
          )
      }
    }

    override def reads(json: JsValue): JsResult[FieldBehaviour] = {
      (json \ discriminatorField).validate[String].flatMap {
        case `createdAtType` => JsSuccess(CreatedAtBehaviour)
        case `updatedAtType` => JsSuccess(UpdatedAtBehaviour)
        case `idType` =>
          for {
            strategy <- (json \ strategyField).validate[String]
            sequence <- (json \ sequenceField).validateOpt[Sequence]
          } yield {
            IdBehaviour(IdStrategy.withName(strategy), sequence)
          }
        case `scalarListType` =>
          (json \ strategyField).validate[String].map { strategy =>
            ScalarListBehaviour(ScalarListStrategy.withName(strategy))
          }
      }
    }
  }

  implicit val fieldReads: Reads[FieldTemplate] = (
    (JsPath \ "name").read[String] and
      (JsPath \ "typeIdentifier").read[TypeIdentifier.Value] and
      (JsPath \ "isRequired").read[Boolean] and
      (JsPath \ "isList").read[Boolean] and
      (JsPath \ "isUnique").read[Boolean] and
      (JsPath \ "isHidden").read[Boolean] and
      (JsPath \ "isReadonly").read[Boolean] and
      (JsPath \ "isAutoGenerated").readNullable[Boolean].map(_.getOrElse(false)) and
      (JsPath \ "enum").readNullable[Enum] and
      (JsPath \ "defaultValue").readNullable[GCValue] and
      readEitherPathNullable[String](JsPath \ "relation" \ "name", JsPath \ "relationName") and
      (JsPath \ "relationSide").readNullable[RelationSide.Value] and
      (JsPath \ "manifestation").readNullable[FieldManifestation] and
      (JsPath \ "behaviour").readNullable[FieldBehaviour]
  )(FieldTemplate.apply _)

  implicit val fieldWrites: Writes[FieldTemplate] = (
    (JsPath \ "name").write[String] and
      (JsPath \ "typeIdentifier").write[TypeIdentifier.Value] and
      (JsPath \ "isRequired").write[Boolean] and
      (JsPath \ "isList").write[Boolean] and
      (JsPath \ "isUnique").write[Boolean] and
      (JsPath \ "isHidden").write[Boolean] and
      (JsPath \ "isReadonly").write[Boolean] and
      (JsPath \ "isAutoGenerated").write[Boolean] and
      (JsPath \ "enum").writeNullable[Enum] and
      (JsPath \ "defaultValue").writeNullable[GCValue] and
      (JsPath \ "relationName").writeNullable[String] and
      (JsPath \ "relationSide").writeNullable[RelationSide.Value] and
      (JsPath \ "manifestation").writeNullable[FieldManifestation] and
      (JsPath \ "behaviour").writeNullable[FieldBehaviour]
  )(unlift(FieldTemplate.unapply))

  implicit val indexReads: Reads[Index] = (
    (JsPath \ "fields").read[Vector[String]] and
      (JsPath \ "name").read[String]
  )(Index.apply _)

  implicit val indexWrites: Writes[Index] = (
    (JsPath \ "fields").write[Vector[String]] and
      (JsPath \ "name").write[String]
  )(unlift(Index.unapply))

  implicit val modelReads: Reads[ModelTemplate] = (
    (JsPath \ "name").read[String] and
      (JsPath \ "stableIdentifier").read[String] and
      (JsPath \ "isEmbedded").readNullable[Boolean].map(_.getOrElse(false)) and
      (JsPath \ "fields").read[List[FieldTemplate]] and
      (JsPath \ "manifestation").readNullable[ModelManifestation] and
      (JsPath \ "indexes").readNullable[Vector[Index]].map(_.getOrElse(Vector()))
  )(ModelTemplate.apply _)

  implicit val modelWrites: Writes[ModelTemplate] = (
    (JsPath \ "name").write[String] and
      (JsPath \ "stableIdentifier").write[String] and
      (JsPath \ "isEmbedded").write[Boolean] and
      (JsPath \ "fields").write[List[FieldTemplate]] and
      (JsPath \ "manifestation").writeNullable[ModelManifestation] and
      (JsPath \ "indexes").write[Vector[Index]]
  )(unlift(ModelTemplate.unapply))

  val schemaReads: Reads[Schema] = (
    (JsPath \ "models").read[List[ModelTemplate]] and
      (JsPath \ "relations").read[List[RelationTemplate]] and
      (JsPath \ "enums").read[List[Enum]] and
      (JsPath \ "version").readNullable[String]
  )(Schema.apply _)

  val schemaWrites: Writes[Schema] = (
    (JsPath \ "models").write[List[ModelTemplate]] and
      (JsPath \ "relations").write[List[RelationTemplate]] and
      (JsPath \ "enums").write[List[Enum]] and
      (JsPath \ "version").writeNullable[String]
  )(s => (s.modelTemplates, s.relationTemplates, s.enums, s.version))

  implicit lazy val schemaFormat          = Format(schemaReads, schemaWrites)
  implicit lazy val projectFormat         = Json.format[Project]
  implicit lazy val migrationStatusFormat = JsonUtils.enumFormat(MigrationStatus)

  val migrationReads: Reads[Migration] = for {
    projectId    <- (JsPath \ "projectId").read[String]
    revision     <- (JsPath \ "revision").read[Int]
    schema       <- (JsPath \ "schema").read[Schema]
    functions    <- (JsPath \ "functions").read[Vector[Function]]
    rawDataModel <- (JsPath \ "datamodel").readNullable[String]
    status       <- (JsPath \ "status").read[MigrationStatus]
    applied      <- (JsPath \ "applied").read[Int]
    rolledBack   <- (JsPath \ "rolledBack").read[Int]
    steps        <- (JsPath \ "steps").read[Vector[MigrationStep]]
    errors       <- (JsPath \ "errors").read[Vector[String]]
    startedAt    <- (JsPath \ "startedAt").readNullable[DateTime]
    finishedAt   <- (JsPath \ "finishedAt").readNullable[DateTime]
  } yield {
    Migration(
      projectId = projectId,
      revision = revision,
      schema = schema,
      functions = functions,
      rawDataModel = rawDataModel.getOrElse(""),
      status = status,
      applied = applied,
      rolledBack = rolledBack,
      steps = steps,
      errors = errors,
      startedAt = startedAt,
      finishedAt = finishedAt,
      previousSchema = Schema.empty
    )
  }

  val migrationWrites = OWrites.apply[Migration] { migration =>
    Json.obj(
      "projectId"  -> migration.projectId,
      "revision"   -> migration.revision,
      "schema"     -> migration.schema,
      "functions"  -> Json.toJson(migration.functions), // somehow this compiled only when written like that
      "datamodel"  -> migration.rawDataModel,
      "status"     -> migration.status,
      "applied"    -> migration.applied,
      "rolledBack" -> migration.rolledBack,
      "steps"      -> migration.steps,
      "errors"     -> migration.errors,
      "startedAt"  -> migration.startedAt,
      "finishedAt" -> migration.finishedAt
    )
  }

  implicit lazy val migrationFormat = OFormat(migrationReads, migrationWrites)

  def failingFormat[T] = new Format[T] {

    override def reads(json: JsValue) = fail
    override def writes(o: T)         = fail

    def fail = sys.error("This JSON Formatter always fails.")
  }

  def readEitherPathNullable[T](path1: JsPath, path2: JsPath)(implicit reads: Reads[T]): Reads[Option[T]] = {
    Reads { json =>
      val path1Lookup = path1.asSingleJson(json)
      val path2Lookup = path2.asSingleJson(json)
      (path1Lookup, path2Lookup) match {
        case (JsDefined(foundJson), _) => foundJson.validateOpt[T]
        case (_, JsDefined(foundJson)) => foundJson.validateOpt[T]
        case _                         => JsSuccess(None)
      }
    }
  }
}
