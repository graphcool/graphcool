package com.prisma.shared.models

import play.api.libs.functional.syntax._
import play.api.libs.json._

import scala.language.implicitConversions

object MigrationStepsJsonFormatter extends DefaultReads {
  implicit val createModelFormat = Json.format[CreateModel]
  implicit val deleteModelFormat = Json.format[DeleteModel]
  implicit val updateModelFormat = new OFormat[UpdateModel] {
    val oldNameField = "name"
    val newNameField = "newName"

    override def reads(json: JsValue): JsResult[UpdateModel] = {
      for {
        name    <- (json \ oldNameField).validate[String]
        newName <- (json \ newNameField).validate[String]
      } yield { UpdateModel(name, newName) }
    }

    override def writes(o: UpdateModel): JsObject = Json.obj(oldNameField -> o.name, newNameField -> o.newName)
  }

  implicit val createFieldFormat = Json.format[CreateField]
  implicit val deleteFieldFormat = Json.format[DeleteField]
  implicit val updateFieldFormat = new OFormat[UpdateField] {
    val modelField    = "model"
    val newModelField = "newModel"
    val nameField     = "name"
    val newNameField  = "newName"

    override def reads(json: JsValue): JsResult[UpdateField] = {
      for {
        model    <- (json \ modelField).validate[String]
        newModel <- (json \ newModelField).validateOpt[String]
        name     <- (json \ nameField).validate[String]
        newName  <- (json \ newNameField).validateOpt[String]
      } yield {
        UpdateField(
          model = model,
          newModel = newModel.getOrElse(model),
          name = name,
          newName = newName
        )
      }
    }

    override def writes(x: UpdateField): JsObject = {
      Json.obj(
        modelField    -> x.model,
        newModelField -> x.newModel,
        nameField     -> x.name,
        newNameField  -> x.newName
      )
    }
  }

  object EnumUtils {
    def enumReads[E <: Enumeration](enum: E): Reads[E#Value] = {
      case JsString(s) =>
        try {
          JsSuccess(enum.withName(s))
        } catch {
          case _: NoSuchElementException => JsError(s"Enumeration expected of type: '${enum.getClass}', but it does not appear to contain the value: '$s'")
        }
      case _ => JsError("String value expected")
    }

    implicit def enumWrites[E <: Enumeration]: Writes[E#Value] = (v: E#Value) => JsString(v.toString)

    implicit def enumFormat[E <: Enumeration](enum: E): Format[E#Value] = {
      Format(EnumUtils.enumReads(enum), EnumUtils.enumWrites)
    }
  }

  implicit val onDeleteEnumTypeFormat = EnumUtils.enumFormat(OnDelete)

  implicit val createEnumFormat = Json.format[CreateEnum]
  implicit val deleteEnumFormat = Json.format[DeleteEnum]
  implicit val updateEnumFormat = Json.format[UpdateEnum]

  implicit val createRelationFormat: OFormat[CreateRelation] = {
    val reads  = (JsPath \ "name").read[String].map(CreateRelation.apply)
    val writes = (JsPath \ "name").write[String].contramap(unlift(CreateRelation.unapply))
    OFormat(reads, writes)
  }

  implicit val deleteRelationFormat: OFormat[DeleteRelation] = {
    val reads  = (JsPath \ "name").read[String].map(DeleteRelation.apply)
    val writes = (JsPath \ "name").write[String].contramap(unlift(DeleteRelation.unapply))
    OFormat(reads, writes)
  }

  implicit val updateRelationFormat: OFormat[UpdateRelation] = {
    val format: OFormat[UpdateRelation] = (
      (JsPath \ "name").format[String] and
        (JsPath \ "newName").formatNullable[String]
    )(UpdateRelation.apply, unlift(UpdateRelation.unapply))

    format
  }

  implicit val updateSecretsFormat: OFormat[UpdateSecrets] = {
    val reads  = (JsPath \ "secrets").read[Vector[String]].map(UpdateSecrets.apply)
    val writes = OWrites[UpdateSecrets](update => Json.obj("secrets" -> update.secrets))
    OFormat(reads, writes)
  }

  implicit val createIndexFormat: OFormat[CreateIndex] = {
    val format: OFormat[CreateIndex] = (
      (JsPath \ "modelName").format[String] and
        (JsPath \ "indexName").format[String]
      )(CreateIndex.apply, unlift(CreateIndex.unapply))

    format
  }

  implicit val deleteIndexFormat: OFormat[DeleteIndex] = {
    val format: OFormat[DeleteIndex] = (
      (JsPath \ "modelName").format[String] and
        (JsPath \ "indexName").format[String]
      )(DeleteIndex.apply, unlift(DeleteIndex.unapply))

    format
  }

  implicit val alterIndexFormat: OFormat[AlterIndex] = {
    val format: OFormat[AlterIndex] =
      ((JsPath \ "modelName").format[String] and
        (JsPath \ "oldName").format[String] and
        (JsPath \ "newName").format[String]
        )(AlterIndex.apply, unlift(AlterIndex.unapply))

    format
  }

  implicit val migrationStepFormat: Format[MigrationStep] = new Format[MigrationStep] {
    val discriminatorField = "discriminator"

    override def reads(json: JsValue): JsResult[MigrationStep] = {
      (json \ discriminatorField).validate[String].flatMap {
        case "CreateModel"    => createModelFormat.reads(json)
        case "DeleteModel"    => deleteModelFormat.reads(json)
        case "UpdateModel"    => updateModelFormat.reads(json)
        case "CreateField"    => createFieldFormat.reads(json)
        case "DeleteField"    => deleteFieldFormat.reads(json)
        case "UpdateField"    => updateFieldFormat.reads(json)
        case "CreateEnum"     => createEnumFormat.reads(json)
        case "DeleteEnum"     => deleteEnumFormat.reads(json)
        case "UpdateEnum"     => updateEnumFormat.reads(json)
        case "CreateRelation" => createRelationFormat.reads(json)
        case "DeleteRelation" => deleteRelationFormat.reads(json)
        case "UpdateRelation" => updateRelationFormat.reads(json)
        case "UpdateSecrets"  => updateSecretsFormat.reads(json)
        case "CreateIndex"    => createIndexFormat.reads(json)
        case "DeleteIndex"    => deleteIndexFormat.reads(json)
        case "AlterIndex"     => alterIndexFormat.reads(json)
      }
    }

    override def writes(step: MigrationStep): JsValue = {
      val withOutDiscriminator = step match {
        case x: CreateModel    => createModelFormat.writes(x)
        case x: DeleteModel    => deleteModelFormat.writes(x)
        case x: UpdateModel    => updateModelFormat.writes(x)
        case x: CreateField    => createFieldFormat.writes(x)
        case x: DeleteField    => deleteFieldFormat.writes(x)
        case x: UpdateField    => updateFieldFormat.writes(x)
        case x: CreateEnum     => createEnumFormat.writes(x)
        case x: DeleteEnum     => deleteEnumFormat.writes(x)
        case x: UpdateEnum     => updateEnumFormat.writes(x)
        case x: CreateRelation => createRelationFormat.writes(x)
        case x: DeleteRelation => deleteRelationFormat.writes(x)
        case x: UpdateRelation => updateRelationFormat.writes(x)
        case x: UpdateSecrets  => updateSecretsFormat.writes(x)
        case x: CreateIndex    => createIndexFormat.writes(x)
        case x: DeleteIndex    => deleteIndexFormat.writes(x)
        case x: AlterIndex     => alterIndexFormat.writes(x)
      }
      withOutDiscriminator ++ Json.obj(discriminatorField -> step.getClass.getSimpleName)
    }
  }

  def writeDoubleOpt[T](field: String, opt: Option[Option[T]])(implicit writes: Writes[T]): JsObject = {
    opt match {
      case Some(innerOpt) => JsObject(Vector(field -> Json.toJson(innerOpt)))
      case None           => JsObject.empty
    }
  }

  implicit class JsLookupResultExtension(jsLookUp: JsLookupResult) {
    def validateDoubleOpt[T](implicit rds: Reads[T]): JsResult[Option[Option[T]]] = jsLookUp match {
      case JsUndefined()     => JsSuccess(None)
      case JsDefined(JsNull) => JsSuccess(Some(None))
      case JsDefined(json)   => rds.reads(json).map(v => Some(Some(v)))
    }
  }

  def readOneOf[T](field1: String, field2: String)(implicit reads: Reads[T]) = (JsPath \ field1).read[T].orElse((JsPath \ field2).read[T])
}
