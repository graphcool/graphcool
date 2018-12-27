package com.prisma.shared.models

import com.prisma.shared.models.ConnectorCapability.{EmbeddedScalarListsCapability, NonEmbeddedScalarListCapability, ScalarListsCapability}
import com.prisma.utils.boolean.BooleanUtils
import enumeratum.{EnumEntry, Enum => Enumeratum}

sealed trait ConnectorCapability extends EnumEntry

object ConnectorCapability extends Enumeratum[ConnectorCapability] {
  val values = findValues

  sealed trait ScalarListsCapability     extends ConnectorCapability
  object ScalarListsCapability           extends ScalarListsCapability
  object EmbeddedScalarListsCapability   extends ScalarListsCapability
  object NonEmbeddedScalarListCapability extends ScalarListsCapability

  object NodeQueryCapability extends ConnectorCapability

  object EmbeddedTypesCapability       extends ConnectorCapability
  object JoinRelationsFilterCapability extends ConnectorCapability

  object ImportExportCapability extends ConnectorCapability

  object TransactionalExecutionCapability extends ConnectorCapability

  object SupportsExistingDatabasesCapability extends ConnectorCapability
  object MigrationsCapability                extends ConnectorCapability
  object LegacyDataModelCapability           extends ConnectorCapability
  object IntrospectionCapability             extends ConnectorCapability
  object JoinRelationLinksCapability         extends ConnectorCapability // the ability to join using relation links
  object MongoJoinRelationLinksCapability    extends ConnectorCapability // does not allow NOT/OR and only _some on manyrelations
  object RelationLinkListCapability          extends ConnectorCapability // relation links can be stored inline in a node in a list
  object RelationLinkTableCapability         extends ConnectorCapability // relation links are stored in a table
  object IndexesCapability                   extends ConnectorCapability

  // RawAccessCapability

  sealed trait IdCapability   extends ConnectorCapability
  object IntIdCapability      extends IdCapability
  object UuidIdCapability     extends IdCapability
  object IdSequenceCapability extends IdCapability

}

case class ConnectorCapabilities(capabilities: Set[ConnectorCapability]) {
  def has(capability: ConnectorCapability): Boolean = capability match {
    case ScalarListsCapability => capabilities.contains(EmbeddedScalarListsCapability) || capabilities.contains(NonEmbeddedScalarListCapability)
    case x                     => capabilities.contains(x)
  }
  def hasNot(capability: ConnectorCapability): Boolean = !has(capability)

  def supportsScalarLists = capabilities.exists(_.isInstanceOf[ScalarListsCapability])
}

object ConnectorCapabilities extends BooleanUtils {
  val empty: ConnectorCapabilities                                     = ConnectorCapabilities(Set.empty[ConnectorCapability])
  def apply(capabilities: ConnectorCapability*): ConnectorCapabilities = ConnectorCapabilities(Set(capabilities: _*))

  import com.prisma.shared.models.ConnectorCapability._

  def mysql: ConnectorCapabilities = {
    val capas = Set(
      LegacyDataModelCapability,
      TransactionalExecutionCapability,
      JoinRelationsFilterCapability,
      JoinRelationLinksCapability,
      RelationLinkTableCapability,
      MigrationsCapability,
      NonEmbeddedScalarListCapability,
      NodeQueryCapability,
      ImportExportCapability
    )
    ConnectorCapabilities(capas)
  }

  def postgres(isActive: Boolean): ConnectorCapabilities = {
    val common = Set(
      LegacyDataModelCapability,
      TransactionalExecutionCapability,
      JoinRelationsFilterCapability,
      JoinRelationLinksCapability,
      RelationLinkTableCapability,
      IntrospectionCapability,
      IntIdCapability,
      UuidIdCapability,
      IndexesCapability
    )
    val capas = if (isActive) {
      common ++ Set(MigrationsCapability, NonEmbeddedScalarListCapability, NodeQueryCapability, ImportExportCapability)
    } else {
      common ++ Set(SupportsExistingDatabasesCapability)
    }
    ConnectorCapabilities(capas)
  }

  def mongo(isActive: Boolean, isTest: Boolean): ConnectorCapabilities = {
    val common = Set(
      NodeQueryCapability,
      EmbeddedScalarListsCapability,
      EmbeddedTypesCapability,
      JoinRelationLinksCapability,
      MongoJoinRelationLinksCapability,
      RelationLinkListCapability,
      EmbeddedTypesCapability
    )
    val dataModelCapability = isTest.toOption(LegacyDataModelCapability)

    ConnectorCapabilities(common ++ dataModelCapability)
  }
}
