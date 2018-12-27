package com.prisma.deploy.connector.jdbc.database

import com.prisma.deploy.connector._
import com.prisma.deploy.connector.jdbc.JdbcBase

import scala.concurrent.{ExecutionContext, Future}

case class JdbcDeployMutactionExecutor(builder: JdbcDeployDatabaseMutationBuilder)(implicit ec: ExecutionContext)
    extends JdbcBase
    with DeployMutactionExecutor {

  val slickDatabase = builder.slickDatabase

  override def execute(mutaction: DeployMutaction): Future[Unit] = {
    val action = mutaction match {
      case x: CreateProject         => CreateProjectInterpreter(builder).execute(x)
      case x: TruncateProject       => TruncateProjectInterpreter(builder).execute(x)
      case x: DeleteProject         => DeleteProjectInterpreter(builder).execute(x)
      case x: CreateColumn          => CreateColumnInterpreter(builder).execute(x)
      case x: UpdateColumn          => UpdateColumnInterpreter(builder).execute(x)
      case x: DeleteColumn          => DeleteColumnInterpreter(builder).execute(x)
      case x: CreateScalarListTable => CreateScalarListInterpreter(builder).execute(x)
      case x: UpdateScalarListTable => UpdateScalarListInterpreter(builder).execute(x)
      case x: DeleteScalarListTable => DeleteScalarListInterpreter(builder).execute(x)
      case x: CreateModelTable      => CreateModelInterpreter(builder).execute(x)
      case x: RenameTable           => RenameModelInterpreter(builder).execute(x)
      case x: DeleteModelTable      => DeleteModelInterpreter(builder).execute(x)
      case x: CreateRelationTable   => CreateRelationInterpreter(builder).execute(x)
      case x: UpdateRelationTable   => UpdateRelationInterpreter(builder).execute(x)
      case x: DeleteRelationTable   => DeleteRelationInterpreter(builder).execute(x)
      case x: CreateInlineRelation  => CreateInlineRelationInterpreter(builder).execute(x)
      case x: DeleteInlineRelation  => DeleteInlineRelationInterpreter(builder).execute(x)
      case x: CreateTableIndex      => CreateIndexInterpreter(builder).execute(x)
      case x: DeleteTableIndex      => DeleteIndexInterpreter(builder).execute(x)
      case x: AlterTableIndex       => AlterIndexInterpreter(builder).execute(x)
    }

    database.run(action).map(_ => ())
  }

  override def rollback(mutaction: DeployMutaction): Future[Unit] = {
    val action = mutaction match {
      case x: CreateProject         => CreateProjectInterpreter(builder).rollback(x)
      case x: TruncateProject       => TruncateProjectInterpreter(builder).rollback(x)
      case x: DeleteProject         => DeleteProjectInterpreter(builder).rollback(x)
      case x: CreateColumn          => CreateColumnInterpreter(builder).rollback(x)
      case x: UpdateColumn          => UpdateColumnInterpreter(builder).rollback(x)
      case x: DeleteColumn          => DeleteColumnInterpreter(builder).rollback(x)
      case x: CreateScalarListTable => CreateScalarListInterpreter(builder).rollback(x)
      case x: UpdateScalarListTable => UpdateScalarListInterpreter(builder).rollback(x)
      case x: DeleteScalarListTable => DeleteScalarListInterpreter(builder).rollback(x)
      case x: CreateModelTable      => CreateModelInterpreter(builder).rollback(x)
      case x: RenameTable           => RenameModelInterpreter(builder).rollback(x)
      case x: DeleteModelTable      => DeleteModelInterpreter(builder).rollback(x)
      case x: CreateRelationTable   => CreateRelationInterpreter(builder).rollback(x)
      case x: UpdateRelationTable   => UpdateRelationInterpreter(builder).rollback(x)
      case x: DeleteRelationTable   => DeleteRelationInterpreter(builder).rollback(x)
      case x: CreateInlineRelation  => CreateInlineRelationInterpreter(builder).rollback(x)
      case x: DeleteInlineRelation  => DeleteInlineRelationInterpreter(builder).rollback(x)
      case x: CreateTableIndex      => CreateIndexInterpreter(builder).rollback(x)
      case x: DeleteTableIndex      => DeleteIndexInterpreter(builder).rollback(x)
      case x: AlterTableIndex       => AlterIndexInterpreter(builder).rollback(x)
    }

    database.run(action).map(_ => ())
  }
}
