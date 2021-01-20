package com.prisma.api.schema

import com.prisma.api.connector._

object Optimizations {

  trait Optimizer {
    def optimize(filter: Filter): Filter

  }

  object FilterOptimizer extends Optimizer {

    override def optimize(filter: Filter): Filter = {
      val optimizations = Vector(RemoveSuperflousFilters, SameRelationFilterOpt)
      optimizations.foldLeft(filter)((current, opt) => opt.transform(current))
    }

    trait Optimization {
      def transform(filter: Filter): Filter
    }

    object RemoveSuperflousFilters extends Optimization {
      // This removes Logical Filters AND/OR when they are only wrapping one nested filter
      // Doing this first allows us to simplify the logic of all later Optimization stages
      // since they do not need to handle these cases anymore
      override def transform(filter: Filter): Filter = {
        filter match {
          case AndFilter(filters) if filters.length == 1   => transform(filters.head)
          case AndFilter(filters)                          => AndFilter(filters.map(transform))
          case OrFilter(filters) if filters.length == 1    => transform(filters.head)
          case OrFilter(filters)                           => OrFilter(filters.map(transform))
          case NotFilter(filters)                          => NotFilter(filters.map(transform))
          case RelationFilter(rf, nestedFilter, condition) => RelationFilter(rf, transform(nestedFilter), condition)
          case x                                           => x
        }
      }
    }

    object InlineOpt extends Optimization {

      //For Mongo this could also handle rf_some{id: "id"} => ScalarListFilter(ScalarListField, ListContains("id"))
      //We run into problems with the native versions when enabling this, since they actually verify that the scalarField is on the Model
      //converting the Relationfilter to a ScalarFilter on a `virtual` ScalarField does not work for them

      //  !!!!!!!! This is currently not active since the Rust side of things does not allow rf.scalarCopy since it actually checks for
      // the ScalarField on the Model and then fails.

      override def transform(filter: Filter): Filter = {
        filter match {
          case AndFilter(filters) => AndFilter(filters.map(transform))
          case OrFilter(filters)  => OrFilter(filters.map(transform))
          case NotFilter(filters) => NotFilter(filters.map(transform))
          case RelationFilter(rf, ScalarFilter(sf, scalarCondition), _) if rf.relationIsInlinedInParent && !rf.isList && sf.isId =>
            ScalarFilter(rf.scalarCopy, scalarCondition)
          case RelationFilter(rf, nestedFilter, cond) => RelationFilter(rf, transform(nestedFilter), cond)
          case x                                      => x
        }
      }
    }

    object SameRelationFilterOpt extends Optimization {
      //needs to be merged for Mongo since our join implementation returns wrong results otherwise
      //merging can provide for simpler native queries in other connectors as well

      //
      //relationFilter / operator     AND                   OR                  NOT
      //toOneRelation                 a: merge with AND
      //_some                         b: merge with OR
      //_every
      //_none
      //

      def mergeRelationFilterGroups(filters: Vector[Filter]): Vector[Filter] = {

        val toOneRelationFilters = filters.collect {
          case x: RelationFilter if x.condition == ToOneRelatedNode && x.nestedFilter.isInstanceOf[ScalarFilter] => x
        }

        val toOneGroups = toOneRelationFilters.map { filter =>
          toOneRelationFilters.filter { other =>
            other.field == filter.field && other.condition == filter.condition
          }
        }.distinct

        val mergedToOneRelationFilters = toOneGroups.map {
          case group if group.length == 1 => group.head
          case group =>
            val head   = group.head
            val nested = AndFilter(group.map(_.nestedFilter))
            RelationFilter(head.field, nested, head.condition)
        }

        val atLeastOneRelationFilters = filters.collect {
          case x: RelationFilter if x.condition == AtLeastOneRelatedNode && x.nestedFilter.isInstanceOf[ScalarFilter] => x
        }

        val atLeastOneGroups = atLeastOneRelationFilters.map { filter =>
          atLeastOneRelationFilters.filter { other =>
            other.field == filter.field && other.condition == filter.condition
          }
        }.distinct

        val mergedAtLeastOneFilters = atLeastOneGroups.map {
          case group if group.length == 1 => group.head
          case group =>
            val head   = group.head
            val nested = OrFilter(group.map(_.nestedFilter))
            RelationFilter(head.field, nested, head.condition)
        }

        val otherFilters = filters.filter(filter => !(toOneRelationFilters ++ atLeastOneRelationFilters).contains(filter))

        mergedToOneRelationFilters ++ mergedAtLeastOneFilters ++ otherFilters
      }

      override def transform(filter: Filter): Filter = {
        filter match {
          case AndFilter(filters)                     => AndFilter(mergeRelationFilterGroups(filters.map(transform)))
          case OrFilter(filters)                      => OrFilter(filters.map(transform))
          case NotFilter(filters)                     => NotFilter(filters.map(transform))
          case RelationFilter(rf, nestedFilter, cond) => RelationFilter(rf, transform(nestedFilter), cond)
          case x                                      => x
        }
      }
    }

  }

}
