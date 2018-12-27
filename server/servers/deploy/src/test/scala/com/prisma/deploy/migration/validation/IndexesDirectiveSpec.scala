package com.prisma.deploy.migration.validation

import com.prisma.shared.models.ConnectorCapability.IndexesCapability
import org.scalatest.{Matchers, WordSpecLike}

class IndexesDirectiveSpec extends WordSpecLike with Matchers with DataModelValidationSpecBase {
  "without explicit directives should work" in {
    val dataModelString =
      """
        |type Model {
        |  id: ID! @id
        |}
      """.stripMargin
    val dataModel = validate(dataModelString, Set(IndexesCapability))
    dataModel.modelType_!("Model").indexes should be(empty)
  }

  "with a one column index should work" in {
    val dataModelString =
      """
        |type Model @indexes(value: [
        |  { fields: ["foo"] name: "Model_foo_idx" }
        |]) {
        |  id: ID! @id
        |  foo: String!
        |}
      """.stripMargin

    val dataModel = validate(dataModelString, Set(IndexesCapability))
    val indexes = dataModel.modelType_!("Model").indexes

    indexes.size should be(1)
    indexes should contain(PrismaIndex(fields = Vector("foo"), name = "Model_foo_idx"))
  }

  "with a two column indexes should work" in {
    val dataModelString =
      """
        |type Model @indexes(value: [
        |  { fields: ["foo"] name: "Model_foo_idx" }
        |  { fields: ["bar", "wtf"] name: "Model_bar_wtf_idx" }
        |]) {
        |  id: ID! @id
        |  foo: String!
        |}
      """.stripMargin

    val dataModel = validate(dataModelString, Set(IndexesCapability))
    val indexes = dataModel.modelType_!("Model").indexes

    indexes.size should be(2)
    indexes should contain(PrismaIndex(fields = Vector("foo"), name = "Model_foo_idx"))
    indexes should contain(PrismaIndex(fields = Vector("bar", "wtf"), name = "Model_bar_wtf_idx"))
  }

  "with a missing value should not work" in {
    val dataModelString =
      """
        |type Model @indexes {
        |  id: ID! @id
        |  foo: String!
        |}
      """.stripMargin

    val errors = validateThatMustError(dataModelString, Set(IndexesCapability))
    errors.size should be(1)

    val error = errors.head
    error.`type` should be("Model")
    error.description should include("The argument `value` is invalid in @indexes.")
  }

  "with a mistyped value should not work" in {
    val dataModelString =
      """
        |type Model @indexes(vlaue: [
        |  { fields: ["foo"] name: "Model_foo_idx" }
        |]) {
        |  id: ID! @id
        |  foo: String!
        |}
      """.stripMargin

    val errors = validateThatMustError(dataModelString, Set(IndexesCapability))
    errors.size should be(1)

    val error = errors.head
    error.`type` should be("Model")
    error.description should include("The argument `value` is invalid in @indexes.")
  }

  "with a missing name should not work" in {
    val dataModelString =
      """
        |type Model @indexes(value: [
        |  { fields: ["foo"] }
        |]) {
        |  id: ID! @id
        |  foo: String!
        |}
      """.stripMargin

    val errors = validateThatMustError(dataModelString, Set(IndexesCapability))
    errors.size should be(1)

    val error = errors.head
    error.`type` should be("Model")
    error.description should include("misses a required field `name`.")
  }

  "with missing fields should not work" in {
    val dataModelString =
      """
        |type Model @indexes(value: [
        |  { name: "Model_foo_idx" }
        |]) {
        |  id: ID! @id
        |  foo: String!
        |}
      """.stripMargin

    val errors = validateThatMustError(dataModelString, Set(IndexesCapability))
    errors.size should be(1)

    val error = errors.head
    error.`type` should be("Model")
    error.description should include("misses a required field `fields`.")
  }

  "with a mistyped name should not work" in {
    val dataModelString =
      """
        |type Model @indexes(value: [
        |  { fields: ["foo"] name: 1 }
        |]) {
        |  id: ID! @id
        |  foo: String!
        |}
      """.stripMargin

    val errors = validateThatMustError(dataModelString, Set(IndexesCapability))
    errors.size should be(1)

    val error = errors.head
    error.`type` should be("Model")
    error.description should include("This argument must be a String.")
  }

  "with a mistyped fields should not work" in {
    val dataModelString =
      """
        |type Model @indexes(value: [
        |  { fields: 1 name: "foo" }
        |]) {
        |  id: ID! @id
        |  foo: String!
        |}
      """.stripMargin

    val errors = validateThatMustError(dataModelString, Set(IndexesCapability))
    errors.size should be(1)

    val error = errors.head
    error.`type` should be("Model")
    error.description should include("This argument must be a List.")
  }

  "with a non-string field should not work" in {
    val dataModelString =
      """
        |type Model @indexes(value: [
        |  { fields: ["foo", 1] name: "Model_foo_idx" }
        |]) {
        |  id: ID! @id
        |  foo: String!
        |}
      """.stripMargin

    val errors = validateThatMustError(dataModelString, Set(IndexesCapability))
    errors.size should be(1)

    val error = errors.head
    error.`type` should be("Model")
    error.description should include("This argument must be a String.")
  }
}

