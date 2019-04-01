# Unit tests with existing schema

These unit tests ensure that prisma introspection leads the exact same schema as the prisma schema the database was created with.

The schemas are taken from the `generate-schema` module, the mysql dumps can be re-generated with the scripts suite that comes with `generate-schema`.
