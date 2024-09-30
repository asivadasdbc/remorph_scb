package com.databricks.labs.remorph.discovery

import com.databricks.labs.remorph.parsers.intermediate.{DataType, StructField}
import com.databricks.labs.remorph.parsers.snowflake.{SnowflakeLexer, SnowflakeParser}
import org.antlr.v4.runtime.{CharStreams, CommonTokenStream}
import com.databricks.labs.remorph.parsers.snowflake.SnowflakeTypeBuilder

import java.sql.Connection
import scala.collection.mutable

class TSqlTableDefinitions(conn: Connection) {

  /**
   * Parses a data type string and returns the corresponding DataType object.
   *
   * @param dataTypeString The string representation of the data type.
   * @return The DataType object corresponding to the input string.
   */
  private def getDataType(dataTypeString: String): DataType = {
    val inputString = CharStreams.fromString(dataTypeString)
    val lexer = new SnowflakeLexer(inputString)
    val tokenStream = new CommonTokenStream(lexer)
    val parser = new SnowflakeParser(tokenStream)
    val ctx = parser.dataType()
    val dataTypeBuilder = new SnowflakeTypeBuilder
    dataTypeBuilder.buildDataType(ctx)
  }

  private def getTableDefinitionQuery(catalogName: String): String = {
    s"""WITH column_info AS (
       |    SELECT
       |        TABLE_CATALOG,
       |        TABLE_SCHEMA,
       |        TABLE_NAME,
       |        STRING_AGG(
       |                CONCAT(
       |                        column_name, ':',
       |                        CASE
       |                            WHEN numeric_precision IS NOT NULL AND numeric_scale IS NOT NULL
       |                                THEN CONCAT(data_type, '(', numeric_precision, ',', numeric_scale, ')')
       |                            WHEN LOWER(data_type) = 'text'
       |                                THEN CONCAT('varchar', '(', CHARACTER_MAXIMUM_LENGTH, ')')
       |                            ELSE data_type
       |                            END,
       |                        ':',
       |                        CASE WHEN IS_NULLABLE = 'YES' THEN 'true' ELSE 'false' END
       |                ),
       |                '~'
       |        ) WITHIN GROUP (ORDER BY ordinal_position) AS DERIVED_SCHEMA
       |    FROM
       |        ${catalogName}.INFORMATION_SCHEMA.COLUMNS
       |    GROUP BY
       |        TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME
       |)
       |
       |, table_file_info AS (
       |    SELECT
       |        s.name AS TABLE_SCHEMA,
       |        t.name AS TABLE_NAME,
       |        f.physical_name AS location,
       |        f.type_desc AS TABLE_FORMAT,
       |        CAST(ROUND(SUM(a.used_pages) * 8.0 / 1024, 2) AS DECIMAL(18,2)) AS SIZE_GB
       |    FROM
       |        ${catalogName}.sys.tables t
       |            INNER JOIN
       |        ${catalogName}.sys.indexes i ON t.object_id = i.object_id
       |            INNER JOIN
       |        ${catalogName}.sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
       |            INNER JOIN
       |        ${catalogName}.sys.allocation_units a ON p.partition_id = a.container_id
       |            INNER JOIN
       |        ${catalogName}.sys.schemas s ON t.schema_id = s.schema_id
       |            INNER JOIN
       |        ${catalogName}.sys.database_files f ON a.data_space_id = f.data_space_id
       |    GROUP BY
       |        s.name, t.name, f.name, f.physical_name, f.type_desc
       |)
       |SELECT
       |    sft.TABLE_CATALOG,
       |    sft.TABLE_SCHEMA,
       |    sft.TABLE_NAME,
       |    tfi.location,
       |    tfi.TABLE_FORMAT,
       |    sfv.view_definition,
       |    column_info.DERIVED_SCHEMA,
       |    tfi.SIZE_GB
       |FROM
       |    column_info
       |        JOIN ${catalogName}.INFORMATION_SCHEMA.TABLES sft
       |             ON column_info.TABLE_CATALOG = sft.TABLE_CATALOG
       |                 AND column_info.TABLE_SCHEMA = sft.TABLE_SCHEMA
       |                 AND column_info.TABLE_NAME = sft.TABLE_NAME
       |        LEFT JOIN ${catalogName}.INFORMATION_SCHEMA.VIEWS sfv
       |                  ON column_info.TABLE_CATALOG = sfv.TABLE_CATALOG
       |                      AND column_info.TABLE_SCHEMA = sfv.TABLE_SCHEMA
       |                      AND column_info.TABLE_NAME = sfv.TABLE_NAME
       |        LEFT JOIN table_file_info tfi
       |                  ON column_info.TABLE_SCHEMA = tfi.TABLE_SCHEMA
       |                      AND column_info.TABLE_NAME = tfi.TABLE_NAME
       |ORDER BY
       |    sft.TABLE_CATALOG, sft.TABLE_SCHEMA, sft.TABLE_NAME;
       |""".stripMargin
  }

  /**
   * Retrieves the definitions of all tables in the Snowflake database.
   *
   * @return A sequence of TableDefinition objects representing the tables in the database.
   */
  private def getTableDefinitions(catalogName: String): Seq[TableDefinition] = {
    val stmt = conn.createStatement()
    try {
      val tableDefinitionList = new mutable.ListBuffer[TableDefinition]()
      val rs = stmt.executeQuery(getTableDefinitionQuery(catalogName))
      try {
        while (rs.next()) {
          val tableSchema = rs.getString("TABLE_SCHEMA")
          val tableName = rs.getString("TABLE_NAME")
          val tableCatalog = rs.getString("TABLE_CATALOG")
          val columns = rs
            .getString("DERIVED_SCHEMA")
            .split("~")
            .map(x => {
              val data = x.split(":")
              val name = data(0)
              val dataType = getDataType(data(1))
              StructField(name, dataType, data(2).toBoolean)
            })
          tableDefinitionList.append(
            TableDefinition(
              tableCatalog,
              tableSchema,
              tableName,
              Option(rs.getString("LOCATION")),
              Option(rs.getString("TABLE_FORMAT")),
              Option(rs.getString("VIEW_DEFINITION")),
              columns,
              rs.getInt("SIZE_GB")))
        }
        tableDefinitionList
      } finally {
        rs.close()
      }
    } finally {
      stmt.close()
    }
  }

  def getAllTableDefinitions: mutable.Seq[TableDefinition] = {
    getAllCatalogs.flatMap(getTableDefinitions)
  }

  def getAllSchemas(catalogName: String): mutable.ListBuffer[String] = {
    val stmt = conn.createStatement()
    try {
      val rs = stmt.executeQuery(s"""select SCHEMA_NAME from ${catalogName}.INFORMATION_SCHEMA.SCHEMATA""")
      try {
        val schemaList = new mutable.ListBuffer[String]()
        while (rs.next()) {
          schemaList.append(rs.getString("SCHEMA_NAME"))
        }
        schemaList
      } catch {
        case e: Exception =>
          e.printStackTrace()
          throw e
      } finally {
        rs.close()
      }
    } finally {
      stmt.close()
    }
  }

  def getAllCatalogs: mutable.ListBuffer[String] = {
    val stmt = conn.createStatement()
    try {
      val rs = stmt.executeQuery("SELECT NAME FROM sys.databases WHERE NAME != 'MASTER'")
      try {
        val catalogList = new mutable.ListBuffer[String]()
        while (rs.next()) {
          catalogList.append(rs.getString("name"))
        }
        catalogList
      } finally {
        rs.close()
      }
    } finally {
      stmt.close()
    }
  }

}
