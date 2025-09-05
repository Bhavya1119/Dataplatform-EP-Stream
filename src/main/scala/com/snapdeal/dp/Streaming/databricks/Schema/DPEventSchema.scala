package com.snapdeal.dp.Streaming.databricks.Schema

import com.snapdeal.dp.Streaming.databricks.Schema.Utils.DatabricksSchemaConverter.Field


/**
 * Created by Bhavya Joshi
 */

case class EventDetails (schema : SQLSchema, isTransactional : Boolean , partitionCols : String)

case class SQLSchema( eventKey : String , columns : java.util.List[Field])
