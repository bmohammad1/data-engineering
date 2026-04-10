resource "aws_glue_catalog_database" "data_horizon" {
  name = replace("${var.name_prefix}_db", "-", "_")

  description = "Data Horizon pipeline catalog database"
}

resource "aws_glue_catalog_table" "raw_data" {
  database_name = aws_glue_catalog_database.data_horizon.name
  name          = "raw_data"
  description   = "Raw API response data (JSON)"

  table_type = "EXTERNAL_TABLE"

  parameters = {
    classification = "json"
  }

  storage_descriptor {
    location      = "s3://${var.raw_bucket_name}/raw/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"

    ser_de_info {
      serialization_library = "org.openx.data.jsonserde.JsonSerDe"
    }

    columns {
      name = "tag_id"
      type = "string"
    }

    columns {
      name = "source_name"
      type = "string"
    }

    columns {
      name = "ingested_at"
      type = "timestamp"
    }

    columns {
      name = "data"
      type = "string"
    }
  }
}

resource "aws_glue_catalog_table" "cleaned_data" {
  database_name = aws_glue_catalog_database.data_horizon.name
  name          = "cleaned_data"
  description   = "Cleaned and transformed data (JSON)"

  table_type = "EXTERNAL_TABLE"

  parameters = {
    classification = "json"
  }

  storage_descriptor {
    location      = "s3://${var.cleaned_bucket_name}/cleaned/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"

    ser_de_info {
      serialization_library = "org.openx.data.jsonserde.JsonSerDe"
    }

    columns {
      name = "tag_id"
      type = "string"
    }

    columns {
      name = "source_name"
      type = "string"
    }

    columns {
      name = "transformed_at"
      type = "timestamp"
    }

    columns {
      name = "data"
      type = "string"
    }
  }
}

resource "aws_glue_catalog_table" "parquet_data" {
  database_name = aws_glue_catalog_database.data_horizon.name
  name          = "parquet_data"
  description   = "Validated data in Parquet format"

  table_type = "EXTERNAL_TABLE"

  parameters = {
    classification = "parquet"
  }

  storage_descriptor {
    location      = "s3://${var.parquet_bucket_name}/parquet/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name = "tag_id"
      type = "string"
    }

    columns {
      name = "source_name"
      type = "string"
    }

    columns {
      name = "validated_at"
      type = "timestamp"
    }

    columns {
      name = "data"
      type = "string"
    }
  }
}
