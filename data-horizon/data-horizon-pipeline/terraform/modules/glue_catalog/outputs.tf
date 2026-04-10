output "database_name" {
  description = "Glue Catalog database name"
  value       = aws_glue_catalog_database.data_horizon.name
}

output "raw_table_name" {
  description = "Glue Catalog raw data table name"
  value       = aws_glue_catalog_table.raw_data.name
}

output "cleaned_table_name" {
  description = "Glue Catalog cleaned data table name"
  value       = aws_glue_catalog_table.cleaned_data.name
}

output "parquet_table_name" {
  description = "Glue Catalog parquet data table name"
  value       = aws_glue_catalog_table.parquet_data.name
}
