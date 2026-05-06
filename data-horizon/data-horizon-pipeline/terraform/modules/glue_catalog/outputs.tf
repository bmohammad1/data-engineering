output "database_name" {
  description = "Glue Catalog database name"
  value       = aws_glue_catalog_database.data_horizon.name
}

output "raw_table_name" {
  description = "Glue Catalog raw data table name"
  value       = aws_glue_catalog_table.raw_data.name
}

output "cleaned_tables_prefix" {
  description = "Glue Catalog name prefix shared by all cleaned tables (cleaned_<table>)"
  value       = "cleaned"
}

output "validated_tables_prefix" {
  description = "Glue Catalog name prefix shared by all validated tables (validated_<table>)"
  value       = "validated"
}
