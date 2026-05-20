output "network_id" {
  description = "ID of the FinFlow Docker network"
  value       = docker_network.finflow_network.id
}

output "postgres_volume_name" {
  description = "Name of the Postgres data Docker volume"
  value       = docker_volume.postgres_data.name
}
