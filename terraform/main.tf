terraform {
  required_providers {
    docker = {
      source  = "kreuzwerker/docker"
      version = "~> 3.0"
    }
  }
}

provider "docker" {}

resource "docker_network" "finflow_network" {
  name = var.network_name
}

resource "docker_volume" "postgres_data" {
  name = "${var.project_name}_postgres_data"
}

resource "docker_volume" "redis_data" {
  name = "${var.project_name}_redis_data"
}

resource "docker_volume" "airflow_logs" {
  name = "${var.project_name}_airflow_logs"
}
