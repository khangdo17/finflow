variable "project_name" {
  description = "Name of the FinFlow project, used as a prefix for resource names"
  type        = string
  default     = "finflow"
}

variable "network_name" {
  description = "Name of the Docker network for FinFlow services"
  type        = string
  default     = "finflow-network"
}
