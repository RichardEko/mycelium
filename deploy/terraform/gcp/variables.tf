variable "project" {
  description = "GCP project ID (required — no default)."
  type        = string
}

variable "region" {
  description = "GCP region. A regional cluster spreads nodes across the region's zones."
  type        = string
  default     = "europe-west1"
}

variable "cluster_name" {
  description = "GKE cluster name (also the Artifact Registry repo id)."
  type        = string
  default     = "mycelium"
}

variable "node_machine_type" {
  description = "Machine type for the worker nodes Mycelium pods run on."
  type        = string
  default     = "e2-standard-2"
}

variable "node_min" {
  description = "Minimum nodes PER ZONE (a regional cluster spans ~3 zones, so total ≈ 3× this)."
  type        = number
  default     = 1
}

variable "node_max" {
  description = "Maximum nodes per zone — raise for a large Mycelium scale run (pods spread across these hosts)."
  type        = number
  default     = 2
}
