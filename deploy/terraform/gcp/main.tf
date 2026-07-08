provider "google" {
  project = var.project
  region  = var.region
}

# A regional GKE cluster: nodes span the region's zones, so Mycelium pods distribute
# across real hosts in multiple zones. The default node pool is removed and replaced by a
# managed pool we control — the canonical Terraform GKE pattern (the default pool can't be
# edited in place).
resource "google_container_cluster" "mycelium" {
  name     = var.cluster_name
  location = var.region

  remove_default_node_pool = true
  initial_node_count       = 1

  deletion_protection = false # allow `terraform destroy` for a demo/scale cluster
}

resource "google_container_node_pool" "workers" {
  name     = "workers"
  location = var.region
  cluster  = google_container_cluster.mycelium.name

  # min/max are PER ZONE; a regional cluster spans ~3 zones, so total nodes ≈ 3× these.
  autoscaling {
    min_node_count = var.node_min
    max_node_count = var.node_max
  }

  node_config {
    machine_type = var.node_machine_type
    oauth_scopes = ["https://www.googleapis.com/auth/cloud-platform"]
  }
}

# Registry the k8s manifests pull the node image from.
resource "google_artifact_registry_repository" "mycelium" {
  location      = var.region
  repository_id = var.cluster_name
  format        = "DOCKER"
}
