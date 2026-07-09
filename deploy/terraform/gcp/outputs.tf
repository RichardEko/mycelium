output "cluster_name" {
  description = "GKE cluster name."
  value       = google_container_cluster.mycelium.name
}

output "configure_kubectl" {
  description = "Run this to point kubectl at the new cluster (needs the gke-gcloud-auth-plugin)."
  value       = "gcloud container clusters get-credentials ${google_container_cluster.mycelium.name} --region ${var.region} --project ${var.project}"
}

output "artifact_registry_url" {
  description = "Push the node image here, then set it as images[].newName in deploy/kubernetes/kustomization.yaml."
  value       = "${var.region}-docker.pkg.dev/${var.project}/${google_artifact_registry_repository.mycelium.repository_id}"
}
