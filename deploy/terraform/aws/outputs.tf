output "cluster_name" {
  description = "EKS cluster name."
  value       = module.eks.cluster_name
}

output "region" {
  description = "AWS region the cluster runs in."
  value       = var.region
}

output "configure_kubectl" {
  description = "Run this to point kubectl at the new cluster."
  value       = "aws eks update-kubeconfig --region ${var.region} --name ${module.eks.cluster_name}"
}

output "ecr_repository_url" {
  description = "Push the node image here, then set it as images[].newName in deploy/kubernetes/kustomization.yaml."
  value       = aws_ecr_repository.mycelium.repository_url
}
