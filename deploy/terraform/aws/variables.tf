variable "region" {
  description = "AWS region to deploy into."
  type        = string
  default     = "eu-west-1"
}

variable "cluster_name" {
  description = "EKS cluster name (also the ECR repo and VPC name prefix)."
  type        = string
  default     = "mycelium"
}

variable "kubernetes_version" {
  description = "EKS control-plane Kubernetes version."
  type        = string
  default     = "1.30"
}

variable "node_instance_type" {
  description = "EC2 instance type for the worker nodes Mycelium pods run on."
  type        = string
  default     = "t3.large"
}

variable "node_min" {
  description = "Minimum nodes in the managed node group."
  type        = number
  default     = 2
}

variable "node_max" {
  description = "Maximum nodes — raise this for a large Mycelium scale run (worker pods spread across these hosts, escaping the single-host bridge ceiling)."
  type        = number
  default     = 6
}

variable "node_desired" {
  description = "Desired node count at apply time."
  type        = number
  default     = 3
}
