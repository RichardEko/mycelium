provider "aws" {
  region = var.region
}

data "aws_availability_zones" "available" {
  state = "available"
}

locals {
  azs  = slice(data.aws_availability_zones.available.names, 0, 3)
  tags = { Project = "mycelium" }
}

# VPC across three AZs — worker nodes land in the private subnets, so Mycelium pods
# spread across three availability zones (real cross-host distribution).
module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "~> 5.0"

  name = "${var.cluster_name}-vpc"
  cidr = "10.0.0.0/16"

  azs             = local.azs
  private_subnets = ["10.0.1.0/24", "10.0.2.0/24", "10.0.3.0/24"]
  public_subnets  = ["10.0.101.0/24", "10.0.102.0/24", "10.0.103.0/24"]

  enable_nat_gateway   = true
  single_nat_gateway   = true # one NAT GW for the whole VPC — cheaper; fine for a demo/scale cluster
  enable_dns_hostnames = true

  # Subnet tags EKS uses to place load balancers.
  public_subnet_tags  = { "kubernetes.io/role/elb" = 1 }
  private_subnet_tags = { "kubernetes.io/role/internal-elb" = 1 }

  tags = local.tags
}

module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 20.0"

  cluster_name    = var.cluster_name
  cluster_version = var.kubernetes_version

  cluster_endpoint_public_access = true

  # Grant the identity running `terraform apply` cluster-admin, so `kubectl` works
  # immediately after `aws eks update-kubeconfig` (EKS module v20 feature).
  enable_cluster_creator_admin_permissions = true

  vpc_id                   = module.vpc.vpc_id
  subnet_ids               = module.vpc.private_subnets
  control_plane_subnet_ids = module.vpc.private_subnets

  eks_managed_node_groups = {
    default = {
      instance_types = [var.node_instance_type]
      min_size       = var.node_min
      max_size       = var.node_max
      desired_size   = var.node_desired
    }
  }

  tags = local.tags
}

# Registry the k8s manifests pull the node image from.
resource "aws_ecr_repository" "mycelium" {
  name                 = var.cluster_name
  image_tag_mutability = "MUTABLE"
  force_delete         = true # allow `terraform destroy` to remove it with images present

  image_scanning_configuration {
    scan_on_push = true
  }

  tags = local.tags
}
