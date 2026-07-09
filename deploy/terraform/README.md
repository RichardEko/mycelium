# Provisioning a cluster for Mycelium — Terraform (EKS / GKE)

The [`deploy/kubernetes/`](../kubernetes/) manifests assume a Kubernetes cluster and a
registry already exist. This closes that gap: **reference Terraform that stands up the
cluster + node pool + a container registry**, so the whole path is three commands.

- [`aws/`](aws/) — **EKS** (via the maintained `terraform-aws-modules` VPC + EKS modules) + **ECR**.
- [`gcp/`](gcp/) — **GKE** (regional cluster + managed node pool) + **Artifact Registry**.

> **Reference scaffolding, not a product.** Mycelium is a library, not a platform — this is
> minimal IaC to copy and adapt (one node group, public API endpoint, no private networking or
> remote state backend), not an opinionated, hardened cluster. Read it before you run it.

## The full path (AWS shown; GCP is identical shape)

```sh
# 1. Provision the cluster + registry
cd deploy/terraform/aws
terraform init
terraform apply                       # creates real, billable resources — see the warning below
eval "$(terraform output -raw configure_kubectl)"   # point kubectl at the new cluster
REPO=$(terraform output -raw ecr_repository_url)

# 2. Build & push the node image to the registry Terraform just made
cd ../../..
docker build -t "$REPO:v2" -f docker/Dockerfile .
# (ECR needs a docker login first: aws ecr get-login-password | docker login --username AWS --password-stdin "$REPO")
docker push "$REPO:v2"

# 3. Point the manifests at that image and deploy
#    edit deploy/kubernetes/kustomization.yaml → images: [{ name: mycelium-demo, newName: <REPO>, newTag: v2 }]
kubectl apply -k deploy/kubernetes
kubectl -n mycelium scale statefulset mycelium-worker --replicas=50
kubectl -n mycelium get pods -o wide   # watch pods spread across the cluster's hosts
```

For **GCP**: `cd deploy/terraform/gcp`, set the required `project` (`terraform apply -var project=my-gcp-project`), then the same `configure_kubectl` / `artifact_registry_url` outputs drive steps 2–3. GKE `get-credentials` needs the `gke-gcloud-auth-plugin`.

## ⚠ Cost & lifecycle

These create **real, billable cloud resources**: an EKS/GKE control plane (hourly), a NAT
gateway (AWS), and worker VMs that bill while running. This is not free-tier. When you're done:

```sh
kubectl delete -k deploy/kubernetes   # remove the workload first
terraform destroy                     # then the cluster + registry
```

Both configs set `force_delete`/`deletion_protection = false` so `destroy` won't be blocked by
images in the registry or the cluster's delete guard.

## Scale knobs

Raise the node ceiling so the scheduler has hosts to spread worker pods across — this is the
whole point of going to a cloud cluster (escaping the single-host bridge ceiling in
[`scale-tests.md`](../../docs/wiki/dev/testing/scale-tests.md)):

- AWS: `terraform apply -var node_max=20 -var node_desired=12 -var node_instance_type=t3.xlarge`
- GCP: `terraform apply -var project=… -var node_max=8` (per-zone; ×~3 zones)

## Validation status

Authored against the AWS provider `~> 5.0` / `terraform-aws-modules` EKS `~> 20.0` and Google
provider `~> 5.0` schemas, but **not machine-validated in this repo's CI** (no `terraform`
binary in the authoring environment) and **not applied** (needs cloud credentials). Before you
apply, run the standard gate locally:

```sh
terraform fmt -check && terraform init && terraform validate
terraform plan        # review every resource before apply
```

If `validate`/`plan` flags a schema drift (provider/module versions move), the fix is a
version bump or attribute rename in these files — the topology (VPC → cluster → node pool →
registry) is the durable part.

## What it does NOT set up

Deliberately out of scope (add per your org's standards): a remote Terraform **state backend**
(local state by default — fine for a throwaway cluster, not for a team), **private cluster**
networking / restricted API endpoint, TLS/DNS/Ingress for external gateway access (the
Mycelium gateway is cluster-internal — see [`deploy/kubernetes/README.md`](../kubernetes/README.md)),
and IAM least-privilege beyond the modules' defaults. [`production-readiness.md`](../../docs/operations/production-readiness.md)
is the gate.
