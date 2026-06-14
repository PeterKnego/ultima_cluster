locals {
  enable_hetzner = var.cloud == "hetzner"
  enable_aws     = var.cloud == "aws"
  enable_gcp     = var.cloud == "gcp"

  # Effective regions used by root-level provider blocks (mirror module defaults).
  aws_region = var.region != "" ? var.region : "us-east-1"
  gcp_region = var.region != "" ? var.region : "us-central1"
}

provider "aws" {
  region = local.aws_region
}

provider "google" {
  region = local.gcp_region
  zone   = "${local.gcp_region}-a"
}

module "hetzner" {
  source = "./modules/hetzner"
  count  = local.enable_hetzner ? 1 : 0

  node_count     = var.node_count
  instance_type  = var.instance_type
  region         = var.region
  ssh_public_key = var.ssh_public_key
  allow_ssh_cidr = var.allow_ssh_cidr
  ttl_hours      = var.ttl_hours
  owner          = var.owner
}

module "aws" {
  source = "./modules/aws"
  count  = local.enable_aws ? 1 : 0

  node_count     = var.node_count
  instance_type  = var.instance_type
  region         = var.region
  ssh_public_key = var.ssh_public_key
  allow_ssh_cidr = var.allow_ssh_cidr
  ttl_hours      = var.ttl_hours
  owner          = var.owner
}

module "gcp" {
  source = "./modules/gcp"
  count  = local.enable_gcp ? 1 : 0

  node_count     = var.node_count
  instance_type  = var.instance_type
  region         = var.region
  ssh_public_key = var.ssh_public_key
  allow_ssh_cidr = var.allow_ssh_cidr
  ttl_hours      = var.ttl_hours
  owner          = var.owner
}

locals {
  active_module = (
    local.enable_hetzner ? module.hetzner[0] :
    local.enable_aws ? module.aws[0] :
    local.enable_gcp ? module.gcp[0] :
    null
  )
}
