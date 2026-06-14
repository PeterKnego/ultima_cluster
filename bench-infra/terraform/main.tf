locals {
  enable_hetzner = var.cloud == "hetzner"
  enable_aws     = var.cloud == "aws"
  enable_gcp     = var.cloud == "gcp"
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

# AWS and GCP modules are added in Tasks 13 and 14 with identical call shape.

locals {
  active_module = (
    local.enable_hetzner ? module.hetzner[0] :
    null
  )
}
