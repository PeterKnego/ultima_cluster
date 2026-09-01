variable "node_count" { type = number }
variable "instance_type" { type = string }
variable "client_instance_type" {
  type    = string
  default = ""
}
variable "voter_count" {
  type    = number
  default = 3
}
variable "client_spot" {
  type    = bool
  default = false
}
variable "region" { type = string }
variable "ssh_public_key" { type = string }
variable "allow_ssh_cidr" { type = string }
variable "ttl_hours" { type = number }
variable "owner" { type = string }

locals {
  instance_type        = var.instance_type != "" ? var.instance_type : "c7i.4xlarge"
  client_instance_type = var.client_instance_type != "" ? var.client_instance_type : (var.instance_type != "" ? var.instance_type : "c7i.4xlarge")
  region               = var.region != "" ? var.region : "us-east-1"
}
