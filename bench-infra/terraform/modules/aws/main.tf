# The AMI architecture is DERIVED from the instance type, not declared: a
# Graviton type (c8gd/c9gd/...) silently boots an arm64 image, an x86 type an
# amd64 one, and the two can never be mismatched by a forgotten variable.
data "aws_ec2_instance_type" "node" {
  instance_type = local.instance_type
}

data "aws_ec2_instance_type" "client" {
  instance_type = local.client_instance_type
}

locals {
  ami_arch        = contains(data.aws_ec2_instance_type.node.supported_architectures, "arm64") ? "arm64" : "amd64"
  client_ami_arch = contains(data.aws_ec2_instance_type.client.supported_architectures, "arm64") ? "arm64" : "amd64"
}

data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"] # Canonical
  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-${local.ami_arch}-server-*"]
  }
}

data "aws_ami" "ubuntu_client" {
  most_recent = true
  owners      = ["099720109477"] # Canonical
  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-${local.client_ami_arch}-server-*"]
  }
}

resource "aws_vpc" "bench" {
  cidr_block           = "10.10.0.0/16"
  enable_dns_hostnames = true
  tags                 = { Name = "${var.owner}-vpc", owner = var.owner }
}

# Pick an AZ that actually offers the requested instance type. AWS otherwise
# auto-places the subnet in an arbitrary AZ (e.g. us-east-1e), where larger
# types like c7i.4xlarge are not offered → RunInstances "Unsupported" 400.
data "aws_ec2_instance_type_offerings" "supported_az" {
  filter {
    name   = "instance-type"
    values = [local.instance_type]
  }
  location_type = "availability-zone"
}

resource "aws_subnet" "bench" {
  vpc_id                  = aws_vpc.bench.id
  cidr_block              = "10.10.1.0/24"
  map_public_ip_on_launch = true
  # Cluster placement group pins all nodes to this one AZ; pick a supported one.
  availability_zone = sort(data.aws_ec2_instance_type_offerings.supported_az.locations)[0]
}

resource "aws_internet_gateway" "bench" {
  vpc_id = aws_vpc.bench.id
}

resource "aws_route_table" "bench" {
  vpc_id = aws_vpc.bench.id
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.bench.id
  }
}

resource "aws_route_table_association" "bench" {
  subnet_id      = aws_subnet.bench.id
  route_table_id = aws_route_table.bench.id
}

resource "aws_security_group" "bench" {
  name   = "${var.owner}-sg"
  vpc_id = aws_vpc.bench.id
  ingress {
    description = "ssh"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.allow_ssh_cidr]
  }
  ingress {
    description = "intra-cluster"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    self        = true
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_key_pair" "bench" {
  key_name   = "${var.owner}-key"
  public_key = var.ssh_public_key
}

resource "aws_placement_group" "bench" {
  name     = "${var.owner}-pg"
  strategy = "cluster"
}

resource "aws_instance" "node" {
  count                  = var.node_count
  ami                    = count.index < var.voter_count ? data.aws_ami.ubuntu.id : data.aws_ami.ubuntu_client.id
  instance_type          = count.index < var.voter_count ? local.instance_type : local.client_instance_type
  subnet_id              = aws_subnet.bench.id
  vpc_security_group_ids = [aws_security_group.bench.id]
  key_name               = aws_key_pair.bench.key_name
  placement_group        = aws_placement_group.bench.id
  private_ip             = "10.10.1.${count.index + 10}"

  # EBS-only types (c6i, ...) have no instance-store NVMe, so /opt/bench —
  # toolchain, synced tree, build artifacts — lives on the ROOT volume; the
  # AMI default (~8 GB) fills mid-provision and wedges rsync. Resizes in
  # place on existing instances (grow the fs with growpart+resize2fs).
  root_block_device {
    volume_size = 64
    volume_type = "gp3"
  }

  # Client hosts can ride Spot: they are stateless TCP load drivers, an
  # interruption just fails the rung visibly, and Spot draws from a separate
  # capacity/quota pool than On-Demand.
  dynamic "instance_market_options" {
    for_each = var.client_spot && count.index >= var.voter_count ? [1] : []
    content {
      market_type = "spot"
      spot_options {
        spot_instance_type             = "one-time"
        instance_interruption_behavior = "terminate"
      }
    }
  }

  tags = {
    Name      = "${var.owner}-node${count.index}"
    owner     = var.owner
    ttl_hours = tostring(var.ttl_hours)
    role      = "node${count.index}"
  }

  # A one-time Spot instance cannot be stopped, so its market options can
  # never be changed in place — converting flags would otherwise force a
  # stop/modify that AWS refuses. Existing instances keep the market they
  # were born with; the client_spot flag only shapes NEW instances.
  lifecycle {
    ignore_changes = [instance_market_options]
  }
}
