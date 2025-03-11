terraform {
  required_providers {
    openstack = {
      source = "terraform-provider-openstack/openstack"
      version = "~> 1.48"
    }
  }
  required_version = ">= 0.13"
}
