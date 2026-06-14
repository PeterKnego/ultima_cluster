TF      := terraform -chdir=terraform
TFVARS  ?= terraform.tfvars
ANSIBLE := ansible-playbook
SSH_KEY := $(shell awk -F'"' '/ssh_private_key_file/{print $$2}' $(TFVARS))

.PHONY: init up provision bench bench-oneshot status destroy ssh-node0 inventory

init:
	$(TF) init

up: ## provision infra + configure hosts
	$(TF) apply -auto-approve -var-file=../$(TFVARS)
	$(MAKE) inventory
	cd ansible && SSH_PRIVATE_KEY_FILE=$(SSH_KEY) $(ANSIBLE) provision.yml

inventory:
	SSH_PRIVATE_KEY_FILE=$(SSH_KEY) ./inventory/terraform_to_inventory.sh

bench: ## run the sweep + collect (hosts must be up)
	cd ansible && $(ANSIBLE) bench.yml

bench-oneshot: up bench destroy ## clean-room one-shot run

status: ## list instances + warn if past TTL
	@$(TF) output -json nodes | jq -r '.[] | "\(.name)\t\(.public_ip)\t\(.private_ip)\t\(.role)"'
	@echo "TTL guard: check instance uptime against ttl_hours in $(TFVARS)."
	@ttl=$$(awk '/ttl_hours/{print $$3}' ../$(TFVARS) 2>/dev/null || echo 4); \
	 cd ansible && ansible cluster -m shell -a "echo $$(hostname): up $$(awk '{print int($$1/3600)}' /proc/uptime)h" 2>/dev/null || true

destroy: ## tear everything down
	$(TF) destroy -auto-approve -var-file=../$(TFVARS)
	rm -f inventory/hosts.yml

ssh-node0:
	@ip=$$($(TF) output -json nodes | jq -r '.[]|select(.role=="node0").public_ip'); \
	 user=$$($(TF) output -raw ssh_user); ssh -i $(SSH_KEY) $$user@$$ip
