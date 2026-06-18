# Load cloud credentials (HCLOUD_TOKEN / AWS_* / GOOGLE_*) from a gitignored
# .env if present, and export them into every recipe's environment so
# terraform/ansible pick them up automatically. Leading '-' makes .env optional.
# .env lines are KEY=value (bare value recommended).
-include .env
# Safety net: make's -include keeps surrounding quotes LITERALLY (unlike shell
# dotenv loaders), so a value like HCLOUD_TOKEN="abc" would be passed to
# terraform as `"abc"` (with quotes) and fail auth. Strip surrounding double
# quotes from known cred vars so both `KEY=abc` and `KEY="abc"` work.
_ENV_CREDS := HCLOUD_TOKEN AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN GOOGLE_PROJECT
$(foreach v,$(_ENV_CREDS),$(eval $(v) := $$(patsubst "%",%,$$($(v)))))
export

TF      := terraform -chdir=terraform
TFVARS  ?= terraform.tfvars
ANSIBLE := ansible-playbook
SSH_KEY := $(shell awk -F'"' '/ssh_private_key_file/{print $$2}' $(TFVARS))

.PHONY: init up up-uc up-ping provision bench bench-oneshot status destroy ssh-node0 inventory iterate

init:
	$(TF) init

up: ## provision infra + configure hosts
	$(TF) apply -auto-approve -var-file=../$(TFVARS)
	$(MAKE) inventory
	cd ansible && SSH_PRIVATE_KEY_FILE=$(SSH_KEY) $(ANSIBLE) provision.yml

up-uc: ## provision a UC-only fleet (no Aeron) for the throughput loop
	$(TF) apply -auto-approve -var-file=../$(TFVARS)
	$(MAKE) inventory
	cd ansible && SSH_PRIVATE_KEY_FILE=$(SSH_KEY) $(ANSIBLE) provision.yml -e aeron_enabled=false

up-ping: ## provision a 2-host ccx13 fleet + persistent UC/Aeron echo responders (cross-host ping harness)
	TF_VAR_node_count=2 TF_VAR_instance_type=ccx13 $(TF) apply -auto-approve -var-file=../$(TFVARS)
	$(MAKE) inventory
	cd ansible && SSH_PRIVATE_KEY_FILE=$(SSH_KEY) $(ANSIBLE) netping.yml

iterate: ## one loop iteration on the live fleet: rebuild UC + UC sweep, print fitness JSON
	cd ansible && $(ANSIBLE) iterate.yml -e aeron_enabled=false
	@python3 scripts/uc_fitness.py ../bench-out/iterate/uc_sweep.csv

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
