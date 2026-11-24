#!/bin/bash
# Patch all cinder-volume-vmware and cinder-volume-backup-vmware deployments
# to use dumb-init --single-child.
# Run this after every cc-autodeploy deploy, until the Helm chart change
# (vcenter_datacenter_cinder_deployment.yaml) is deployed via the normal pipeline.
#
# Usage: ./patch-dumb-init-single-child.sh [context]
#   context: kubectl context (default: qa-de-1)

CONTEXT="${1:-qa-de-1}"
NAMESPACE="monsoon3"

echo "Patching cinder-volume-vmware deployments in ${CONTEXT}/${NAMESPACE}..."

DEPLOYMENTS=$(kubectl --context "$CONTEXT" -n "$NAMESPACE" get deployments --no-headers \
  -o custom-columns=NAME:.metadata.name | grep cinder-volume-vmware)

for dep in $DEPLOYMENTS; do
  # Skip scaled-to-zero deployments
  replicas=$(kubectl --context "$CONTEXT" -n "$NAMESPACE" get deployment "$dep" \
    -o jsonpath='{.spec.replicas}')
  if [ "$replicas" = "0" ]; then
    echo "  $dep: skipped (0 replicas)"
    continue
  fi

  # Check current command
  cmd=$(kubectl --context "$CONTEXT" -n "$NAMESPACE" get deployment "$dep" \
    -o jsonpath='{.spec.template.spec.containers[0].command}')

  if echo "$cmd" | grep -q "single-child"; then
    echo "  $dep: already has --single-child"
  else
    kubectl --context "$CONTEXT" -n "$NAMESPACE" patch deployment "$dep" \
      --type='json' \
      -p='[{"op": "replace", "path": "/spec/template/spec/containers/0/command", "value": ["dumb-init", "--single-child", "cinder-volume"]}]'
    echo "  $dep: patched"
  fi
done

echo ""
echo "Patching cinder-volume-backup-vmware deployments in ${CONTEXT}/${NAMESPACE}..."

BACKUP_DEPLOYMENTS=$(kubectl --context "$CONTEXT" -n "$NAMESPACE" get deployments --no-headers \
  -o custom-columns=NAME:.metadata.name | grep cinder-volume-backup-vmware)

for dep in $BACKUP_DEPLOYMENTS; do
  # Skip scaled-to-zero deployments
  replicas=$(kubectl --context "$CONTEXT" -n "$NAMESPACE" get deployment "$dep" \
    -o jsonpath='{.spec.replicas}')
  if [ "$replicas" = "0" ]; then
    echo "  $dep: skipped (0 replicas)"
    continue
  fi

  # Check current command
  cmd=$(kubectl --context "$CONTEXT" -n "$NAMESPACE" get deployment "$dep" \
    -o jsonpath='{.spec.template.spec.containers[0].command}')

  if echo "$cmd" | grep -q "single-child"; then
    echo "  $dep: already has --single-child"
  else
    kubectl --context "$CONTEXT" -n "$NAMESPACE" patch deployment "$dep" \
      --type='json' \
      -p='[{"op": "replace", "path": "/spec/template/spec/containers/0/command", "value": ["dumb-init", "--single-child", "cinder-backup"]}]'
    echo "  $dep: patched"
  fi
done

echo "Done."
