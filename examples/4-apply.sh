#!/bin/bash

function apply() {
    kubectl create namespace team-viti2 --dry-run=client -o yaml | kubectl apply -f -
    kubectl -n team-viti2 apply -f 4-networkNamespace.yaml
    kubectl -n team-viti2 apply -f 4-kubernetescluster.yaml
}


function delete() {
    kubectl -n team-viti2 delete kubernetescluster --all \
        --ignore-not-found --wait=true --timeout=10m

    kubectl -n team-viti2 delete networknamespace --all \
        --ignore-not-found --wait=true --timeout=5m

    kubectl delete namespace team-viti2 \
        --ignore-not-found --wait=true --timeout=5m
}


case "$1" in
    apply)
        apply
        ;;
    delete)
        delete
        ;;
    *)
        echo "Usage: $0 {apply|delete}"
        exit 1
        ;;
esac