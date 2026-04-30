#!/usr/bin/env bash
#
# bulk-upgrade.sh
#
# Bulk-trigger Talos upgrades on KubernetesCluster resources in a management
# (supervisor) cluster.
#
# For each KubernetesCluster matching the requested environment filter, the
# script verifies that:
#   - upgrade.vitistack.io/talos-current is older than the target version
#   - upgrade.vitistack.io/talos-target is not already set
#
# When all checks pass, it sets upgrade.vitistack.io/talos-target on the
# KubernetesCluster (always with a leading "v", e.g. "v1.12.7"), which the
# talos-operator picks up to perform the upgrade.
#
# Requirements: kubectl, jq

set -euo pipefail

# ---- defaults ----------------------------------------------------------------

DEFAULT_KUBECONFIG="${HOME}/kubeconfig/viti-super-test.config"
DEFAULT_TARGET_VERSION="1.12.7"

KUBECONFIG_PATH="${DEFAULT_KUBECONFIG}"
TARGET_VERSION="${DEFAULT_TARGET_VERSION}"
ENV_FILTER=""
LIMIT=0
DRY_RUN=false
ASSUME_YES=false

CURRENT_ANNOTATION="upgrade.vitistack.io/talos-current"
TARGET_ANNOTATION="upgrade.vitistack.io/talos-target"

# ---- usage -------------------------------------------------------------------

usage() {
    cat <<EOF
Usage: $(basename "$0") --env <dev|test|qa|prod> [options]

Required:
  --env <env>             Environment to filter on. Accepts common aliases:
                            dev   = dev | develop | development
                            test  = test | testing
                            qa    = qa  | staging | stage
                            prod  = prod | production | live

Options:
  --target-version <ver>  Target Talos version to upgrade to. The value of the
                          'talos-target' annotation written to each cluster
                          (always with a leading 'v').
                          Default: ${DEFAULT_TARGET_VERSION}
  --kubeconfig <path>     Path to the supervisor kubeconfig.
                          Default: ${DEFAULT_KUBECONFIG}
  --limit <n>             Only trigger upgrades on at most N clusters this run
                          (0 = all matching). Default: 0
  --dry-run               Print what would change but do not modify anything.
  -y, --yes               Skip the confirmation prompt.
  -h, --help              Show this help.

Examples:
  $(basename "$0") --env dev --dry-run
  $(basename "$0") --env test --target-version 1.12.7 --limit 5
  $(basename "$0") --env prod --yes
EOF
}

# ---- arg parsing -------------------------------------------------------------

while [[ $# -gt 0 ]]; do
    case "$1" in
        --env)              ENV_FILTER="${2:-}"; shift 2 ;;
        --target-version)   TARGET_VERSION="${2:-}"; shift 2 ;;
        --kubeconfig)       KUBECONFIG_PATH="${2:-}"; shift 2 ;;
        --limit)            LIMIT="${2:-0}"; shift 2 ;;
        --dry-run)          DRY_RUN=true; shift ;;
        -y|--yes)           ASSUME_YES=true; shift ;;
        -h|--help)          usage; exit 0 ;;
        *)
            echo "Unknown argument: $1" >&2
            usage >&2
            exit 2
            ;;
    esac
done

if [[ -z "${ENV_FILTER}" ]]; then
    echo "Error: --env is required" >&2
    usage >&2
    exit 2
fi

if ! [[ "${LIMIT}" =~ ^[0-9]+$ ]]; then
    echo "Error: --limit must be a non-negative integer" >&2
    exit 2
fi

# ---- prerequisites -----------------------------------------------------------

for bin in kubectl jq; do
    if ! command -v "$bin" >/dev/null 2>&1; then
        echo "Error: required binary '$bin' not found in PATH" >&2
        exit 1
    fi
done

if [[ ! -f "${KUBECONFIG_PATH}" ]]; then
    echo "Error: kubeconfig not found: ${KUBECONFIG_PATH}" >&2
    exit 1
fi

KCTL=(kubectl --kubeconfig "${KUBECONFIG_PATH}")

# ---- helpers -----------------------------------------------------------------

# Normalize an environment value to one of: dev, test, qa, prod, or "" if no
# match. The match is case-insensitive.
normalize_env() {
    local raw
    raw="$(printf '%s' "${1:-}" | tr '[:upper:]' '[:lower:]')"
    case "${raw}" in
        dev|develop|development)        echo "dev" ;;
        test|testing)                   echo "test" ;;
        qa|staging|stage)               echo "qa" ;;
        prod|production|live)           echo "prod" ;;
        *)                              echo "" ;;
    esac
}

NORMALIZED_FILTER="$(normalize_env "${ENV_FILTER}")"
if [[ -z "${NORMALIZED_FILTER}" ]]; then
    echo "Error: --env '${ENV_FILTER}' did not match a known environment" >&2
    echo "       valid: dev|develop|development | test|testing | qa|staging|stage | prod|production|live" >&2
    exit 2
fi

# Compare two semver-ish versions (strips leading "v"). Returns:
#   0 if a == b
#   1 if a >  b
#   2 if a <  b
# Pure-bash; does not depend on `sort -V` (which is missing on older macOS).
version_cmp() {
    local a="${1#v}"
    local b="${2#v}"
    if [[ "$a" == "$b" ]]; then
        echo 0; return
    fi
    local IFS=.
    # shellcheck disable=SC2206
    local -a aa=($a)
    # shellcheck disable=SC2206
    local -a bb=($b)
    local len=${#aa[@]}
    if [[ ${#bb[@]} -gt $len ]]; then len=${#bb[@]}; fi
    local i av bv
    for (( i = 0; i < len; i++ )); do
        av="${aa[i]:-0}"
        bv="${bb[i]:-0}"
        # Strip any non-digit suffix (e.g. "7-rc1" -> "7") so arithmetic compare works.
        av="${av%%[!0-9]*}"
        bv="${bv%%[!0-9]*}"
        av="${av:-0}"
        bv="${bv:-0}"
        if (( 10#$av > 10#$bv )); then echo 1; return; fi
        if (( 10#$av < 10#$bv )); then echo 2; return; fi
    done
    echo 0
}

# ---- main --------------------------------------------------------------------

echo "Supervisor kubeconfig : ${KUBECONFIG_PATH}"
echo "Environment filter    : ${ENV_FILTER}  (normalized: ${NORMALIZED_FILTER})"
echo "Target Talos version  : ${TARGET_VERSION}"
echo "Dry run               : ${DRY_RUN}"
[[ "${LIMIT}" -gt 0 ]] && echo "Per-run limit         : ${LIMIT}"
echo

echo "Fetching KubernetesClusters from supervisor cluster..."
CLUSTERS_JSON="$("${KCTL[@]}" get kubernetesclusters.vitistack.io -A -o json)"

# Build a TSV stream of: namespace<TAB>name<TAB>env<TAB>current<TAB>target
ALL_ROWS="$(jq -r '
    .items[]
    | [
        .metadata.namespace,
        .metadata.name,
        (.spec.data.environment // ""),
        (.metadata.annotations["'"${CURRENT_ANNOTATION}"'"] // ""),
        (.metadata.annotations["'"${TARGET_ANNOTATION}"'"]  // "")
      ]
    | @tsv
' <<<"${CLUSTERS_JSON}")"

if [[ -z "${ALL_ROWS}" ]]; then
    echo "No KubernetesClusters found."
    exit 0
fi

ELIGIBLE=()
SKIPPED_REASONS=()

TARGET_VERSION_BARE="${TARGET_VERSION#v}"
TARGET_VERSION_V="v${TARGET_VERSION_BARE}"

while IFS=$'\t' read -r ns name env_raw current target; do
    [[ -z "${name}" ]] && continue

    env_norm="$(normalize_env "${env_raw}")"
    if [[ "${env_norm}" != "${NORMALIZED_FILTER}" ]]; then
        continue  # not in scope; do not even report
    fi

    if [[ -z "${current}" ]]; then
        SKIPPED_REASONS+=("${ns}/${name}: talos-current annotation missing")
        continue
    fi

    cmp="$(version_cmp "${current}" "${TARGET_VERSION_BARE}")"
    if [[ "${cmp}" != "2" ]]; then
        SKIPPED_REASONS+=("${ns}/${name}: talos-current='${current}' is not older than '${TARGET_VERSION_BARE}'")
        continue
    fi

    if [[ -n "${target}" ]]; then
        SKIPPED_REASONS+=("${ns}/${name}: talos-target already set to '${target}' (upgrade pending)")
        continue
    fi

    ELIGIBLE+=("${ns}/${name}|${current}")
done <<<"${ALL_ROWS}"

if [[ ${#SKIPPED_REASONS[@]} -gt 0 ]]; then
    echo "Skipped (in-scope but not eligible):"
    for r in "${SKIPPED_REASONS[@]}"; do
        echo "  - ${r}"
    done
    echo
fi

if [[ ${#ELIGIBLE[@]} -eq 0 ]]; then
    echo "No eligible clusters to upgrade in environment '${NORMALIZED_FILTER}'."
    exit 0
fi

if [[ "${LIMIT}" -gt 0 && ${#ELIGIBLE[@]} -gt ${LIMIT} ]]; then
    ELIGIBLE=("${ELIGIBLE[@]:0:${LIMIT}}")
fi

echo "Eligible clusters (${#ELIGIBLE[@]}):"
printf "  %-50s  %-12s  %s\n" "NAMESPACE/NAME" "CURRENT" "-> TARGET"
for entry in "${ELIGIBLE[@]}"; do
    IFS='|' read -r id current <<<"${entry}"
    printf "  %-50s  %-12s  -> %s\n" "${id}" "${current}" "${TARGET_VERSION_V}"
done
echo

if [[ "${DRY_RUN}" == "true" ]]; then
    echo "Dry run: no annotations will be applied."
    exit 0
fi

if [[ "${ASSUME_YES}" != "true" ]]; then
    read -r -p "Proceed and set ${TARGET_ANNOTATION}=${TARGET_VERSION_V} on the ${#ELIGIBLE[@]} cluster(s) above? [y/N] " ans
    case "${ans}" in
        y|Y|yes|YES) ;;
        *) echo "Aborted."; exit 0 ;;
    esac
fi

failed=0
for entry in "${ELIGIBLE[@]}"; do
    IFS='|' read -r id _ _ <<<"${entry}"
    ns="${id%%/*}"
    name="${id##*/}"
    echo "Annotating ${ns}/${name}..."
    if ! "${KCTL[@]}" -n "${ns}" annotate kubernetescluster.vitistack.io "${name}" \
            "${TARGET_ANNOTATION}=${TARGET_VERSION_V}" --overwrite; then
        echo "  ERROR: failed to annotate ${ns}/${name}" >&2
        failed=$((failed + 1))
    fi
done

echo
if [[ "${failed}" -gt 0 ]]; then
    echo "Done with ${failed} failure(s)."
    exit 1
fi
echo "Done. Triggered Talos upgrade to ${TARGET_VERSION_V} on ${#ELIGIBLE[@]} cluster(s)."
