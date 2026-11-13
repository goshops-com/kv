#!/usr/bin/env bash
# Deploy a prod con canary y rollback automático. Lo usa el job `deploy` de .github/workflows/test.yml.
#
# Variables:
#   IMAGE        imagen a deployar (inmutable: goshops/go-search-api:develop-<sha>)
#   WEB          deployment:contenedor que sirve tráfico (canary + observación), ej. go-discover:go-discover
#   OTHERS       deployments:contenedor que siguen al web, separados por espacio (workers, réplicas en 0)
#   DRY_RUN      "true" = solo lee y muestra qué haría
#   CANARY_SECS  cuánto se observa el canary (default 180)
#   KUBECONFIG   credencial del ServiceAccount github-deployer (permisos solo sobre estos deployments)
#
# Canary: `set image` + `rollout pause` inmediato. Con maxUnavailable 0 el controller primero levanta
# la tanda de maxSurge con la imagen nueva y no baja ningún pod viejo hasta que esos estén listos, así
# que el rollout queda pausado con ~10 % de pods nuevos atendiendo tráfico. Se observa CANARY_SECS:
# pods listos, sin reinicios, y errores en logs no peores que los pods viejos. Si algo falla → undo.
set -euo pipefail

: "${IMAGE:?falta IMAGE}" "${WEB:?falta WEB}"
OTHERS="${OTHERS:-}"
DRY_RUN="${DRY_RUN:-false}"
CANARY_SECS="${CANARY_SECS:-180}"
K="kubectl --request-timeout=30s"
SUMMARY="${GITHUB_STEP_SUMMARY:-/dev/stdout}"
WEB_DEPLOY="${WEB%%:*}"; WEB_CONTAINER="${WEB##*:}"
CHANGED=()   # deployments ya modificados (para el rollback)

log() { echo "[deploy] $*"; }
summary() { echo "$*" >> "$SUMMARY"; }

current_image() { $K get deploy "$1" -o jsonpath="{.spec.template.spec.containers[?(@.name==\"$2\")].image}"; }

# Líneas de error por pod en los últimos N segundos (mismo criterio para pods viejos y nuevos).
errors_of_pod() {
  $K logs "$1" -c "$WEB_CONTAINER" --since="${2}s" 2>/dev/null \
    | grep -ciE '(^|[^a-z])(error|exception|unhandled|fatal)([^a-z]|$)' || true
}

pods_with_image() { # deploy image -> nombres de pods de ese deployment con esa imagen
  local sel; sel=$($K get deploy "$1" -o jsonpath='{.spec.selector.matchLabels}' | python3 -c 'import sys,json;print(",".join(f"{k}={v}" for k,v in json.load(sys.stdin).items()))')
  $K get pods -l "$sel" -o json | python3 -c '
import sys,json
img=sys.argv[1]
for p in json.load(sys.stdin)["items"]:
    if p["metadata"].get("deletionTimestamp"): continue
    if any(c["image"]==img for c in p["spec"]["containers"]): print(p["metadata"]["name"])' "$2"
}

pod_health() { # pod -> "ready restarts"
  $K get pod "$1" -o json | python3 -c '
import sys,json
p=json.load(sys.stdin); cs=p["status"].get("containerStatuses",[])
print("true" if cs and all(c.get("ready") for c in cs) else "false", sum(c.get("restartCount",0) for c in cs))'
}

rollback() {
  local why="$1"
  log "ROLLBACK: $why"
  summary "### ❌ Rollback: $why"
  for d in ${CHANGED[@]+"${CHANGED[@]}"}; do
    $K rollout resume "deploy/$d" >/dev/null 2>&1 || true
    $K rollout undo "deploy/$d" && summary "- \`$d\`: vuelto a la revisión anterior" || summary "- \`$d\`: **el undo falló, revisar a mano**"
  done
  for d in ${CHANGED[@]+"${CHANGED[@]}"}; do $K rollout status "deploy/$d" --timeout=600s || true; done
  exit 1
}

# ---------------------------------------------------------------------------------------------------
PREV_WEB=$(current_image "$WEB_DEPLOY" "$WEB_CONTAINER")
summary "### Deploy a prod"
summary "- imagen nueva: \`$IMAGE\`"
summary "- imagen actual de \`$WEB_DEPLOY\`: \`$PREV_WEB\`"
if [ "$PREV_WEB" = "$IMAGE" ]; then summary "- ya está deployada, nada que hacer"; exit 0; fi

# Errores de referencia: pods actuales del web, últimos CANARY_SECS.
OLD_PODS=(); while IFS= read -r p; do [ -n "$p" ] && OLD_PODS+=("$p"); done < <(pods_with_image "$WEB_DEPLOY" "$PREV_WEB" | head -5)
BASE_TOTAL=0
for p in ${OLD_PODS[@]+"${OLD_PODS[@]}"}; do BASE_TOTAL=$(( BASE_TOTAL + $(errors_of_pod "$p" "$CANARY_SECS") )); done
BASE_PER_POD=$(( ${#OLD_PODS[@]} > 0 ? BASE_TOTAL / ${#OLD_PODS[@]} : 0 ))
# Tolerancia: el doble de la referencia + 10 líneas por pod.
MAX_PER_POD=$(( BASE_PER_POD * 2 + 10 ))
summary "- errores de referencia: ${BASE_PER_POD} líneas/pod en ${CANARY_SECS}s (${#OLD_PODS[@]} pods); máximo tolerado en el canary: ${MAX_PER_POD}"

if [ "$DRY_RUN" = "true" ]; then
  summary "#### Dry-run (no se modificó nada)"
  for chk in "patch deployment/$WEB_DEPLOY"; do summary "- permiso \`$chk\`: $($K auth can-i $chk)"; done
  for o in $OTHERS; do summary "- permiso \`patch deployment/${o%%:*}\`: $($K auth can-i patch "deployment/${o%%:*}")"; summary "- \`${o%%:*}\` hoy: \`$(current_image "${o%%:*}" "${o##*:}")\`"; done
  summary "- haría: \`kubectl set image deploy/$WEB_DEPLOY $WEB_CONTAINER=$IMAGE && kubectl rollout pause deploy/$WEB_DEPLOY\`"
  summary "- observaría el canary ${CANARY_SECS}s, después \`rollout resume\` + \`rollout status\`"
  for o in $OTHERS; do summary "- después: \`kubectl set image deploy/${o%%:*} ${o##*:}=$IMAGE\` + \`rollout status\`"; done
  exit 0
fi

# 1) Canary --------------------------------------------------------------------------------------------
# A run cancelled mid-canary leaves the deployment paused, and `rollout pause` then fails with
# "already paused" (2026-11-10). Resume it first: its pods keep serving whatever revision they run.
if [ "$($K get "deploy/$WEB_DEPLOY" -o jsonpath='{.spec.paused}')" = "true" ]; then
  log "$WEB_DEPLOY estaba pausado (deploy anterior cortado): lo reanudo"
  $K rollout resume "deploy/$WEB_DEPLOY"
fi
log "canary: $WEB_DEPLOY → $IMAGE"
$K set image "deploy/$WEB_DEPLOY" "$WEB_CONTAINER=$IMAGE"
$K rollout pause "deploy/$WEB_DEPLOY"
CHANGED+=("$WEB_DEPLOY")

all_ready() { # true si hay al menos un pod y todos están listos
  [ $# -gt 0 ] || return 1
  local p
  for p in "$@"; do [ "$(pod_health "$p" | cut -d' ' -f1)" = true ] || return 1; done
}
deadline=$(( SECONDS + 600 ))
CANARY_PODS=()
while ! all_ready ${CANARY_PODS[@]+"${CANARY_PODS[@]}"}; do
  if [ $SECONDS -gt $deadline ]; then rollback "los pods canary no quedaron listos en 10 min"; fi
  sleep 10
  CANARY_PODS=(); while IFS= read -r p; do [ -n "$p" ] && CANARY_PODS+=("$p"); done < <(pods_with_image "$WEB_DEPLOY" "$IMAGE")
done
summary "- canary listo: ${#CANARY_PODS[@]} pods (${CANARY_PODS[*]})"

log "observando el canary ${CANARY_SECS}s"
sleep "$CANARY_SECS"
for p in "${CANARY_PODS[@]}"; do
  read -r ready restarts <<<"$(pod_health "$p")"
  errs=$(errors_of_pod "$p" "$CANARY_SECS")
  summary "- \`$p\`: listo=$ready reinicios=$restarts errores=$errs"
  [ "$ready" = true ] || rollback "$p dejó de estar listo"
  [ "$restarts" -eq 0 ] || rollback "$p se reinició $restarts veces"
  [ "$errs" -le "$MAX_PER_POD" ] || rollback "$p tuvo $errs líneas de error (máximo $MAX_PER_POD)"
done

# 2) Rollout completo del web --------------------------------------------------------------------------
log "canary OK → rollout completo de $WEB_DEPLOY"
$K rollout resume "deploy/$WEB_DEPLOY"
$K rollout status "deploy/$WEB_DEPLOY" --timeout=1200s || rollback "el rollout de $WEB_DEPLOY no terminó"
summary "- \`$WEB_DEPLOY\`: rollout completo"

# 3) El resto (workers, réplicas en 0) -----------------------------------------------------------------
for o in $OTHERS; do
  d="${o%%:*}"; c="${o##*:}"
  $K set image "deploy/$d" "$c=$IMAGE"
  CHANGED+=("$d")
  $K rollout status "deploy/$d" --timeout=900s || rollback "el rollout de $d no terminó"
  summary "- \`$d\`: rollout completo"
done

# 4) Workers: sin readiness probe, se mira que no se reinicien en el primer minuto.
sleep 60
for o in $OTHERS; do
  d="${o%%:*}"
  for p in $(pods_with_image "$d" "$IMAGE"); do
    read -r _ restarts <<<"$(pod_health "$p")"
    [ "$restarts" -eq 0 ] || rollback "$p ($d) se reinició $restarts veces"
  done
done
summary "### ✅ Deployado: \`$IMAGE\`"
