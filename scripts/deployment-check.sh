#!/bin/sh
# Two disposable replicas and a tmpfs database; never restart the real payment/ride service or mount Docker's socket.
set -eu
cd "$(dirname "$0")/.."
compose() { docker compose --profile deployment-check "$@"; }
cleanup() {
  compose stop deployment-a deployment-b deployment-store >/dev/null 2>&1 || true
  compose rm -f -v deployment-a deployment-b deployment-store >/dev/null 2>&1 || true
}
trap cleanup 0
trap 'exit 130' INT
trap 'exit 143' TERM
compose build deployment-a
compose up -d --wait --wait-timeout 90 identity deployment-store deployment-a deployment-b
request="deployment-$(date +%s)-$$"
compose run --rm --no-deps deployment-check exercise "$request"
# Stop also provides a final Docker-side deadline if a fixture process fails to exit after its application budget.
compose stop --timeout 6 deployment-a
container=$(compose ps -a -q deployment-a)
code=$(docker inspect "$container" --format '{{.State.ExitCode}}')
if [ "$code" != 0 ]; then echo "Fixture required abnormal termination: exit $code" >&2; exit 1; fi
compose up -d --no-deps --wait --wait-timeout 30 deployment-a
compose run --rm --no-deps deployment-check verify "$request"
echo "Deployment drill passed; disposable fixture containers and tmpfs data will now be removed."
