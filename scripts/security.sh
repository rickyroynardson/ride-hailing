#!/bin/sh
# Scoped Compose operations only. No host key generation, global prune, or implicit data deletion.
set -eu
cd "$(dirname "$0")/.."
core="identity user ride location matching payment gateway devconsole"
project_name() {
  project=$(docker compose config | awk '/^name: / {print $2; exit}')
  if [ -z "$project" ]; then echo "Cannot determine Compose project" >&2; exit 1; fi
}
remove_unused_project_images() {
  # Includes old/dangling images from earlier builds, but never forces removal of an image used elsewhere.
  images=$(docker image ls -a --filter "label=com.docker.compose.project=$project" --format '{{.ID}}' | sort -u)
  for image in $images; do
    if docker ps -a --filter "ancestor=$image" -q | grep -q .; then
      echo "Keeping image $image: still used by a container"
    elif ! docker image rm "$image"; then
      echo "Keeping image $image: it may have another tag or dependent image" >&2
    fi
  done
}
case "${1:-help}" in
  up)
    # One image at a time: Compose/BuildKit can otherwise fan out all Go builds even with a parallel limit.
    for service in security-init migrate $core; do docker compose build "$service"; done
    docker compose up -d --wait --wait-timeout 90 $core
    ;;
  check)
    docker compose --profile security-check run --rm --build --no-deps security-check
    ;;
  deployment-check)
    sh scripts/deployment-check.sh
    ;;
  rotate)
    docker compose run --rm --no-deps security-init --rotate
    docker compose up -d --no-deps --force-recreate --wait --wait-timeout 90 $core
    docker compose --profile security-check run --rm --build --no-deps security-check
    ;;
  clean)
    # Remove only the security-using service containers and volumes for this Compose project. DB volumes stay.
    project_name
    docker compose --profile security-check --profile deployment-check stop security-init security-check migrate deployment-check deployment-a deployment-b deployment-store $core
    docker compose --profile security-check --profile deployment-check rm -f -v security-init security-check migrate deployment-check deployment-a deployment-b deployment-store $core
    volumes=$(docker volume ls --filter "label=com.docker.compose.project=$project" --filter "label=ride-hailing.security=true" --format '{{.Name}}')
    for volume in $volumes; do docker volume rm "$volume"; done
    if [ "${2:-}" = "--images" ]; then remove_unused_project_images; fi
    echo "Removed this project's security volumes; database volumes were kept. Images are kept unless --images is supplied."
    ;;
  reset)
    if [ "${2:-}" != "--yes-delete-data" ]; then
      echo "Full reset deletes ALL Compose data, including Postgres, and removes project-built images." >&2
      echo "Use: sh scripts/security.sh reset --yes-delete-data" >&2
      exit 1
    fi
    project_name
    docker compose --profile security-check --profile deployment-check --profile web down --volumes --remove-orphans --rmi local
    remove_unused_project_images
    ;;
  *)
    echo "Usage: sh scripts/security.sh {up|check|deployment-check|rotate|clean [--images]|reset --yes-delete-data}"
    ;;
esac
