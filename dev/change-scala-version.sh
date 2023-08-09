#!/usr/bin/env bash
set -e

VALID_VERSIONS=( 2.12 )

usage() {
  echo "Usage: $(basename $0) [-h|--help] <version>
where :
  -h| --help Display this help text
  valid version values : ${VALID_VERSIONS[*]}
" 1>&2
  exit 1
}

if [[ ($# -ne 1) || ( $1 == "--help") ||  $1 == "-h" ]]; then
  usage
fi

TO_VERSION=$1

check_scala_version() {
  for i in ${VALID_VERSIONS[*]}; do [ $i = "$1" ] && return 0; done
  echo "Invalid Scala version: $1. Valid versions: ${VALID_VERSIONS[*]}" 1>&2
  exit 1
}

check_scala_version "$TO_VERSION"

if [ $TO_VERSION = "2.11" ]; then
  FROM_VERSION="2.12"
else
  FROM_VERSION="2.11"
fi

sed_i() {
  sed -e "$1" "$2" > "$2.tmp" && mv "$2.tmp" "$2"
}

export -f sed_i

BASEDIR=$(dirname $0)/..
find "$BASEDIR" -name 'pom.xml' -not -path '*target*' -print \
  -exec bash -c "sed_i 's/\(artifactId.*\)_'$FROM_VERSION'/\1_'$TO_VERSION'/g' {}" \;

# Also update <scala.binary.version> in parent POM
# Match any scala binary version to ensure idempotency
sed_i '1,/<scala\.binary\.version>[0-9]*\.[0-9]*</s/<scala\.binary\.version>[0-9]*\.[0-9]*</<scala.binary.version>'$TO_VERSION'</' \
  "$BASEDIR/pom.xml"
