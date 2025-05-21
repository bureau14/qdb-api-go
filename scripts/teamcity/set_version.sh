SCRIPT_DIR="$(cd "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null && pwd)"
sed -i -e 's/const GitHash string = .*/const GitHash string = "'${GIT_HASH}'"/' $SCRIPT_DIR/../../version.go