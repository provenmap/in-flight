#!/usr/bin/env sh
# A self-contained download: the jar with a trimmed Java runtime, so no JDK is needed to run it.
#   packaging/jpackage.sh [version]         -> dist/release/in-flight-<version>-<os>-<arch>.tar.gz (or .zip on Windows)
# Needs a JDK 25 (for jlink and jpackage) and the jar (npm run build && ./mvnw package).
set -eu
here=$(cd "$(dirname "$0")/.." && pwd)
version=${1:-$(sed -n 's:.*<version>\(.*\)</version>.*:\1:p' "$here/pom.xml" | head -1 | sed 's/-SNAPSHOT//')}
jar="$here/cli/target/in-flight.jar"
[ -f "$jar" ] || { echo "no jar at $jar: npm run build && ./mvnw package" >&2; exit 1; }
os=$(uname -s | tr '[:upper:]' '[:lower:]'); case "$os" in mingw*|msys*|cygwin*) os=windows ;; darwin) os=macos ;; esac
arch=$(uname -m); case "$arch" in x86_64|amd64) arch=x64 ;; arm64|aarch64) arch=arm64 ;; esac
out="$here/dist/release"
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
mkdir -p "$work/input" "$out"
cp "$jar" "$work/input/"
# The modules the jar uses, plus what TLS, SASL and the keychain lookups need at run time.
modules=$(jdeps --ignore-missing-deps --multi-release 25 --print-module-deps "$jar" 2>/dev/null || echo java.base)
modules="$modules,jdk.crypto.ec,jdk.crypto.cryptoki,java.security.jgss,java.security.sasl,jdk.security.auth,jdk.naming.dns,jdk.zipfs,jdk.httpserver"
jlink --add-modules "$modules" --strip-debug --no-header-files --no-man-pages --compress=zip-6 --output "$work/runtime"
# macOS refuses an app version starting with 0; the file name carries the real one.
appversion=$version
case "$os:$version" in macos:0.*) appversion="1.${version#0.}" ;; esac
jpackage --type app-image --name in-flight --app-version "$appversion" --input "$work/input" \
  --main-jar in-flight.jar --runtime-image "$work/runtime" --dest "$work/image" \
  --java-options "-Xss1m" --description "The money moving through your Kafka: a console and an MCP server" --vendor "in-flight"
name="in-flight-$version-$os-$arch"
cd "$work/image"
if [ "$os" = windows ]; then
  powershell -Command "Compress-Archive -Path in-flight -DestinationPath '$out/$name.zip'" 2>/dev/null || zip -qr "$out/$name.zip" in-flight
  echo "$out/$name.zip"
else
  tar -czf "$out/$name.tar.gz" in-flight* 
  echo "$out/$name.tar.gz"
fi
