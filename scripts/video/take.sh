#!/usr/bin/env sh
# The usage video, made from scratch (docs/VIDEO.md): a fresh Kafka and registry of its own, the sample shop on it, the
# app on an empty workspace, one continuous recording driven by scripts/video/record.mjs, then an H.264 MP4.
#   npm run docs:video                  writes docs/usage.mp4
#   scripts/video/take.sh out.mp4       writes elsewhere
# Needs Docker, a JDK 25 or newer, ffmpeg, and the built jar (npm run build && ./mvnw package). Uses ports 9592, 8580,
# 8099 and 8088, and refuses to start while any of them is taken. Everything it starts is stopped and removed at the end.
set -eu
repo=$(cd "$(dirname "$0")/../.." && pwd)
out=${1:-$repo/docs/usage.mp4}
jar=$repo/cli/target/in-flight.jar
[ -f "$jar" ] || { echo "no jar at $jar: npm run build && ./mvnw package" >&2; exit 1; }
command -v ffmpeg >/dev/null || { echo "ffmpeg is needed" >&2; exit 1; }

java=""
for candidate in "${JAVA_HOME:-}" "$(/usr/libexec/java_home -v 25+ 2>/dev/null || true)" \
  /opt/homebrew/opt/openjdk/libexec/openjdk.jdk/Contents/Home /opt/homebrew/opt/openjdk@*/libexec/openjdk.jdk/Contents/Home /usr/lib/jvm/*; do
  if [ -n "$candidate" ] && [ -x "$candidate/bin/java" ]; then
    v=$("$candidate/bin/java" -version 2>&1 | sed -n 's/.*version "\([0-9][0-9]*\).*/\1/p' | head -1)
    if [ -n "$v" ] && [ "$v" -ge 25 ]; then java=$candidate/bin/java; break; fi
  fi
done
[ -n "$java" ] || { echo "no JDK 25 or newer found: set JAVA_HOME" >&2; exit 1; }

for port in 9592 8580 8099 8088; do
  if lsof -nP -iTCP:$port -sTCP:LISTEN >/dev/null 2>&1; then echo "port $port is in use: free it first" >&2; exit 1; fi
done

work=$(mktemp -d "${TMPDIR:-/tmp}/in-flight-video.XXXXXX")
stack="docker compose -f $repo/scripts/video/stack.yml"
shop=""
cleanup() {
  [ -n "$shop" ] && kill -TERM "$shop" 2>/dev/null || true
  pkill -TERM -f "in-flight.jar --no-open" 2>/dev/null || true
  $stack down -v >/dev/null 2>&1 || true
  rm -rf "$work"
}
trap cleanup EXIT INT TERM

echo "video: the stack"
$stack up -d --wait >/dev/null 2>&1
echo "video: the shop"
"$java" -cp "$jar" "$repo/scripts/video/Shop.java" localhost:9592 http://localhost:8580 8099 >"$work/shop.log" 2>&1 &
shop=$!
for _ in $(seq 1 60); do curl -sf localhost:8099/state >/dev/null && break; sleep 1; done
curl -sf localhost:8099/state >/dev/null || { echo "the shop did not start:" >&2; tail -20 "$work/shop.log" >&2; exit 1; }
sleep 6

echo "video: recording (about two minutes)"
JAVA_HOME=$(dirname "$(dirname "$java")") node "$repo/scripts/video/record.mjs" "$work" | grep -E '^[0-9.]+s |until|teaser|board|ERRORS'
take=$(ls "$work"/take/*.webm)

# The take's first frame is the blank page before anything paints. The board clip, when it was recorded, goes in at the
# take's `board` mark, fading in and out over 0.3 s. The preview, when it was recorded, comes first and fades into the
# take over half a second.
encode="-c:v libx264 -preset slow -crf 22 -pix_fmt yuv420p -movflags +faststart -an"
clean="setpts=PTS-STARTPTS,fps=30,format=yuv420p"
set -- -i "$take"
if [ -f "$work/board.json" ]; then
  read -r bfile bstart bdur bout bat <<EOF
$(node -p "const b = require('$work/board.json'); [b.file, b.start, b.dur, (b.dur - 0.3).toFixed(2), b.at].join(' ')")
EOF
  set -- "$@" -i "$bfile"
  main="[0:v]split[t1][t2];[t1]trim=start=0.2:end=$bat,$clean[m1];[t2]trim=start=$bat,$clean[m2];[1:v]trim=start=$bstart:duration=$bdur,$clean,fade=t=in:d=0.3,fade=t=out:st=$bout:d=0.3[b];[m1][b][m2]concat=n=3:v=1:a=0,fps=30[m]"
  next=2
else
  echo "video: no board scene"
  main="[0:v]trim=start=0.2,$clean[m]"
  next=1
fi
if [ -f "$work/teaser.json" ]; then
  read -r tfile tstart tdur toff <<EOF
$(node -p "const t = require('$work/teaser.json'); [t.file, t.start, t.dur, (t.dur - 0.5).toFixed(2)].join(' ')")
EOF
  set -- "$@" -i "$tfile"
  filter="$main;[$next:v]trim=start=$tstart:duration=$tdur,$clean[p];[p][m]xfade=transition=fade:duration=0.5:offset=$toff[v]"
else
  echo "video: no preview"
  filter="$main;[m]null[v]"
fi
ffmpeg -v error -y "$@" -filter_complex "$filter" -map "[v]" $encode "$out"
echo "video: $out, $(ffprobe -v error -show_entries format=duration -of csv=p=0 "$out" | cut -d. -f1) s"
