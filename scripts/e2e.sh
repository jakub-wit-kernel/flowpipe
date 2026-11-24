#!/usr/bin/env bash
# SPDX-License-Identifier: MIT
#
# End-to-end: compare the flowpipe CLI with standard tools on generated text.
#   scripts/e2e.sh path/to/flowpipe
set -euo pipefail

FP=$1
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

python3 - "$tmp/corpus.txt" <<'EOF'
import random, sys
random.seed(42)
words = ["the", "The", "kernel", "Queue", "lock-free", "don't", "x86_64",
         "alpha", "beta", "gamma", "delta", "epoll", "mutex", "a", "of"]
weights = [30, 5, 8, 4, 3, 2, 2, 6, 5, 4, 3, 2, 2, 10, 9]
with open(sys.argv[1], "w") as f:
    for _ in range(60000):
        line = " ".join(random.choices(words, weights, k=random.randint(1, 20)))
        f.write(line + random.choice([".", ",", "!", ""]) + "\n")
EOF
echo "corpus: $(wc -c <"$tmp/corpus.txt") bytes"

# 1. upper: byte-identical to tr, across worker counts and tiny chunks.
tr 'a-z' 'A-Z' <"$tmp/corpus.txt" >"$tmp/want_upper"
for args in "-j 1" "-j 4" "-j 8 -c 1" "-j 3 --queue mutex -c 7"; do
	# shellcheck disable=SC2086
	"$FP" upper $args "$tmp/corpus.txt" -o "$tmp/got_upper"
	cmp "$tmp/want_upper" "$tmp/got_upper"
	# shellcheck disable=SC2086
	"$FP" upper $args <"$tmp/corpus.txt" | cmp "$tmp/want_upper" -
	echo "upper $args: identical"
done

# 2. wc: same counts as a python reference (same tokenizer, ties by word).
python3 - "$tmp/corpus.txt" >"$tmp/want_wc" <<'EOF'
import collections, re, sys
text = open(sys.argv[1]).read().lower()
c = collections.Counter(re.findall(r"[a-z0-9_']+", text))
for w, n in sorted(c.items(), key=lambda kv: (-kv[1], kv[0]))[:15]:
    print(n, w)
EOF
for args in "-j 1" "-j 4 -c 16" "-j 2 --queue mutex"; do
	# shellcheck disable=SC2086
	"$FP" wc $args --top 15 "$tmp/corpus.txt" 2>/dev/null >"$tmp/got_wc"
	diff "$tmp/want_wc" "$tmp/got_wc"
	echo "wc $args: matches reference"
done

# 3. sum: CRC-32 matches zlib, in argument order, plus a missing file.
for i in 1 2 3; do head -c $((i * 100000)) "$tmp/corpus.txt" >"$tmp/f$i"; done
"$FP" sum -j 3 "$tmp/f1" "$tmp/f2" "$tmp/f3" >"$tmp/got_sum"
python3 - "$tmp/f1" "$tmp/f2" "$tmp/f3" >"$tmp/want_sum" <<'EOF'
import sys, zlib
for p in sys.argv[1:]:
    d = open(p, "rb").read()
    print("%08x %d %s" % (zlib.crc32(d), len(d), p))
EOF
diff "$tmp/want_sum" "$tmp/got_sum"
if "$FP" sum "$tmp/missing" 2>/dev/null; then
	echo "sum of a missing file should fail"; exit 1
fi
echo "sum: matches zlib"

# 4. Ctrl-C cancels promptly even while input is idle: the writer stays
#    open (no EOF), so only the cancel path can end the run.
mkfifo "$tmp/fifo"
"$FP" upper -j 2 "$tmp/fifo" -o /dev/null &
pid=$!
exec 3>"$tmp/fifo"		# open blocks until flowpipe opens the FIFO
printf 'some data\n' >&3
sleep 0.5			# let it reach the blocking read
kill -INT "$pid"
status=0
for _ in $(seq 100); do		# must exit within 10 s, writer still open
	kill -0 "$pid" 2>/dev/null || break
	sleep 0.1
done
if kill -0 "$pid" 2>/dev/null; then
	echo "flowpipe did not exit after SIGINT with input idle"; kill -9 "$pid"; exit 1
fi
wait "$pid" || status=$?
exec 3>&-
[[ $status -eq 130 ]] || { echo "expected exit 130 after SIGINT, got $status"; exit 1; }
echo "SIGINT with idle input: cancelled cleanly (130)"
echo "e2e: all checks passed"
