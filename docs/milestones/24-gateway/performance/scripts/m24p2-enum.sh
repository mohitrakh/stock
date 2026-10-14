#!/usr/bin/env bash
# Milestone 24 part 2: m24p1-enum.sh's check, on the Part 1 and the Part 2 binary, the same day.
# One idle exchange per binary; one account registered; then, alternating, 20 logins with that
# email and a wrong password, and 20 with an email that has no account. curl prints each answer's
# status, total time and body.
# Usage: m24p2-enum.sh
set -u
source ~/stock-scripts/m24-lib.sh
echo "### m24p2-enum: $(date -Is)"
login() { # EMAIL
  docker exec $X curl -s -m 10 -o /tmp/body -w '%{http_code} %{time_total}' -X POST 127.0.0.1:4000/users/login \
    -H 'Content-Type: application/json' -d "{\"email\":\"$1\",\"password\":\"wrong-password-1\"}"
  echo " $(docker exec $X cat /tmp/body)"
}
for binary in stock-m24p1 stock-m24p2; do
  BIN=/scripts/$binary
  echo "### $binary"
  start_exchange no
  host_note
  docker exec $X curl -s -o /dev/null -w 'register: %{http_code}\n' -X POST 127.0.0.1:4000/users/register \
    -H 'Content-Type: application/json' \
    -d '{"username":"known","email":"known@bench.test","password":"right-password-1"}'
  echo "right password: $(docker exec $X curl -s -o /dev/null -w '%{http_code} %{time_total}' -X POST 127.0.0.1:4000/users/login -H 'Content-Type: application/json' -d '{"email":"known@bench.test","password":"right-password-1"}')"
  echo "GET /users: $(docker exec $X curl -s -w ' [%{http_code}]' 127.0.0.1:4000/users)"
  for i in $(seq 1 20); do
    echo "known   $(login known@bench.test)"
    echo "unknown $(login nobody-$i@bench.test)"
  done
done
clean
echo "### done"
