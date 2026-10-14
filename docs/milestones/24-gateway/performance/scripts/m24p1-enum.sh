#!/usr/bin/env bash
# Milestone 24 part 1: what a login answer reveals about whether an email has an account, by its
# body and by its time. One idle exchange. One account is registered; then, alternating, 20 logins
# with that email and a wrong password, and 20 with an email that has no account. curl prints each
# answer's status, body and total time.
# Usage: m24p1-enum.sh
set -u
source ~/stock-scripts/m24-lib.sh
echo "### m24p1-enum: $BIN, $(date -Is)"
start_exchange no
host_note
login() { # EMAIL
  docker exec $X curl -s -m 10 -o /tmp/body -w '%{http_code} %{time_total}' -X POST 127.0.0.1:4000/users/login \
    -H 'Content-Type: application/json' -d "{\"email\":\"$1\",\"password\":\"wrong-password-1\"}"
  echo " $(docker exec $X cat /tmp/body)"
}
docker exec $X curl -s -o /dev/null -w 'register: %{http_code}\n' -X POST 127.0.0.1:4000/users/register \
  -H 'Content-Type: application/json' \
  -d '{"username":"known","email":"known@bench.test","password":"right-password-1"}'
for i in $(seq 1 20); do
  echo "known   $(login known@bench.test)"
  echo "unknown $(login nobody-$i@bench.test)"
done
clean
echo "### done"
