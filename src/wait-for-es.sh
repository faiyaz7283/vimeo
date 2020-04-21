#!/bin/sh

set -e
url="$1"
shift
cmd="$@"
#
until [ `curl -sL -w "%{http_code}\\n" "$url" -o /dev/null` -eq "200" ]; do
    >&2 echo "Server is unavailable at $url - sleeping"
    sleep 1
done
>&2 echo "Server is up at $url - executing command"
  exec $cmd