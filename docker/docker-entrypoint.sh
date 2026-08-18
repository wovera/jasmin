#!/bin/bash
set -e

# Change binding host:port for redis, and amqp
sed -i "/\[redis-client\]/,/host=/  s/host=.*/host=$REDIS_CLIENT_HOST/" ${CONFIG_PATH}/jasmin.cfg
sed -i "/\[redis-client\]/,/port=/  s/port=.*/port=$REDIS_CLIENT_PORT/" ${CONFIG_PATH}/jasmin.cfg
sed -i "/\[amqp-broker\]/,/host=/  s/host=.*/host=$AMQP_BROKER_HOST/" ${CONFIG_PATH}/jasmin.cfg
sed -i "/\[amqp-broker\]/,/port=/  s/port=.*/port=$AMQP_BROKER_PORT/" ${CONFIG_PATH}/jasmin.cfg

# A container restart re-runs this script against the same writable layer, so a bare append accumulates a second
# copy of the option and configparser then refuses the whole file. Replace within the section instead of adding.
set_option() {
  local section="$1" key="$2" value="$3"
  sed -i "/^\\[$section\\]/,/^\\[/ { /^[[:space:]]*$key[[:space:]]*=/d }" ${CONFIG_PATH}/jasmin.cfg
  sed -i "/^\\[$section\\]/a $key=$value" ${CONFIG_PATH}/jasmin.cfg
}

# The config reader resolves <SECTION>_<OPTION> from the environment, so credentials and log_privacy need no
# rewriting here. The jCli password is the exception: the file stores an MD5 digest, and the native
# JCLI_ADMIN_PASSWORD binding is unhexlified, so a plaintext value there stops the console binding at all.
if [ -n "${JCLI_ADMIN_PASSWORD_PLAIN:-}" ]; then
  set_option jcli admin_password "$(printf '%s' "$JCLI_ADMIN_PASSWORD_PLAIN" | md5sum | cut -d' ' -f1)"
fi

# Optional JSON outcome forwarder queue (empty = disabled)
if [ -n "${DLR_FORWARD_QUEUE:-}" ]; then
  set_option dlr dlr_forward_queue "$DLR_FORWARD_QUEUE"
fi

# Optional internal billing toggle (unset = Jasmin's built-in default of enabled)
if [ -n "${BILLING_FEATURE:-}" ]; then
  set_option smpp-server billing_feature "$BILLING_FEATURE"
  set_option http-api billing_feature "$BILLING_FEATURE"
fi

echo 'Cleaning lock files'
rm -f /tmp/*.lock


if [ "$2" = "--enable-interceptor-client" ]; then
  echo 'Starting interceptord'
  interceptord.py &
fi

# jasmind restores its persisted profile at startup only when given jCli credentials, so a deployment that
# replaced the factory pair comes back from a restart with no connectors and a console that still answers.
set -- "$@" -u "${JCLI_ADMIN_USERNAME:-jcliadmin}" -p "${JCLI_ADMIN_PASSWORD_PLAIN:-jclipwd}"

echo 'Starting jasmind'
exec "$@"
