#!/bin/bash
set -e

# Change binding host:port for redis, and amqp
sed -i "/\[redis-client\]/,/host=/  s/host=.*/host=$REDIS_CLIENT_HOST/" ${CONFIG_PATH}/jasmin.cfg
sed -i "/\[redis-client\]/,/port=/  s/port=.*/port=$REDIS_CLIENT_PORT/" ${CONFIG_PATH}/jasmin.cfg
sed -i "/\[amqp-broker\]/,/host=/  s/host=.*/host=$AMQP_BROKER_HOST/" ${CONFIG_PATH}/jasmin.cfg
sed -i "/\[amqp-broker\]/,/port=/  s/port=.*/port=$AMQP_BROKER_PORT/" ${CONFIG_PATH}/jasmin.cfg

# The config reader resolves <SECTION>_<OPTION> from the environment, so credentials and log_privacy need no
# rewriting here. The jCli password is the exception: the file stores an MD5 digest, and the native
# JCLI_ADMIN_PASSWORD binding is unhexlified, so a plaintext value there stops the console binding at all.
if [ -n "${JCLI_ADMIN_PASSWORD_PLAIN:-}" ]; then
  sed -i "/\[jcli\]/a admin_password=$(printf '%s' "$JCLI_ADMIN_PASSWORD_PLAIN" | md5sum | cut -d' ' -f1)" ${CONFIG_PATH}/jasmin.cfg
fi

# Optional JSON outcome forwarder queue (empty = disabled)
if [ -n "${DLR_FORWARD_QUEUE:-}" ]; then
  sed -i "/\[dlr\]/a dlr_forward_queue=$DLR_FORWARD_QUEUE" ${CONFIG_PATH}/jasmin.cfg
fi

# Optional internal billing toggle (unset = Jasmin's built-in default of enabled)
if [ -n "${BILLING_FEATURE:-}" ]; then
  sed -i "/\[smpp-server\]/a billing_feature=$BILLING_FEATURE" ${CONFIG_PATH}/jasmin.cfg
  sed -i "/\[http-api\]/a billing_feature=$BILLING_FEATURE" ${CONFIG_PATH}/jasmin.cfg
fi

echo 'Cleaning lock files'
rm -f /tmp/*.lock


if [ "$2" = "--enable-interceptor-client" ]; then
  echo 'Starting interceptord'
  interceptord.py &
fi

echo 'Starting jasmind'
exec "$@"
