processing-enabled = true
level-manager-enabled = true
compaction-workers-enabled = true

// This is aq comment
processor-count = 96
max-processor-batches-in-progress = 1000
memtable-max-size-bytes = "10000000"
memtable-max-replace-interval = "37s" // and another comment on the line
memtable-flush-queue-max-size = 50
store-write-blocked-retry-interval = "777ms"
min-replicas = 3
max-replicas = 5
table-format = 1
min-snapshot-interval = "13s"
idle-processor-check-interval = "23s"
batch-flush-check-interval = "7s"
consumer-retry-interval = "30s"
max-backfill-batch-size = 998
client-type = 1
forward-resend-delay = "876ms"

query-max-batch-rows = 999

http-api-path = "/wibble"

// Level-Manager config
registry-format = 3
master-registry-record-id = "avocados"
max-registry-segment-table-entries = 12345
level-manager-flush-interval = "10s"
segment-cache-max-size = 777
level-manager-retry-delay = "750ms"
l0-compaction-trigger = 12
l0-max-tables-before-blocking = 21
l1-compaction-trigger = 23
level-multiplier = 3
compaction-poller-timeout = "777ms"
compaction-job-timeout = "7m"
compaction-worker-count = 12
ss-table-delete-check-interval = "350ms"
ss-table-delete-delay = "1h"
ss-table-register-retry-delay = "35s"
ss-table-push-retry-delay = "6s"
prefix-retention-remove-check-interval = "17s"
compaction-max-ss-table-size = 54321

command-compaction-interval = "3s"

prefix-retention-refresh-interval = "13s"
table-cache-max-size-bytes = "12345678"

// Cluster-manager config

cluster-name = "the_cluster_name"
cluster-manager-lock-timeout = "7m"
cluster-manager-key-prefix = "test_keyprefix"
cluster-manager-addresses = ["etcd1","etcd2"]
cluster-eviction-timeout = "13s"
cluster-state-update-interval = "1500ms"
etcd-call-timeout = "7s"

sequences-object-name = "my_sequences"
sequences-retry-delay = "300ms"

object-store-type = "dev"
bucket-name = "silly-bucket"
dev-object-store-addresses = [
"addr23"
]


/*
And one of these
comments
*/

cluster-addresses = [
  "addr1",
  "addr2",
  "addr3",
  "addr4",
  "addr5"
]

http-api-enabled           = true
http-api-addresses  = [
  "addr11-1",
  "addr12-1",
  "addr13-1",
  "addr14-1",
  "addr15-1"
]
http-api-tls-key-path      = "http-key-path"
http-api-tls-cert-path     = "http-cert-path"
http-api-tls-client-certs-path = "http-client-certs-path"
http-api-tls-client-auth = "require-and-verify-client-cert"

kafka-server-enabled              = true
kafka-server-listener-addresses  = [
  "kafka1:9301",
  "kafka2:9301",
  "kafka3:9301",
  "kafka4:9301",
  "kafka5:9301"
]
kafka-use-server-timestamp        = true
kafka-server-listener-tls-enabled       = true
kafka-server-listener-tls-key-path      = "kafka-key-path"
kafka-server-listener-tls-cert-path     = "kafka-cert-path"
kafka-server-listener-tls-client-certs-path = "kafka-client-certs-path"
kafka-server-listener-tls-client-auth = "require-and-verify-client-cert"
kafka-initial-join-delay = "2s"
kafka-min-session-timeout = "7s"
kafka-max-session-timeout = "25s"
kafka-new-member-join-timeout = "4s"
kafka-fetch-cache-max-size-bytes = "7654321"

dd-profiler-types                 = "HEAP,CPU"
dd-profiler-service-name          = "my-service"
dd-profiler-environment-name      = "playing"
dd-profiler-port                  = 1324
dd-profiler-version-name          = "2.3"
dd-profiler-host-env-var-name     = "FOO_IP"

cluster-tls-enabled       = true
cluster-tls-key-path      = "intra-cluster-key-path"
cluster-tls-cert-path     = "intra-cluster-cert-path"
cluster-tls-client-certs-path = "intra-cluster-client-certs-path"
cluster-tls-client-auth = "require-and-verify-client-cert"

log-format = "json"
log-level = "debug"

version-completed-broadcast-interval = "2s"
version-manager-store-flushed-interval = "23s"
version-manager-level-manager-retry-delay = "13s"

authentication-enabled = true
authentication-cache-timeout = "12s"

wasm-module-instances = 23

log-scope = "foo"