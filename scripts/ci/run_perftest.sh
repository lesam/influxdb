#!/bin/bash -ex

echo "Running as user: $(whoami)"

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

# Source env variables
if [[ -f /home/ubuntu/vars.sh ]] ; then
  . /home/ubuntu/vars.sh
fi

install_influxdb() {
  # Install influxdb
  DEBIAN_FRONTEND=noninteractive apt-get install --assume-yes /home/ubuntu/influxdb2*amd64.deb
  systemctl start influxdb

  # set up influxdb
  export INFLUXDB2=true
  export TEST_ORG=example_org
  export TEST_TOKEN=token
  result="$(curl -s -o /dev/null -H "Content-Type: application/json" -XPOST -d '{"username": "default", "password": "thisisnotused", "retentionPeriodSeconds": 0, "org": "'"$TEST_ORG"'", "bucket": "unused_bucket", "token": "'"$TEST_TOKEN"'"}' http://localhost:8086/api/v2/setup -w %{http_code})"
  if [[ "$result" != "201" ]] ; then
    echo "Influxdb2 failed to setup correctly"
    exit 1
  fi
}

install_telegraf() {
  # Install Telegraf
  wget -qO- https://repos.influxdata.com/influxdb.key | apt-key add -
  echo "deb https://repos.influxdata.com/ubuntu focal stable" | tee /etc/apt/sources.list.d/influxdb.list

  DEBIAN_FRONTEND=noninteractive apt-get update
  DEBIAN_FRONTEND=noninteractive apt-get install -y git jq telegraf awscli

  # Install influx_tools
  aws --region us-west-2 s3 cp s3://perftest-binaries-influxdb/influx_tools/influx_tools-d3be25b251256755d622792ec91826c5670c6106 ./influx_tools
  mv ./influx_tools /usr/bin/influx_tools
  chmod 755 /usr/bin/influx_tools

  root_branch="$(echo "${INFLUXDB_VERSION}" | rev | cut -d '-' -f1 | rev)"
  log_date=$(date +%Y%m%d%H%M%S)

  mkdir -p /etc/telegraf
cat << EOF > /etc/telegraf/telegraf.conf
[[outputs.influxdb_v2]]
  urls = ["https://us-west-2-1.aws.cloud2.influxdata.com"]
  token = "${DB_TOKEN}"
  organization = "${CLOUD2_ORG}"
  bucket = "${CLOUD2_BUCKET}"

[[inputs.file]]
  name_override = "ingest"
  files = ["$working_dir/test-ingest-*.json"]
  data_format = "json"
  json_strict = true
  json_string_fields = [
    "branch",
    "commit",
    "i_type",
    "time",
    "use_case"
  ]
  tagexclude = ["host"]
  json_time_key = "time"
  json_time_format = "unix"
  tag_keys = [
    "i_type",
    "use_case",
    "branch"
  ]

[[inputs.file]]
  name_override = "query"
  files = ["$working_dir/test-query-*.json"]
  data_format = "json"
  json_strict = true
  json_string_fields = [
    "branch",
    "commit",
    "i_type",
    "query_format",
    "query_type",
    "time",
    "use_case"
  ]
  tagexclude = ["host"]
  json_time_key = "time"
  json_time_format = "unix"
  tag_keys = [
    "i_type",
    "query_format",
    "use_case",
    "query_type",
    "branch"
  ]
EOF
  systemctl restart telegraf
}


install_go() {
  # install golang latest version
  go_version=$(curl https://golang.org/VERSION?m=text)
  go_endpoint="$go_version.linux-amd64.tar.gz"

  wget "https://dl.google.com/go/$go_endpoint" -O "$working_dir/$go_endpoint"
  rm -rf /usr/local/go
  tar -C /usr/local -xzf "$working_dir/$go_endpoint"

  # set env variables necessary for go to work during cloud-init
  if [[ `whoami` = root ]] ; then
    mkdir -p /root/go/bin
    export HOME=/root
    export GOPATH=/root/go
    export PATH=$PATH:/usr/local/go/bin:$GOPATH/bin
  fi
  go version
}

install_influxdb_comparisons() {
  # install influxdb-comparisons cmds
  go get \
    github.com/influxdata/influxdb-comparisons/cmd/bulk_data_gen \
    github.com/influxdata/influxdb-comparisons/cmd/bulk_load_influx \
    github.com/influxdata/influxdb-comparisons/cmd/bulk_query_gen \
    github.com/influxdata/influxdb-comparisons/cmd/query_benchmarker_influxdb
}

# Helper functions containing common logic
bucket_id() {
  bucket_id=$(curl -H "Authorization: Token $TEST_TOKEN" "http://${NGINX_HOST}:8086/api/v2/buckets?org=$TEST_ORG" | jq -r ".buckets[] | select(.name | contains(\"$db_name\")).id")
  echo $bucket_id
}


force_compaction() {
  # id of the bucket that will be compacted
  b_id=$(bucket_id)

  # stop daemon and force compaction
  systemctl stop influxdb
  set +e
  shards=$(find /var/lib/influxdb/engine/data/$b_id/autogen/ -maxdepth 1 -mindepth 1)

  set -e
  for shard in $shards; do
    if [[ -n "$(find $shard -name *.tsm)" ]]; then
      # compact as the influxdb user in order to keep file permissions correct
      sudo -u influxdb influx_tools compact-shard -force -verbose -path $shard
    fi
  done

  # restart daemon
  systemctl start influxdb
}

query_types() {
  case $1 in
    window-agg|group-agg|bare-agg|ungrouped-agg|group-window-transpose-low-card|group-window-transpose-high-card)
      echo min mean max first last count sum
      ;;
    iot)
      echo fast-query-small-data standalone-filter aggregate-keep aggregate-drop sorted-pivot
      ;;
    metaquery)
      echo field-keys tag-values cardinality
      ;;
    multi-measurement)
      echo multi-measurement-or
      ;;
    *)
      echo "unknown use-case: $1"
      exit 1
      ;;
  esac
}

# Many of the query generator use-cases have aliases to make reporting more
# clear. This function will translate the aliased query use cases to their
# dataset use cases. Effectively this means "for this query use case, run the
# queries against this dataset use case".
queries_for_dataset() {
  case $1 in
    iot)
      echo window-agg group-agg bare-agg ungrouped-agg iot group-window-transpose-low-card
      ;;
    metaquery)
      echo metaquery group-window-transpose-high-card
      ;;
    multi-measurement)
      echo multi-measurement
      ;;
    *)
      echo "unknown use-case: $1"
      exit 1
      ;;
  esac
}

org_flag() {
  case $1 in
    flux-http)
      echo -organization=$TEST_ORG
      ;;
    http)
      echo -use-compatibility=true
      ;;
    *)
      echo echo "unknown query format: $1"
      exit 1
      ;;
  esac
}

create_dbrp() {
curl -XPOST -H "Authorization: Token ${TEST_TOKEN}" \
  -d "{\"org\":\"${TEST_ORG}\",\"bucketID\":\"$(bucket_id)\",\"database\":\"$db_name\",\"retention_policy\":\"autogen\"}" \
  http://${NGINX_HOST}:8086/api/v2/dbrps
}

bulk_load_influx() {
  $GOPATH/bin/bulk_load_influx "$@" | \
    jq ". += {branch: \"$INFLUXDB_VERSION\", commit: \"$TEST_COMMIT\", time: \"$TEST_COMMIT_TIME\", i_type: \"$DATA_I_TYPE\", use_case: \"$test_name\"}" > "$working_dir/test-ingest-$test_name.json"

  # Cleanup from the data generation and loading.
  force_compaction

  # Generate a DBRP mapping for use by InfluxQL queries.
  create_dbrp
}

bulk_data_file_loader() {
  local data_fname="influx-bulk-records-usecase-$test_name"
  # Note -scale-var=1000 implies cardinality of 1MM
  dry_out $GOPATH/bin/bulk_data_gen \
      -seed=$TEST_COMMIT_TIME \
      -use-case=$test_name \
      -scale-var=1000 \
      -timestamp-start="$start_time" \
      -timestamp-end="$end_time" > \
    ${USECASE_DIR}/$data_fname

  load_opts="-file=${USECASE_DIR}/$data_fname -batch-size=5000 -workers=4 -urls=http://${NGINX_HOST}:8086 -do-abort-on-exist=false -do-db-create=true -backoff=1s -backoff-timeout=300m0s"
  if [[ -z $INFLUXDB2 || $INFLUXDB2 = true ]] ; then
    load_opts="$load_opts -organization=$TEST_ORG -token=$TEST_TOKEN"
  fi

  dry_out bulk_load_influx $load_opts

  rm ${USECASE_DIR}/$data_fname

}

dry_out() {
  if [[ -n "$DRY_RUN" ]] ; then
    echo "DRY RUN: $@" >&3
  else
    "$@"
  fi
}

run_dataset() {
  yaml_file="$1"

  test_name="$( yq e '.name' "$yaml_file" )"
  USECASE_DIR="${DATASET_DIR}/$test_name"
  mkdir "$USECASE_DIR"

  start_time="$( yq e '.start_time' "$yaml_file" )"
  end_time="$( yq e '.end_time' "$yaml_file" )"

  data_loader_type="$( yq e '.data.type' "$yaml_file" )"
  case "$data_loader_type" in
    bulk_data_file_loader)
      bulk_data_file_loader
      ;;
    *)
      echo "ERROR: unknown data loader type $data_loader_type"
      exit 1
      ;;
  esac

  # Generate queries to test.
  query_files=""
  for TEST_FORMAT in http flux-http ; do
    for query_usecase in $(queries_for_dataset $test_name) ; do
      for type in $(query_types $query_usecase) ; do
        local query_fname="${TEST_FORMAT}_${query_usecase}_${type}"
        local scale_var=1000
        dry_out $GOPATH/bin/bulk_query_gen \
            -use-case=$query_usecase \
            -query-type=$type \
            -format=influx-${TEST_FORMAT} \
            -timestamp-start="$start_time" \
            -timestamp-end="$end_time" \
            -queries=$queries \
            -scale-var=$scale_var > \
          ${USECASE_DIR}/$query_fname
        query_files="$query_files $query_fname"
      done
    done
  done

  # Run the query tests applicable to this dataset.
  for query_file in $query_files; do
    format=$(echo $query_file | cut -d '_' -f1)
    query_usecase=$(echo $query_file | cut -d '_' -f2)
    type=$(echo $query_file | cut -d '_' -f3)
    local workers=4

    dry_out ${GOPATH}/bin/query_benchmarker_influxdb \
        -file=${USECASE_DIR}/$query_file \
        -urls=http://${NGINX_HOST}:8086 \
        -debug=0 \
        -print-interval=0 \
        -json=true \
        $(org_flag $format) \
        -token=$TEST_TOKEN \
        -workers=$workers \
        -benchmark-duration=$duration | \
      jq '."all queries"' | \
      jq -s '.[-1]' | \
      jq ". += {use_case: \"$query_usecase\", query_type: \"$type\", branch: \"$INFLUXDB_VERSION\", commit: \"$TEST_COMMIT\", time: \"$TEST_COMMIT_TIME\", i_type: \"$DATA_I_TYPE\", query_format: \"$format\"}" > \
        $working_dir/test-query-$format-$query_usecase-$type.json

      # Restart daemon between query tests.
      dry_out systemctl restart influxdb
  done

  # Delete DB to start anew.
  dry_out curl -X DELETE -H "Authorization: Token ${TEST_TOKEN}" http://${NGINX_HOST}:8086/api/v2/buckets/$(bucket_id)
  rm -rf "$USECASE_DIR"
}


##########################
## Setup for perf tests ##
##########################

DRY_RUN=
while true; do
  case $1 in
    -d | --dry-run )
      DRY_RUN=true
      shift
      ;;
    *)
      break ;;
  esac
done

working_dir=$(mktemp -d)

if [[ -z "DRY_RUN" ]] ; then
  DATASET_DIR=/mnt/ramdisk
  mkdir -p "$DATASET_DIR"
  mount -t tmpfs -o size=32G tmpfs "$DATASET_DIR"

  install_influxdb
  install_telegraf
  install_go
  install_influxdb_comparisons
else
  # The intention of the dry run is to be able to run something like the following command:
  #
  # TEST_TOKEN=mytoken NGINX_HOST=somehost TEST_ORG=foo GOPATH=/go/path ./scripts/ci/run_perftest.sh -d 2>/dev/null
  #
  # And have a repeatable output to test script refactoring quickly

  tmpdir="$(dirname "$working_dir")"
  DATASET_DIR="$tmpdir/perftest_dataset_dir"
  working_dir="$tmpdir/perftest_working_dir"
  mkdir "$DATASET_DIR"
  mkdir "$working_dir"
  exec 3>&1
fi

# Common variables used across all tests
db_name="benchmark_db"

# How many queries to generate.
queries=500

# How long to run each set of query tests. Specify a duration to limit the maximum amount of time the queries can run,
# since individual queries can take a long time.
duration=30s

##########################
## Run and record tests ##
##########################

# Generate and ingest bulk data. Record the time spent as an ingest test if
# specified, and run the query performance tests for each dataset.
for file in "$SCRIPT_DIR"/perf-tests/* ; do
  run_dataset $file
done

if [[ -z "DRY_RUN" ]] ; then
  echo "Using Telegraph to report results from the following files:"
  ls $working_dir
  if [[ "${TEST_RECORD_RESULTS}" = "true" ]] ; then
    telegraf --debug --once
  else
    telegraf --debug --test
  fi
else
  # For the dry run we pick deterministic names for the directories, so delete them
  rm -rf "$DATASET_DIR"
  rm -rf "$working_dir"
fi
