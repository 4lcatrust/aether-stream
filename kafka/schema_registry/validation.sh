#!/usr/bin/env bash
set -euo pipefail
SR_HOST="http://localhost:8085"
SR_DOCKER="http://schema-registry:8081"
NET="aether-stream_aether-net"
KAFKA_BROKER="kafka:9092"
KCAT_IMAGE="edenhill/kcat:1.7.1"
KCAT_RUN=(docker run --rm --network "${NET}" --entrypoint sh "${KCAT_IMAGE}" -lc)
SUBJECTS=(
  "bronze.market_caps.avro-value"
  "bronze.market_prices.avro-value"
)
echo "============ SR global config ============"
curl -fsS "${SR_HOST}/config" | jq
echo "============ Subjects ============"
SUBJECTS_JSON="$(curl -fsS "${SR_HOST}/subjects" | jq -c 'sort')"
echo "${SUBJECTS_JSON}" | jq
echo "============ Assert expected subjects ============"
ACTUAL_SUBJECTS_JSON="$(curl -fsS "${SR_HOST}/subjects" | jq -c 'sort')"
EXPECTED_SUBJECTS_JSON="$(printf '%s\n' "${SUBJECTS[@]}" | jq -R . | jq -cs 'sort')"
if [[ "${ACTUAL_SUBJECTS_JSON}" != "${EXPECTED_SUBJECTS_JSON}" ]]; then
  echo "ERROR: subjects mismatch"
  echo "expected=${EXPECTED_SUBJECTS_JSON}"
  echo "actual=${ACTUAL_SUBJECTS_JSON}"
  exit 1
fi
for subject in "${SUBJECTS[@]}"; do
  echo "============ Latest: ${subject} ============"
  curl -fsS "${SR_HOST}/subjects/${subject}/versions/latest" | jq '{subject, version, id}'
done
echo "============ Wire format headers (magic + schemaId) ============"
"${KCAT_RUN[@]}" "kcat -C -b ${KAFKA_BROKER} -t bronze.market_caps.avro -c 1 -o beginning -u | head -c 5 | od -An -t u1"
"${KCAT_RUN[@]}" "kcat -C -b ${KAFKA_BROKER} -t bronze.market_prices.avro -c 1 -o beginning -u | head -c 5 | od -An -t u1"
echo "============ Decode using SR (value_schema_id must appear) ============"
"${KCAT_RUN[@]}" "kcat -C -b ${KAFKA_BROKER} -t bronze.market_caps.avro -c 1 -o beginning -u -s value=avro -r ${SR_DOCKER} -J" \
  | jq '{topic, value_schema_id}'
"${KCAT_RUN[@]}" "kcat -C -b ${KAFKA_BROKER} -t bronze.market_prices.avro -c 1 -o beginning -u -s value=avro -r ${SR_DOCKER} -J" \
  | jq '{topic, value_schema_id}'
echo "Schema Registry Validation Pass"