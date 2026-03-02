#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/../lib/common.sh"
ROOT="$(repo_root_from_script_dir "$SCRIPT_DIR")"
EXAMPLES="$ROOT/examples/revops"

TMP_DIR="$(mktemp -d /tmp/maintenance_e2e.XXXXXX)"
DB="$TMP_DIR/maintenance_e2e.nano"
DATA_APPEND="$TMP_DIR/append.jsonl"
VECTOR_DB="$TMP_DIR/vector_load_modes.nano"
VECTOR_SCHEMA="$TMP_DIR/vector_load_modes.pg"
VECTOR_OVERWRITE_DATA="$TMP_DIR/vector_overwrite.jsonl"
VECTOR_APPEND_DATA="$TMP_DIR/vector_append.jsonl"
VECTOR_MERGE_DATA="$TMP_DIR/vector_merge.jsonl"

cleanup() {
    rm -rf "$TMP_DIR"
}
trap cleanup EXIT

build_nanograph_binary "$ROOT"

cat > "$DATA_APPEND" << 'DATA'
{"type":"Signal","data":{"slug":"sig-maint-append","observedAt":"2026-02-16T00:00:00Z","summary":"Maintenance append event","urgency":"low","sourceType":"observation","assertion":"fact","createdAt":"2026-02-16T00:00:00Z"}}
DATA

info "Initializing and loading maintenance test database..."
INIT_JSON=$("$NG" --json init "$DB" --schema "$EXAMPLES/revops.pg")
assert_contains "$INIT_JSON" '"status":"ok"' "init --json status"

LOAD1_JSON=$("$NG" --json load "$DB" --data "$EXAMPLES/revops.jsonl" --mode overwrite)
assert_contains "$LOAD1_JSON" '"status":"ok"' "overwrite load --json status"

LOAD2_JSON=$("$NG" --json load "$DB" --data "$DATA_APPEND" --mode append)
assert_contains "$LOAD2_JSON" '"status":"ok"' "append load --json status"

info "Running vector @embed append/merge regression checks..."
cat > "$VECTOR_SCHEMA" << 'SCHEMA'
node Lesson {
    slug: String @key
    summary: String
    embedding: Vector(8)? @embed(summary)
}
SCHEMA

cat > "$VECTOR_OVERWRITE_DATA" << 'DATA'
{"type":"Lesson","data":{"slug":"lesson-1","summary":"alpha summary baseline"}}
DATA

cat > "$VECTOR_APPEND_DATA" << 'DATA'
{"type":"Lesson","data":{"slug":"lesson-2","summary":"beta summary append"}}
DATA

cat > "$VECTOR_MERGE_DATA" << 'DATA'
{"type":"Lesson","data":{"slug":"lesson-1","summary":"alpha summary updated"}}
{"type":"Lesson","data":{"slug":"lesson-3","summary":"gamma summary merge insert"}}
DATA

VECTOR_INIT_JSON=$("$NG" --json init "$VECTOR_DB" --schema "$VECTOR_SCHEMA")
assert_contains "$VECTOR_INIT_JSON" '"status":"ok"' "vector regression init --json status"

VECTOR_OVERWRITE_JSON=$(NANOGRAPH_EMBEDDINGS_MOCK=1 "$NG" --json load "$VECTOR_DB" --data "$VECTOR_OVERWRITE_DATA" --mode overwrite)
assert_contains "$VECTOR_OVERWRITE_JSON" '"status":"ok"' "vector regression overwrite load status"

VECTOR_APPEND_JSON=$(NANOGRAPH_EMBEDDINGS_MOCK=1 "$NG" --json load "$VECTOR_DB" --data "$VECTOR_APPEND_DATA" --mode append)
assert_contains "$VECTOR_APPEND_JSON" '"status":"ok"' "vector regression append load status"

VECTOR_MERGE_JSON=$(NANOGRAPH_EMBEDDINGS_MOCK=1 "$NG" --json load "$VECTOR_DB" --data "$VECTOR_MERGE_DATA" --mode merge)
assert_contains "$VECTOR_MERGE_JSON" '"status":"ok"' "vector regression merge load status"

VECTOR_EXPORT_JSONL=$("$NG" export --db "$VECTOR_DB" --format jsonl)
assert_contains "$VECTOR_EXPORT_JSONL" '"slug":"lesson-1"' "vector regression export includes updated key row"
assert_contains "$VECTOR_EXPORT_JSONL" '"summary":"alpha summary updated"' "vector regression keyed merge updates summary"
assert_contains "$VECTOR_EXPORT_JSONL" '"slug":"lesson-2"' "vector regression export includes appended row"
assert_contains "$VECTOR_EXPORT_JSONL" '"slug":"lesson-3"' "vector regression export includes merge-inserted row"
LESSON_ROW_COUNT=$(echo "$VECTOR_EXPORT_JSONL" | rg -c '"type":"Lesson"')
assert_int_eq "$LESSON_ROW_COUNT" 3 "vector regression keeps expected row count after append+merge"

info "Running compact command..."
COMPACT_JSON=$("$NG" --json compact "$DB" --target-rows-per-fragment 1024)
assert_contains "$COMPACT_JSON" '"status":"ok"' "compact --json status"

info "Running doctor command..."
DOCTOR_JSON=$("$NG" --json doctor "$DB")
assert_contains "$DOCTOR_JSON" '"status":"ok"' "doctor --json status before cleanup"
assert_contains "$DOCTOR_JSON" '"healthy":true' "doctor reports healthy before cleanup"

info "Running cleanup command..."
CLEANUP_JSON=$("$NG" --json cleanup "$DB" --retain-tx-versions 2 --retain-dataset-versions 1)
assert_contains "$CLEANUP_JSON" '"status":"ok"' "cleanup --json status"
assert_contains "$CLEANUP_JSON" '"tx_rows_kept"' "cleanup reports tx retention stats"

info "Running CDC analytics materializer..."
CDC_MAT_JSON=$("$NG" --json cdc-materialize "$DB" --min-new-rows 1)
assert_contains "$CDC_MAT_JSON" '"status":"ok"' "cdc-materialize --json status"
assert_contains "$CDC_MAT_JSON" '"dataset_written":true' "cdc-materialize writes analytics dataset"
[ -d "$DB/__cdc_analytics" ] || fail "expected $DB/__cdc_analytics to exist"
pass "cdc analytics dataset exists"

CDC_MAT_SKIP_JSON=$("$NG" --json cdc-materialize "$DB" --min-new-rows 99999)
assert_contains "$CDC_MAT_SKIP_JSON" '"status":"ok"' "cdc-materialize threshold run status"
assert_contains "$CDC_MAT_SKIP_JSON" '"skipped_by_threshold":true' "cdc-materialize threshold skip"

DOCTOR_AFTER_JSON=$("$NG" --json doctor "$DB")
assert_contains "$DOCTOR_AFTER_JSON" '"status":"ok"' "doctor --json status after cleanup"
assert_contains "$DOCTOR_AFTER_JSON" '"healthy":true' "doctor reports healthy after cleanup"

info "Running describe command..."
DESCRIBE_JSON=$("$NG" describe --db "$DB" --format json)
assert_contains "$DESCRIBE_JSON" '"schema_ir_version"' "describe includes schema version"
assert_contains "$DESCRIBE_JSON" '"name": "Opportunity"' "describe includes Opportunity node type"

CHANGES_JSON=$("$NG" changes "$DB" --since 0 --format json)
assert_contains "$CHANGES_JSON" '"db_version": 2' "changes keeps replay window events after cleanup"
assert_contains "$CHANGES_JSON" '"sig-maint-append"' "changes includes append payload after cleanup"

echo ""
pass "maintenance CLI e2e passed"
