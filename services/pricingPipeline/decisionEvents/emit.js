/**
 * Emit a single decision_event row to the per-office BigQuery events dataset.
 *
 * Append-only. No deduplication on retries (per Audit 1.1A in
 * plans/active/20260504_pricing_pipeline_refactor/06_decision_log.md). Re-runs
 * produce duplicate-by-(plan_id, account, rule_id) rows that differ in
 * `evaluated_at` — drift forensics require both rows. The trace UI filters to
 * "latest per (account, phase, rule_id) within a plan" via window function.
 *
 * BQ insert is non-streaming-batch via @google-cloud/bigquery's `table.insert()`.
 * Failures bubble up to the caller; per-phase wrappers catch errors and convert
 * them to evaluation_error events before re-throwing.
 *
 * Note: this writes to `pco-{env}.rcp_{office_key}.decision_event` — events
 * live alongside the analytical RCP tables in the same per-office dataset
 * (one dataset per office for everything per-office). The dataset already
 * exists (created by the BQ pipeline); this code only writes to the
 * decision_event table inside it. Table bootstrap is a separate concern
 * (Step 1 of the implementation plan).
 */

import { bigqueryClient } from '../../../utils/bigquery.js';
import logger from '../../../lib/logger.js';
import { EmitArgsSchema } from './types.js';

const DECISION_EVENT_TABLE = 'decision_event';
/**
 * BQ table.insert() accepts up to 50K rows per call but practical limits are
 * lower (10MB request size). 2000 rows × ~1KB each ≈ 2MB — comfortable
 * headroom even with large inputs/outputs JSON.
 */
const BATCH_CHUNK_SIZE = 2000;

/**
 * Insert one decision_event row.
 *
 * @param {import('./types.js').EmitArgs} args
 * @returns {Promise<void>}
 */
export async function emitDecisionEvent(args) {
    // Validate at the boundary — fail loudly if a wrapper ships a malformed event.
    const parsed = EmitArgsSchema.parse(args);

    const datasetId = `rcp_${parsed.office_key}`;
    const dataset = bigqueryClient.dataset(datasetId);
    const table = dataset.table(DECISION_EVENT_TABLE);

    // BigQuery JSON columns can accept JS objects directly when using `table.insert()`,
    // but we explicitly stringify here so the row shape matches the BQ schema (JSON columns
    // accept either a JSON string or a structured object — strings are unambiguous).
    const row = {
        plan_id:           parsed.plan_id,
        office_key:        parsed.office_key,
        master_account_id: parsed.master_account_id,
        subscription_id:   parsed.subscription_id ?? null,
        phase:             parsed.phase,
        rule_id:           parsed.rule_id,
        decision_code:     parsed.decision_code,
        decision_reason:   parsed.decision_reason ?? null,
        inputs:            parsed.inputs  ? JSON.stringify(parsed.inputs)  : null,
        outputs:           parsed.outputs ? JSON.stringify(parsed.outputs) : null,
        config_version_id: parsed.config_version_id,
        evaluated_at:      parsed.evaluated_at.toISOString(),
        evaluator_run_id:  parsed.evaluator_run_id,
        emitted_by:        parsed.emitted_by,
    };

    try {
        await table.insert([row]);
    } catch (err) {
        // BQ surfaces partial errors as a `PartialFailureError` with `errors` on each row.
        // Log with context, then re-throw — caller decides how to handle.
        logger.error({
            err: err?.message || String(err),
            row_errors: err?.errors,
            dataset: datasetId,
            table: DECISION_EVENT_TABLE,
            office_key: parsed.office_key,
            master_account_id: parsed.master_account_id,
            phase: parsed.phase,
            rule_id: parsed.rule_id,
            decision_code: parsed.decision_code,
        }, '[decision-events] insert failed');
        throw err;
    }
}

/**
 * Insert many decision_event rows in chunks of BATCH_CHUNK_SIZE.
 *
 * Caller is responsible for grouping by office_key (each office writes to its
 * own dataset). Mixed-office batches are rejected at the boundary — pass them
 * one office at a time.
 *
 * Per-row validation happens up front so a single bad row fails the whole
 * batch loudly rather than landing partial garbage. Per-chunk insert errors
 * are logged with row-level diagnostics and re-thrown so the caller can
 * choose to swallow them (for fire-and-forget paths like the pricing engine,
 * which prefers a logged warning to a crashed plan-gen run).
 *
 * @param {import('./types.js').EmitArgs[]} events
 * @returns {Promise<void>}
 */
export async function emitDecisionEventsBatch(events) {
    if (!Array.isArray(events) || events.length === 0) return;

    const officeKeys = new Set(events.map((e) => e?.office_key));
    if (officeKeys.size !== 1) {
        throw new Error(
            `emitDecisionEventsBatch requires a single office per call; got ${officeKeys.size} ` +
            `(office_keys: ${[...officeKeys].join(', ')}). Group by office_key on the caller side.`,
        );
    }
    const officeKey = [...officeKeys][0];

    // Validate every row before any insert — fail-fast on a bad row.
    const rows = events.map((args) => {
        const parsed = EmitArgsSchema.parse(args);
        return {
            plan_id:           parsed.plan_id,
            office_key:        parsed.office_key,
            master_account_id: parsed.master_account_id,
            subscription_id:   parsed.subscription_id ?? null,
            phase:             parsed.phase,
            rule_id:           parsed.rule_id,
            decision_code:     parsed.decision_code,
            decision_reason:   parsed.decision_reason ?? null,
            inputs:            parsed.inputs  ? JSON.stringify(parsed.inputs)  : null,
            outputs:           parsed.outputs ? JSON.stringify(parsed.outputs) : null,
            config_version_id: parsed.config_version_id,
            evaluated_at:      parsed.evaluated_at.toISOString(),
            evaluator_run_id:  parsed.evaluator_run_id,
            emitted_by:        parsed.emitted_by,
        };
    });

    const datasetId = `rcp_${officeKey}`;
    const table = bigqueryClient.dataset(datasetId).table(DECISION_EVENT_TABLE);

    for (let i = 0; i < rows.length; i += BATCH_CHUNK_SIZE) {
        const chunk = rows.slice(i, i + BATCH_CHUNK_SIZE);
        try {
            await table.insert(chunk);
        } catch (err) {
            logger.error({
                err: err?.message || String(err),
                row_errors: err?.errors,
                dataset: datasetId,
                table: DECISION_EVENT_TABLE,
                office_key: officeKey,
                chunk_size: chunk.length,
                chunk_offset: i,
                total_rows: rows.length,
            }, '[decision-events] batch insert failed');
            throw err;
        }
    }
}
