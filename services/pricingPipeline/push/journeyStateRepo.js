/**
 * account_journey_state — explicit per-(plan, account) state machine.
 *
 * Every transition writes BOTH the current-row UPDATE and the history INSERT
 * in one transaction via `transition()`. Bypassing this helper to write the
 * tables directly is a bug — code review should reject any direct INSERT/UPDATE
 * against account_journey_state outside this file.
 *
 * State enum (locked, mirrors the migration 115 CHECK):
 *   planned, notified, ready_to_push, pushed, verified,
 *   reverse_requested, reversed, failed, skipped
 */

import { withTransaction } from '../../../lib/postgres.js';

export const JOURNEY_STATES = Object.freeze([
    'planned', 'notified', 'ready_to_push',
    'pushed', 'verified',
    'reverse_requested', 'reversed',
    'failed', 'skipped',
]);

/**
 * Allowed transitions. Forward edges only — reversals go through
 * reverse_requested → reversed, never directly back to planned.
 *
 * Calling transition() with a from→to that isn't in this map throws.
 * Skipping levels (e.g., planned → pushed) is fine; we only enforce that
 * the transition isn't backwards or to a terminal state's ancestor.
 */
const ALLOWED_TRANSITIONS = new Map([
    ['planned',           new Set(['notified', 'ready_to_push', 'pushed', 'failed', 'skipped'])],
    ['notified',          new Set(['ready_to_push', 'pushed', 'failed', 'skipped'])],
    ['ready_to_push',     new Set(['pushed', 'failed', 'skipped'])],
    ['pushed',            new Set(['verified', 'reverse_requested', 'failed'])],
    ['verified',          new Set(['reverse_requested'])],
    ['reverse_requested', new Set(['reversed', 'failed'])],
    ['reversed',          new Set([])],          // terminal
    ['failed',            new Set(['ready_to_push', 'reverse_requested'])],  // operator retry / unstuck
    ['skipped',           new Set(['ready_to_push'])],                       // re-eligibility flipped
]);

/**
 * @typedef {'planned'|'notified'|'ready_to_push'|'pushed'|'verified'|'reverse_requested'|'reversed'|'failed'|'skipped'} JourneyState
 */

/**
 * Idempotently set the initial state for a (plan, account). No-op if a row
 * already exists. Useful at plan-publish time to seed every account at
 * 'planned' without worrying about duplicate inserts.
 *
 * @param {Object} args
 * @param {number} args.planId
 * @param {string} args.masterAccountId
 * @param {JourneyState} [args.state]    defaults 'planned'.
 * @param {string} [args.enteredBy]
 * @param {string} [args.note]
 */
export async function ensureInitialState({
    planId,
    masterAccountId,
    state = 'planned',
    enteredBy = null,
    note = null,
}) {
    if (!JOURNEY_STATES.includes(state)) {
        throw new Error(`Unknown journey state '${state}'`);
    }
    return withTransaction(async (client) => {
        const result = await client.query(
            `INSERT INTO account_journey_state
                (plan_id, master_account_id, state, state_entered_by, state_note)
             VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT (plan_id, master_account_id) DO NOTHING
             RETURNING state`,
            [planId, masterAccountId, state, enteredBy, note],
        );
        if (result.rows.length > 0) {
            await client.query(
                `INSERT INTO account_journey_state_history
                    (plan_id, master_account_id, from_state, to_state, transitioned_by, note)
                 VALUES ($1, $2, NULL, $3, $4, $5)`,
                [planId, masterAccountId, state, enteredBy, note],
            );
        }
        return { inserted: result.rows.length > 0 };
    });
}

/**
 * Transition the state. Reads the current row inside the same transaction
 * (FOR UPDATE) so concurrent transitions serialize. Validates from→to
 * against ALLOWED_TRANSITIONS — invalid edges throw with a clear message
 * rather than silently skipping.
 *
 * No-op if the current state already equals `to` (idempotent retries).
 *
 * @param {Object} args
 * @param {number} args.planId
 * @param {string} args.masterAccountId
 * @param {JourneyState} args.to
 * @param {string} [args.enteredBy]
 * @param {string} [args.note]
 * @param {boolean} [args.allowInsert]   when true and no row exists, inserts
 *                                       a new row at `to` instead of throwing.
 *                                       Useful for paths that don't always seed.
 * @returns {Promise<{ from: JourneyState|null, to: JourneyState, changed: boolean }>}
 */
export async function transition({
    planId,
    masterAccountId,
    to,
    enteredBy = null,
    note = null,
    allowInsert = false,
}) {
    if (!JOURNEY_STATES.includes(to)) {
        throw new Error(`Unknown journey state '${to}'`);
    }
    return withTransaction(async (client) => {
        const current = await client.query(
            `SELECT state FROM account_journey_state
              WHERE plan_id = $1 AND master_account_id = $2
              FOR UPDATE`,
            [planId, masterAccountId],
        );

        if (current.rows.length === 0) {
            if (!allowInsert) {
                throw new Error(
                    `No account_journey_state row for plan ${planId} / ${masterAccountId}. ` +
                    `Seed via ensureInitialState before transitioning, or pass allowInsert: true.`,
                );
            }
            await client.query(
                `INSERT INTO account_journey_state
                    (plan_id, master_account_id, state, state_entered_by, state_note)
                 VALUES ($1, $2, $3, $4, $5)`,
                [planId, masterAccountId, to, enteredBy, note],
            );
            await client.query(
                `INSERT INTO account_journey_state_history
                    (plan_id, master_account_id, from_state, to_state, transitioned_by, note)
                 VALUES ($1, $2, NULL, $3, $4, $5)`,
                [planId, masterAccountId, to, enteredBy, note],
            );
            return { from: null, to, changed: true };
        }

        const from = current.rows[0].state;
        if (from === to) {
            return { from, to, changed: false };
        }
        const allowed = ALLOWED_TRANSITIONS.get(from);
        if (!allowed || !allowed.has(to)) {
            throw new Error(
                `Invalid journey-state transition for plan ${planId}/${masterAccountId}: ` +
                `${from} → ${to}. Allowed from ${from}: [${[...(allowed ?? [])].join(', ')}].`,
            );
        }

        await client.query(
            `UPDATE account_journey_state SET
                state            = $3,
                state_entered_at = NOW(),
                state_entered_by = $4,
                state_note       = $5
              WHERE plan_id = $1 AND master_account_id = $2`,
            [planId, masterAccountId, to, enteredBy, note],
        );
        await client.query(
            `INSERT INTO account_journey_state_history
                (plan_id, master_account_id, from_state, to_state, transitioned_by, note)
             VALUES ($1, $2, $3, $4, $5, $6)`,
            [planId, masterAccountId, from, to, enteredBy, note],
        );
        return { from, to, changed: true };
    });
}

/**
 * Convenience: read the current state for a (plan, account). Returns null
 * if no row exists.
 *
 * @param {number} planId
 * @param {string} masterAccountId
 * @returns {Promise<JourneyState|null>}
 */
export async function getCurrentState(planId, masterAccountId) {
    const { query: pgQuery } = await import('../../../lib/postgres.js');
    const result = await pgQuery(
        `SELECT state FROM account_journey_state
          WHERE plan_id = $1 AND master_account_id = $2`,
        [planId, masterAccountId],
    );
    return result.rows[0]?.state ?? null;
}
