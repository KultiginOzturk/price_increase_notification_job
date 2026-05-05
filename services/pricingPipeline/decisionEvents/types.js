/**
 * Decision-event types and validators.
 *
 * Phases are a fixed enum across the entire pipeline. Every decision_event
 * row carries a phase + rule_id + decision_code, with phase being the coarsest
 * grouping used by the trace UI to organize the per-account view.
 *
 * See plans/active/20260504_pricing_pipeline_refactor/06_decision_log.md.
 */

import { z } from 'zod';

/** All pipeline phases, ordered roughly by execution time. */
export const PHASES = Object.freeze([
    'data_foundation',
    'eligibility',
    'pricing',
    'scheduling',
    'notification',
    'push',
    'post_push',
    'reversal',
]);

export const PhaseSchema = z.enum(PHASES);

/**
 * Names of services that emit events. Used in `decision_event.emitted_by` for
 * traceability — helps debug coverage gaps ("which wrapper failed to fire?").
 */
export const EMITTED_BY = Object.freeze({
    ELIGIBILITY_EVALUATOR:  'eligibility_evaluator',
    PRICING_ENGINE:         'pricing_engine',
    SCHEDULING_ENGINE:      'scheduling_engine',
    FOUNDATION_SYNTHESIZER: 'foundation_synthesizer',
    NOTIFICATION_SERVICE:   'notification_service',
    PUSH_RUNNER:            'push_runner',
    POST_PUSH_VALIDATOR:    'post_push_validator',
    REVERSAL_RUNNER:        'reversal_runner',
});

/** Zod schema for `emitDecisionEvent()` arguments. */
export const EmitArgsSchema = z.object({
    plan_id:           z.number().int().nullable(),
    office_key:        z.string().min(1),
    master_account_id: z.string().min(1),
    subscription_id:   z.string().nullable().optional(),
    phase:             PhaseSchema,
    rule_id:           z.string().min(1),
    decision_code:     z.string().min(1),
    decision_reason:   z.string().nullable().optional(),
    inputs:            z.record(z.unknown()).nullable().optional(),
    outputs:           z.record(z.unknown()).nullable().optional(),
    config_version_id: z.number().int(),
    evaluated_at:      z.date(),
    evaluator_run_id:  z.string().min(1),
    emitted_by:        z.string().min(1),
});

/** @typedef {z.infer<typeof EmitArgsSchema>} EmitArgs */
