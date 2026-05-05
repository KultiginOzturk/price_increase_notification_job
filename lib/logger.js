/**
 * Minimal pino-shape logger shim for the standalone notification job.
 *
 * The main client-ops-pilot repo uses pino. The shared modules copied
 * over here (decisionEvents/emit, pricingPipeline/notification/*) call
 * `logger.error(ctxObj, msg)`-style. This shim implements that surface
 * over console.* without pulling pino in as a dep.
 *
 * If we ever do pull pino into this repo, drop this file and import
 * the real one — every call site already uses the pino-style 2-arg
 * (object, message) shape.
 */

function emit(level, ctxOrMsg, msg) {
    const isObj = ctxOrMsg !== null && typeof ctxOrMsg === 'object';
    const message = msg ?? (isObj ? '' : String(ctxOrMsg ?? ''));
    const ctx = isObj ? ctxOrMsg : null;
    const stream = level === 'error' ? console.error
        : level === 'warn' ? console.warn
        : level === 'debug' ? console.debug
        : console.log;
    if (ctx) stream(`[${level}] ${message}`, ctx);
    else stream(`[${level}] ${message}`);
}

const logger = {
    trace: (a, m) => emit('trace', a, m),
    debug: (a, m) => emit('debug', a, m),
    info:  (a, m) => emit('info',  a, m),
    warn:  (a, m) => emit('warn',  a, m),
    error: (a, m) => emit('error', a, m),
    fatal: (a, m) => emit('error', a, m),
};

export default logger;
