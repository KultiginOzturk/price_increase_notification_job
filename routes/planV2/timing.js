const MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

const MONTH_TO_INDEX = new Map(MONTHS.map((month, index) => [month, index]));
const PERIOD_RE = /^\d{4}-(0[1-9]|1[0-2])$/;

export function getMonthIndex(month) {
  if (!month) return null;
  return MONTH_TO_INDEX.has(month) ? MONTH_TO_INDEX.get(month) : null;
}

export function getMonthNumber(month) {
  const index = getMonthIndex(month);
  return index == null ? null : index + 1;
}

export function isEffectivePeriod(value) {
  if (!value) return false;
  return PERIOD_RE.test(String(value).trim());
}

export function normalizeEffectivePeriod(value) {
  if (!isEffectivePeriod(value)) return null;
  return String(value).trim();
}

export function getMonthFromEffectivePeriod(period) {
  const normalized = normalizeEffectivePeriod(period);
  if (!normalized) return null;
  return MONTHS[Number(normalized.slice(5, 7)) - 1] || null;
}

export function getEffectiveMonthNumberFromPeriod(period) {
  const normalized = normalizeEffectivePeriod(period);
  if (!normalized) return null;
  return Number(normalized.slice(5, 7));
}

export function getEffectiveYearFromPeriod(period) {
  const normalized = normalizeEffectivePeriod(period);
  if (!normalized) return null;
  return Number(normalized.slice(0, 4));
}

export function getReferenceYear(referenceDateInput = null) {
  const referenceDate = referenceDateInput ? new Date(referenceDateInput) : new Date();
  return referenceDate.getFullYear();
}

export function getEffectiveYear(month, referenceDateInput = null) {
  const monthIndex = getMonthIndex(month);
  if (monthIndex == null) return null;

  const referenceDate = referenceDateInput ? new Date(referenceDateInput) : new Date();
  const referenceYear = referenceDate.getFullYear();
  const referenceMonth = referenceDate.getMonth();

  // Treat the effective month as the next occurrence of that calendar month
  // relative to the plan reference date. This keeps late-year planning cycles
  // from deriving past dates for Jan/Feb rollouts.
  return monthIndex < referenceMonth ? referenceYear + 1 : referenceYear;
}

export function deriveTiming(month, referenceDateInput = null) {
  if (isEffectivePeriod(month)) return deriveTimingFromPeriod(month);

  const monthNumber = getMonthNumber(month);
  if (monthNumber == null) {
    return {
      effectivePeriod: null,
      effectiveMonth: month,
      effectiveMonthNumber: null,
      effectiveDate: null,
      noticeDate: null,
      reviewDeadline: null,
      rolloutYear: null,
    };
  }

  const year = getEffectiveYear(month, referenceDateInput);
  const effectiveMonthStart = new Date(Date.UTC(year, monthNumber - 1, 1));
  // Business rule: the rollout period names the target month cohort, but the
  // actual effective date is the day before day 1 of that month. Example:
  // a June rollout takes effect on May 31, with notice/review deadlines
  // derived backward from that date.
  const effectiveDate = new Date(Date.UTC(year, monthNumber - 1, 0));
  const noticeDate = new Date(effectiveDate);
  noticeDate.setUTCDate(noticeDate.getUTCDate() - 30);
  const reviewDeadline = new Date(noticeDate);
  reviewDeadline.setUTCDate(reviewDeadline.getUTCDate() - 1);

  return {
    effectivePeriod: `${year}-${String(monthNumber).padStart(2, '0')}`,
    effectiveMonth: month,
    effectiveMonthNumber: monthNumber,
    effectiveDate: effectiveDate.toISOString().slice(0, 10),
    noticeDate: noticeDate.toISOString().slice(0, 10),
    reviewDeadline: reviewDeadline.toISOString().slice(0, 10),
    rolloutYear: year,
  };
}

export function getEffectiveDate(month, referenceDateInput = null) {
  if (isEffectivePeriod(month)) return getEffectiveDateFromPeriod(month);

  const monthNumber = getMonthNumber(month);
  if (monthNumber == null) return null;
  const year = getEffectiveYear(month, referenceDateInput);
  const date = new Date(Date.UTC(year, monthNumber - 1, 0));
  return date.toISOString().slice(0, 10);
}

export function deriveTimingFromPeriod(period) {
  const normalized = normalizeEffectivePeriod(period);
  if (!normalized) {
    return {
      effectivePeriod: null,
      effectiveMonth: null,
      effectiveMonthNumber: null,
      effectiveDate: null,
      noticeDate: null,
      reviewDeadline: null,
      rolloutYear: null,
    };
  }

  const year = getEffectiveYearFromPeriod(normalized);
  const monthNumber = getEffectiveMonthNumberFromPeriod(normalized);
  const effectiveDate = new Date(Date.UTC(year, monthNumber - 1, 0));
  const noticeDate = new Date(effectiveDate);
  noticeDate.setUTCDate(noticeDate.getUTCDate() - 30);
  const reviewDeadline = new Date(noticeDate);
  reviewDeadline.setUTCDate(reviewDeadline.getUTCDate() - 1);

  return {
    effectivePeriod: normalized,
    effectiveMonth: getMonthFromEffectivePeriod(normalized),
    effectiveMonthNumber: monthNumber,
    effectiveDate: effectiveDate.toISOString().slice(0, 10),
    noticeDate: noticeDate.toISOString().slice(0, 10),
    reviewDeadline: reviewDeadline.toISOString().slice(0, 10),
    rolloutYear: year,
  };
}

export function getEffectiveDateFromPeriod(period) {
  const normalized = normalizeEffectivePeriod(period);
  if (!normalized) return null;
  const year = getEffectiveYearFromPeriod(normalized);
  const monthNumber = getEffectiveMonthNumberFromPeriod(normalized);
  const date = new Date(Date.UTC(year, monthNumber - 1, 0));
  return date.toISOString().slice(0, 10);
}

/** Clients where scheduling compares eligible_after to the **end of the cohort month** (YYYY-MM). Pilot: MODERN. */
const SCHEDULING_COHORT_MONTH_END_CLIENTS = new Set(['MODERN']);

/**
 * Last calendar day of the month named in YYYY-MM (e.g. 2026-05 → 2026-05-31).
 * Scheduling-only: first assignable cohort is the named month that contains eligible_after.
 */
export function getCohortMonthEndDateFromPeriod(period) {
  const normalized = normalizeEffectivePeriod(period);
  if (!normalized) return null;
  const year = getEffectiveYearFromPeriod(normalized);
  const monthNumber = getEffectiveMonthNumberFromPeriod(normalized);
  if (!year || !monthNumber) return null;
  const date = new Date(Date.UTC(year, monthNumber, 0));
  return date.toISOString().slice(0, 10);
}

/**
 * @param {string|null|undefined} eligibleAfter - YYYY-MM-DD
 * @param {string} effectivePeriod - YYYY-MM
 * @param {string|null|undefined} [client] - company key; MODERN uses cohort month-end, others use prior month-end (effectiveDate)
 */
export function isEligibleForEffectivePeriod(eligibleAfter, effectivePeriod, client) {
  if (!eligibleAfter) return true;
  const key = client != null ? String(client).toUpperCase() : '';
  const useCohortMonthEnd = SCHEDULING_COHORT_MONTH_END_CLIENTS.has(key);
  const effectiveDate = useCohortMonthEnd
    ? getCohortMonthEndDateFromPeriod(effectivePeriod)
    : getEffectiveDateFromPeriod(effectivePeriod);
  if (!effectiveDate) return false;
  return String(eligibleAfter) <= effectiveDate;
}

export function sortMonths(months) {
  return [...months].sort((left, right) => (getMonthNumber(left) ?? 99) - (getMonthNumber(right) ?? 99));
}

export function sortMonthCohorts(rows, referenceDateInput = null) {
  return [...rows].sort((left, right) => {
    const leftTiming = deriveTiming(
      left?.effective_period ?? null,
      referenceDateInput
    );
    const rightTiming = deriveTiming(
      right?.effective_period ?? null,
      referenceDateInput
    );

    if (leftTiming.reviewDeadline && rightTiming.reviewDeadline) {
      return leftTiming.reviewDeadline.localeCompare(rightTiming.reviewDeadline);
    }
    if (leftTiming.effectiveDate && rightTiming.effectiveDate) {
      return leftTiming.effectiveDate.localeCompare(rightTiming.effectiveDate);
    }
    return (leftTiming.effectiveMonthNumber ?? 99) - (rightTiming.effectiveMonthNumber ?? 99);
  });
}
