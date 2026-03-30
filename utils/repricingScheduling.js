const MONTH_ABBREVIATIONS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
const MONTH_FULL_NAMES = [
    'January',
    'February',
    'March',
    'April',
    'May',
    'June',
    'July',
    'August',
    'September',
    'October',
    'November',
    'December'
];

const MONTH_NAME_LOOKUP = new Map(
    MONTH_ABBREVIATIONS.flatMap((abbr, idx) => ([
        [abbr.toLowerCase(), idx + 1],
        [MONTH_FULL_NAMES[idx].toLowerCase(), idx + 1]
    ]))
);

export const DEFAULT_SCHEDULING_MONTHS = [...MONTH_ABBREVIATIONS];

const padNumber = (value) => String(value).padStart(2, '0');

const parseDateOnly = (value) => {
    if (!value) return null;
    if (value instanceof Date) return Number.isNaN(value.getTime()) ? null : value;

    const text = String(value).trim();
    if (!text) return null;

    const dateOnlyMatch = /^(\d{4})-(\d{2})-(\d{2})$/.exec(text);
    if (dateOnlyMatch) {
        const [, year, month, day] = dateOnlyMatch;
        return new Date(Number(year), Number(month) - 1, Number(day));
    }

    const parsed = new Date(text);
    return Number.isNaN(parsed.getTime()) ? null : parsed;
};

export const formatDateOnly = (value) => {
    const date = parseDateOnly(value);
    if (!date) return null;
    return `${date.getFullYear()}-${padNumber(date.getMonth() + 1)}-${padNumber(date.getDate())}`;
};

export const normalizeRenewalDate = (value) => {
    const formatted = formatDateOnly(value);
    if (!formatted) return null;

    // Some source systems emit epoch/sentinel renewal dates for "no renewal".
    return formatted <= '1970-01-01' ? null : formatted;
};

export const normalizeSchedulingMonth = (value) => {
    if (value === null || value === undefined || value === '') return null;

    if (typeof value === 'number' && Number.isFinite(value)) {
        return value >= 1 && value <= 12 ? Math.trunc(value) : null;
    }

    const raw = String(value).trim();
    if (!raw) return null;

    const numeric = Number(raw);
    if (Number.isFinite(numeric) && numeric >= 1 && numeric <= 12) {
        return Math.trunc(numeric);
    }

    const normalized = raw.toLowerCase();
    if (MONTH_NAME_LOOKUP.has(normalized)) {
        return MONTH_NAME_LOOKUP.get(normalized);
    }

    return null;
};

export const getMonthAbbreviation = (monthNumber) => {
    const month = Number(monthNumber);
    if (!Number.isFinite(month) || month < 1 || month > 12) return null;
    return MONTH_ABBREVIATIONS[month - 1];
};

export const getMonthFullName = (monthNumber) => {
    const month = Number(monthNumber);
    if (!Number.isFinite(month) || month < 1 || month > 12) return null;
    return MONTH_FULL_NAMES[month - 1];
};

export const getMonthEndDate = (planYear, monthNumber) => {
    const year = Number(planYear);
    const month = Number(monthNumber);
    if (!Number.isFinite(year) || !Number.isFinite(month) || month < 1 || month > 12) {
        return null;
    }

    // Day 0 of next month gives the last day of the target month.
    return new Date(year, month, 0);
};

export const formatYearMonth = (value) => {
    const date = parseDateOnly(value);
    if (!date) return null;
    return `${date.getFullYear()}-${padNumber(date.getMonth() + 1)}`;
};

export const isEligibleForMonth = (eligibleAfter, planYear, monthNumber) => {
    const eligibleDate = parseDateOnly(eligibleAfter);
    if (!eligibleDate) return true;

    const monthEnd = getMonthEndDate(planYear, monthNumber);
    if (!monthEnd) return false;

    return eligibleDate.getTime() <= monthEnd.getTime();
};

export const deriveEarliestSchedulableMonth = (eligibleAfter, isRenewalBased = false) => {
    const eligibleDate = parseDateOnly(eligibleAfter);
    if (!eligibleDate) return null;

    const schedulableMonthDate = isRenewalBased
        ? new Date(eligibleDate.getFullYear(), eligibleDate.getMonth() - 1, 1)
        : new Date(eligibleDate.getFullYear(), eligibleDate.getMonth(), 1);

    return formatYearMonth(schedulableMonthDate);
};

export const isSchedulableForMonth = (earliestSchedulableMonth, planYear, monthNumber) => {
    if (!earliestSchedulableMonth) return true;

    const year = Number(planYear);
    const month = Number(monthNumber);
    if (!Number.isFinite(year) || !Number.isFinite(month) || month < 1 || month > 12) {
        return false;
    }

    const targetMonth = `${year}-${padNumber(month)}`;
    return String(earliestSchedulableMonth) <= targetMonth;
};

export const isEligibleWithinPlanYear = (eligibleAfter, planYear) => {
    const eligibleDate = parseDateOnly(eligibleAfter);
    if (!eligibleDate) return true;

    const yearEnd = getMonthEndDate(planYear, 12);
    if (!yearEnd) return false;

    return eligibleDate.getTime() <= yearEnd.getTime();
};

const addMonths = (dateValue, monthsToAdd) => {
    const baseDate = parseDateOnly(dateValue);
    if (!baseDate) return null;

    const next = new Date(baseDate.getFullYear(), baseDate.getMonth() + Number(monthsToAdd || 0), baseDate.getDate());
    return formatDateOnly(next);
};

export const deriveEligibleAfter = ({
    eligibilityStatus,
    earliestSubscriptionDate,
    mostRecentProductChangeDate,
    newAccountsProtectionMonths,
    productChangeProtectionMonths,
}) => {
    switch (eligibilityStatus) {
        case 'insufficient_tenure':
            return addMonths(earliestSubscriptionDate, newAccountsProtectionMonths || 0);
        case 'recent_product_change':
            return addMonths(mostRecentProductChangeDate, productChangeProtectionMonths || 0);
        default:
            return null;
    }
};

export const humanizeNonEligibleReason = (value) => {
    const reason = String(value || '').trim().toLowerCase();
    if (!reason) return null;

    switch (reason) {
        case 'never':
            return 'Never';
        case 'defer':
            return 'Defer';
        case 'insufficient_tenure':
            return 'New Customer';
        case 'recent_product_change':
            return 'Recent Service Change';
        case 'no_recurring_subscriptions':
            return 'No Recurring Subscriptions';
        case 'no_forecast':
            return 'No Forecast';
        default:
            return value;
    }
};

export const deriveReviewStyle = ({ isManualReview, manualReviewType }) => {
    if (!isManualReview) return 'Normal';
    if (String(manualReviewType || '').toUpperCase() === 'VIP') return 'VIP';
    return 'Manual';
};
