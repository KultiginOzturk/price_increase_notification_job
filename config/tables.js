/**
 * Central table constants for RevCostPipeline (RCP)
 * 
 * All routes should import table names from here.
 * This prevents typos and makes future dataset changes easy.
 * 
 * Naming: camelCase keys matching the table name without prefix
 * Example: rcp_account_ttm -> accountTtm
 */

// Dataset configuration
// NOTE: BQ_PROJECT env var is REQUIRED - no fallback to prevent cross-project accidents
const BQ_PROJECT = process.env.BQ_PROJECT;
if (!BQ_PROJECT) {
    throw new Error('BQ_PROJECT environment variable is required. Set it in .env (e.g., BQ_PROJECT=pco-prod)');
}
const CFG_DATASET = `${BQ_PROJECT}.DATA_CONFIGURATIONS`;
const INP_DATASET = `${BQ_PROJECT}.APP_INPUTS`;


// NOTE: Per-client datasets (rcp_${CLIENT}) are the primary architecture as of 2026-01
// Legacy shared dataset removed - all tables now use getRCP(client) helper

/**
 * Per-client dataset helper (NEW 2026-01 architecture)
 * Each client has their own dataset: rcp_${CLIENT}
 * 
 * @param {string} tableName - Table name without prefix
 * @param {string} client - Client identifier (e.g., 'GREENCOUNTRY')
 * @returns {string} Fully qualified table reference
 */
const rcp = (tableName, client) => {
    if (!client) {
        throw new Error(`Client required for RCP table: ${tableName}. Pass client to the rcp() helper.`);
    }
    return `\`${BQ_PROJECT}.rcp_${client}.${tableName}\``;
};

// Shared/config table helpers (no client needed)
const cfg = (tableName) => `\`${CFG_DATASET}.${tableName}\``;
const inp = (tableName) => `\`${INP_DATASET}.${tableName}\``;

/**
 * RCP Tables - RevCostPipeline (Per-Client Datasets)
 * 
 * NEW 2026-01 Architecture: Each client has their own dataset.
 * Usage: 
 *   const tables = getRCP(client);
 *   const query = `SELECT * FROM ${tables.accountTtm}`;
 * 
 * @param {string} client - Client identifier (e.g., 'GREENCOUNTRY')
 * @returns {Object} Object with all RCP table references for this client
 */
export const getRCP = (client) => {
    if (!client) {
        throw new Error('Client required for RCP tables. Pass client to getRCP().');
    }

    return {
        // ============================================
        // 00_base - Foundation tables
        // ============================================
        configTtmWindow: rcp('rcp_config_ttm_window', client),
        configValidClients: rcp('rcp_config_valid_clients', client),
        lkpServiceType: rcp('rcp_lkp_service_type', client),
        customerMaster: rcp('rcp_customer_master', client),
        customerAccountBridge: rcp('rcp_customer_account_bridge', client),
        accountMaster: rcp('rcp_account_master', client),
        subscriptionMaster: rcp('rcp_subscription_master', client),
        appointmentEnriched: rcp('rcp_appointment_enriched', client),
        ticketEnriched: rcp('rcp_ticket_enriched', client),
        masterAccountRegistry: rcp('rcp_master_account_registry', client),

        // ============================================
        // 01_drive - Drive time & hours
        // ============================================
        techDayTimeline: rcp('rcp_tech_day_timeline', client),
        visitDriveAttribution: rcp('rcp_visit_drive_attribution', client),
        customerDriveHistory: rcp('rcp_customer_drive_history', client),
        clientDriveAvg: rcp('rcp_client_drive_avg', client),
        customerNearestNeighbors: rcp('rcp_customer_nearest_neighbors', client),
        customerDriveForecast: rcp('rcp_customer_drive_forecast', client),
        clientOneTimeDriveAvg: rcp('rcp_client_one_time_drive_avg', client),
        clientHoursSummary: rcp('rcp_client_hours_summary', client),
        customerReserviceRate: rcp('rcp_customer_reservice_rate', client),
        onsiteRatioHierarchy: rcp('rcp_onsite_ratio_hierarchy', client),
        serviceTypeStackingStats: rcp('rcp_service_type_stacking_stats', client),
        customerStackingStats: rcp('rcp_customer_stacking_stats', client),

        // ============================================
        // 02_ttm - Trailing Twelve Months
        // ============================================
        subscriptionRevenueTtm: rcp('rcp_subscription_revenue_ttm', client),
        subscriptionHoursTtm: rcp('rcp_subscription_hours_ttm', client),
        clientTtmCogs: rcp('rcp_client_ttm_cogs', client),
        subscriptionTtm: rcp('rcp_subscription_ttm', client),
        customerTtm: rcp('rcp_customer_ttm', client),
        accountTtm: rcp('rcp_account_ttm', client),
        clientTtmSummary: rcp('rcp_client_ttm_summary', client),

        // ============================================
        // 03_ntm - Next Twelve Months (Forecast)
        // ============================================
        subscriptionForecast: rcp('rcp_subscription_forecast', client),
        accountNtmForecast: rcp('rcp_account_ntm_forecast', client),

        // ============================================
        // 04_account_comparison - Peer benchmarks
        // ============================================
        clientPeerStats: rcp('rcp_client_peer_stats', client),
        serviceTypePeerStats: rcp('rcp_service_type_peer_stats', client),
        accountPeerComparison: rcp('rcp_account_peer_comparison', client),
        accountMarginSegment: rcp('rcp_account_margin_segment', client),

        // ============================================
        // 05_validation - Driver trees
        // ============================================
        clientDriverTree: rcp('rcp_client_driver_tree', client),
        clientDriverTreeClean: rcp('rcp_client_driver_tree_clean', client),
        clientDriverTreeLegacy: rcp('rcp_client_driver_tree_legacy', client),
        clientDriverTreeCleanLegacy: rcp('rcp_client_driver_tree_clean_legacy', client),

        // ============================================
        // 06_flagged - Client review flags
        // ============================================
        flagLowDeliverySubscriptions: rcp('rcp_flag_low_delivery_subscriptions', client),
        flagHighReserviceCustomers: rcp('rcp_flag_high_reservice_customers', client),
        pipelineFailures: rcp('rcp_pipeline_failures', client),

        // ============================================
        // Service Cost Model (migrated from bq-ratecard to main pipeline)
        rcServiceCostModel: rcp('rcp_service_cost_model', client),      // Phase 8: visits, cost, reservice per service type
    };
};

// LEGACY: For backward compatibility during migration
// This will throw if used without being replaced with getRCP()
export const RCP = new Proxy({}, {
    get: (target, prop) => {
        throw new Error(
            `RCP.${prop} is deprecated. Use getRCP(client).${prop} instead. ` +
            `Per-client datasets require specifying the client.`
        );
    }
});


/**
 * CONFIG Tables - DATA_CONFIGURATIONS (Source of Truth)
 * 
 * NOTE: cfg_master_account_map is a VIEW used by RCP pipeline SQL.
 * API routes that need finalized master account mappings should query
 * cfg_master_account_map_finalized directly (not via this constant).
 */
export const CONFIG = {
    clientRegistry: cfg('cfg_client_registry'),                 // Client list and configuration
    serviceTypeDefinitions: cfg('cfg_service_type_definitions'),
    customerLinearFootage: cfg('cfg_customer_linear_footage'),
    masterAccountMap: cfg('cfg_master_account_map'),  // VIEW - used by pipeline SQL
    masterAccountMapFinalized: cfg('cfg_master_account_map_finalized'),  // Finalized customer-to-group mapping
    masterAccountSettings: cfg('cfg_master_account_settings'),
    reportingPeriods: cfg('cfg_reporting_periods'),
    financialActuals: cfg('cfg_financial_actuals'),
    tagDefinitions: cfg('cfg_tag_definitions'),
    clientBusinessSettings: cfg('cfg_client_business_settings'),
    termiteRiskSettings: cfg('cfg_termite_risk_settings'),  // Termite warranty risk config
    repricingPlans: cfg('cfg_repricing_plans'),             // Repricing plan config (SCD2)
    clientCeilingSettings: cfg('cfg_client_ceiling_settings'),  // Price ceiling configurations
    bundleReferenceVisits: cfg('cfg_bundle_reference_visits'),  // Bundle visit configurations
    rateCardPackageDefinitions: cfg('cfg_rate_card_package_definitions'), // Package include/exclude + discount overrides (SCD2)
    rateCardPrices: cfg('cfg_rate_card_prices'),                // Client rate card prices (SCD2)
    competitiveQuotes: cfg('cfg_competitive_quotes'),            // Mystery-shopping competitive quotes
};

/**
 * INPUTS Tables - APP_INPUTS (User Decisions)
 */
export const INPUTS = {
    accountTags: inp('v_active_account_tags'),              // Live Cloud SQL-backed bridge view
    effectiveReviewStyle: inp('v_account_effective_review_style'),  // Resolved review style per account (tag + attribute rules)
    clientSettings: inp('inp_client_settings'),
    marginAdjustmentRules: inp('inp_margin_adjustment_rules'),  // Legacy shell table
    marginAdjustments: inp('inp_margin_adjustments'),           // NEW: Direct Margin adjustments
    accountDecisions: inp('inp_account_decisions'),
    missionSelections: inp('inp_mission_selections'),
    // Note: v_account_review_routing was removed - replaced by direct query on per-client tables

    // Repricing persistence tables
    accountRepricingHistory: inp('inp_account_repricing_history'),
    pricePushQueue: inp('inp_price_push_queue'),
    pricePushSubscriptionDetail: inp('inp_price_push_subscription_detail'),  // Subscription-level price push details
    pricePushReady: inp('v_price_push_ready'),                                // View for Python price-push-api
    repricingExecutionLog: inp('inp_repricing_execution_log'),

    // Email notification tables
    emailUnsubscribes: inp('cfg_email_unsubscribes'),                        // Customer email unsubscribe preferences
    priceIncreaseNotifications: inp('inp_price_increase_notifications'),      // Notification audit log
    priceIncreaseNotificationEvents: inp('inp_price_increase_notification_events'), // Unified pre/post notification audit log
};

/**
 * SERVICES Tables - 12_service_statistics (Ceiling & Bundle Logic)
 * NOTE: These require a client to be passed in as well (per-client datasets)
 */
export const getSERVICES = (client) => {
    if (!client) {
        throw new Error('Client required for SERVICES tables.');
    }
    return {
        accountCeilingStatus: rcp('svc_account_ceiling_status', client),
        accountEffectiveBundle: rcp('svc_account_effective_bundle', client),
        competitorQuotes: rcp('svc_competitor_quotes', client),
        competitorQuoteSummary: rcp('svc_competitor_quote_summary', client),
    };
};

// LEGACY export for backward compatibility
export const SERVICES = new Proxy({}, {
    get: (target, prop) => {
        throw new Error(`SERVICES.${prop} is deprecated. Use getSERVICES(client).${prop} instead.`);
    }
});

/**
 * Shared layers (used for curation_layer - these are shared across clients)
 */
export const SHARED = {
    // Curation layer (shared across all clients)
    curCustomer: `\`${BQ_PROJECT}.curation_layer.cur_customer\``,
    curSubscription: `\`${BQ_PROJECT}.curation_layer.cur_subscription\``,
    curAppointment: `\`${BQ_PROJECT}.curation_layer.cur_appointment\``,
    curServiceType: `\`${BQ_PROJECT}.curation_layer.cur_service_type\``,
    curTicket: `\`${BQ_PROJECT}.curation_layer.cur_ticket\``,
    curEmployee: `\`${BQ_PROJECT}.curation_layer.cur_employee\``,
    curCustomerFlag: `\`${BQ_PROJECT}.curation_layer.cur_customer_flag\``,

    // Normalization layer (shared across all clients)
    normSubscriptionFieldroutes: `\`${BQ_PROJECT}.normalization_layer.norm_subscription_fieldroutes\``,
    normCustomerFieldroutes: `\`${BQ_PROJECT}.normalization_layer.norm_customer_fieldroutes\``,
    normAppointmentFieldroutes: `\`${BQ_PROJECT}.normalization_layer.norm_appointment_fieldroutes\``,

    // Raw layer (shared across all clients)
    rawSubscription: `\`${BQ_PROJECT}.raw_layer.FR_SUBSCRIPTION\``,
    rawCustomer: `\`${BQ_PROJECT}.raw_layer.FR_CUSTOMER\``,
    rawAppointment: `\`${BQ_PROJECT}.raw_layer.FR_APPOINTMENT\``,
    rawAppliedPayment: `\`${BQ_PROJECT}.raw_layer.FR_APPLIED_PAYMENT\``,
    rawTicket: `\`${BQ_PROJECT}.raw_layer.FR_TICKET\``,
    rawEmployee: `\`${BQ_PROJECT}.raw_layer.FR_EMPLOYEE\``,
    rawServiceType: `\`${BQ_PROJECT}.raw_layer.FR_SERVICE_TYPE\``,

    // Config tables (shared)
    lkpMasterAccountMap: `\`${CFG_DATASET}.cfg_master_account_map\``,
    lkpCogsHours: `\`${CFG_DATASET}.cfg_financial_actuals\``,
};

// Export project for direct access if needed
export const PROJECT = BQ_PROJECT;

export default { getRCP, RCP, CONFIG, INPUTS, getSERVICES, SERVICES, SHARED, PROJECT };
