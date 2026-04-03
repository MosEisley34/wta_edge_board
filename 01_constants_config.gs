const SHEETS = {
  CONFIG: 'Config',
  RUN_LOG: 'Run_Log',
  RAW_ODDS: 'Raw_Odds',
  RAW_SCHEDULE: 'Raw_Schedule',
  RAW_PLAYER_STATS: 'Raw_Player_Stats',
  MATCH_MAP: 'Match_Map',
  SIGNALS: 'Signals',
  STATE: 'State',
  PROVIDER_HEALTH: 'ProviderHealth',
};

const DEFAULT_CONFIG = {
  RUN_ENABLED: 'true',
  LOOKAHEAD_HOURS: '36',
  ODDS_API_BASE_URL: 'https://api.the-odds-api.com/v4',
  ODDS_API_KEY: '',
  ODDS_SPORT_KEY: '',
  ODDS_MARKETS: 'h2h',
  ODDS_REGIONS: 'us',
  ODDS_ODDS_FORMAT: 'american',
  ODDS_CACHE_TTL_SEC: '300',
  ODDS_WINDOW_CACHE_TTL_MIN: '5',
  ODDS_WINDOW_REFRESH_MIN: '5',
  ODDS_WINDOW_FORCE_REFRESH: 'false',
  ODDS_API_MAX_RETRIES: '3',
  ODDS_API_BACKOFF_BASE_MS: '250',
  ODDS_API_BACKOFF_MAX_MS: '3000',
  ODDS_WINDOW_PRE_FIRST_MIN: '120',
  ODDS_WINDOW_POST_LAST_MIN: '180',
  ODDS_REFRESH_TIER_LOW_UPPER_MIN: '240',
  ODDS_REFRESH_TIER_MED_UPPER_MIN: '180',
  ODDS_REFRESH_TIER_HIGH_UPPER_MIN: '90',
  ODDS_REFRESH_TIER_LOW_INTERVAL_MIN: '20',
  ODDS_REFRESH_TIER_MED_INTERVAL_MIN: '10',
  ODDS_REFRESH_TIER_HIGH_INTERVAL_MIN: '5',
  ODDS_NO_GAMES_BEHAVIOR: 'SKIP',
  ODDS_BOOTSTRAP_LOOKAHEAD_HOURS: '18',
  BOOTSTRAP_EMPTY_CYCLE_THRESHOLD: '3',
  EMPTY_PRODUCTIVE_OUTPUT_THRESHOLD: '3',
  SCHEDULE_ONLY_STREAK_NOTICE_THRESHOLD: '3',
  PRODUCTIVE_OUTPUT_MITIGATION_ENABLED: 'false',
  PRODUCTIVE_OUTPUT_MITIGATION_OPENING_LAG_WIDEN_ENABLED: 'false',
  PRODUCTIVE_OUTPUT_MITIGATION_OPENING_LAG_EXTRA_MINUTES: '30',
  PRODUCTIVE_OUTPUT_MITIGATION_FORCE_FRESH_ODDS_PROBE_ENABLED: 'false',
  PRODUCTIVE_OUTPUT_MITIGATION_VERBOSE_DIAGNOSTICS_ENABLED: 'false',
  ROLLUP_EVERY_N_RUNS: '10',
  SCHEDULE_BUFFER_BEFORE_MIN: '180',
  SCHEDULE_BUFFER_AFTER_MIN: '180',
  MATCH_TIME_TOLERANCE_MIN: '45',
  MATCH_FALLBACK_EXPANSION_MIN: '120',
  MATCH_FALLBACK_HARD_MAX_DELTA_MIN: '1440',
  MATCH_NEAREST_CANDIDATE_MIN_SIMILARITY: '0.55',
  MATCHER_PLAYER_IDENTITY_MISSING_RATE_BLOCK_THRESHOLD: '0.6',
  MATCHER_PLAYER_IDENTITY_MISSING_MIN_ROWS: '3',
  ALLOW_WTA_125: 'true',
  ALLOW_WTA_250: 'true',
  COMPETITION_SOURCE_FIELDS_JSON: '["competition","tournament","event_name","sport_title","home_team","away_team"]',
  GRAND_SLAM_ALIASES_JSON: '["australian open","roland garros","french open","wimbledon","us open"]',
  WTA_1000_ALIASES_JSON: '["wta 1000","wta-1000","masters 1000","wta indian wells","indian wells"]',
  WTA_500_ALIASES_JSON: '["wta 500","wta-500"]',
  COMPETITION_DENY_ALIASES_JSON: '["itf"]',
  GRAND_SLAM_ALIAS_MAP_JSON: '{"GRAND_SLAM":["australian open","french open","roland garros","wimbledon","us open","u.s. open","the championships wimbledon"]}',
  WTA_500_ALIAS_MAP_JSON: '{"WTA_500":["wta 500","wta-500","adelaide international","brisbane international","abu dhabi open","qatar totalenergies open","doha","dubai tennis championships","credit one charleston open","charleston","merida open","ostrava open","transylvania open","asb classic","washington open","pan pacific open","linz open","wta finals"]}',
  WTA_1000_ALIAS_MAP_JSON: '{"WTA_1000":["wta 1000","wta-1000","masters 1000","bnp paribas open","indian wells","miami open","miami","madrid open","rome","italian open","canadian open","cincinnati open","china open","wuhan open"]}',
  COMPETITION_DENY_ALIAS_MAP_JSON: '{"ITF":["itf","international tennis federation"]}',
  LOG_PROFILE: 'compact',
  INVARIANT_ENFORCEMENT_LEVEL: 'warn',
  REASON_CODE_ALIAS_ALLOW_CANONICAL_PASSTHROUGH: 'false',
  VERBOSE_LOGGING: 'true',
  LOG_VERBOSITY_LEVEL: '2',
  DUPLICATE_DEBOUNCE_MS: '90000',
  PIPELINE_TRIGGER_EVERY_MIN: '15',
  PLAYER_ALIAS_MAP_JSON: '{}',
  MODEL_VERSION: 'wta_mvp_playerstats_v3',
  EDGE_THRESHOLD_MICRO: '0.015',
  EDGE_THRESHOLD_SMALL: '0.03',
  EDGE_THRESHOLD_MED: '0.05',
  EDGE_THRESHOLD_STRONG: '0.08',
  STAKE_UNITS_MICRO: '0.25',
  STAKE_UNITS_SMALL: '0.5',
  STAKE_UNITS_MED: '1',
  STAKE_UNITS_STRONG: '1.5',
  STAKE_POLICY_MODE: 'strict_suppress_below_min',
  ACCOUNT_CURRENCY: 'MXN',
  DISPLAY_CURRENCY: 'MXN',
  MIN_STAKE_PER_CURRENCY_JSON: '{"MXN":20}',
  SIGNAL_COOLDOWN_MIN: '150',
  MINUTES_BEFORE_START_CUTOFF: '60',
  MINUTES_BEFORE_START_CUTOFF_H2H: '45',
  MIN_START_CUTOFF_RELAXED_STATS_CONFIDENCE: '0.85',
  STALE_ODDS_WINDOW_MIN: '60',
  SUPPRESSION_ANALYTICS_RUN_WINDOW: '20',
  SIGNAL_SUPPRESSION_PRECHECK_SKIP_SCORING: 'true',
  MAX_CURRENT_VS_OPEN_LINE_DELTA: '0',
  MAX_MINUTES_SINCE_OPEN_SNAPSHOT: '0',
  MIN_BOOK_COUNT: '0',
  MIN_LIQUIDITY: '0',
  ODDS_MIN_CREDITS_SOFT_LIMIT: '50',
  ODDS_MIN_CREDITS_HARD_LIMIT: '10',
  ODDS_BURN_RATE_NOTIFY_ENABLED: 'false',
  DISCORD_WEBHOOK: '',
  NOTIFY_ENABLED: 'true',
  NOTIFY_TEST_MODE: 'false',
  PLAYER_STATS_API_BASE_URL: '',
  PLAYER_STATS_ITF_ENDPOINT: 'https://www.itftennis.com/-/media/project/itf/shared/data/rankings/wta/singles.json',
  PLAYER_STATS_API_KEY: '',
  PLAYER_STATS_SCRAPE_URLS: '',
  PLAYER_STATS_PROVIDER_MODE: 'tennisabstract_leaders',
  MODEL_MODE: 'ta_only',
  DISABLE_SOFASCORE: 'true',
  REQUIRE_OPENING_LINE_PROXIMITY: 'true',
  MAX_OPENING_LAG_MINUTES: '60',
  OPENING_LAG_FALLBACK_EXEMPTION_MAX_AGE_MINUTES: '240',
  OPENING_LAG_FALLBACK_EXEMPTION_MAX_ROWS_PER_RUN: '0',
  OPENING_LAG_FALLBACK_KEY_MATCH_WINDOW_MINUTES: '120',
  OPENING_LAG_FALLBACK_KEY_MATCH_MAX_AGE_MINUTES: '240',
  OPENING_LAG_FALLBACK_EXEMPTION_ALLOWED_SOURCES_JSON: '["fallback_cached_stale","fallback_cached_stale_bounded_window"]',
  OPENING_LAG_FALLBACK_EXEMPTION_DENIED_SOURCES_JSON: '["strict_gate","fallback_cached_fresh"]',
  OPENING_LAG_FALLBACK_EXEMPTION_CAP_MODE: 'unlimited_when_zero',
  PLAYER_STATS_TA_LEADERS_URL: 'https://www.tennisabstract.com/cgi-bin/leaders_wta.cgi?players=top',
  PLAYER_STATS_TA_H2H_URL: 'https://tennisabstract.com/reports/h2hMatrixWta.html',
  PLAYER_STATS_FETCH_USER_AGENT: 'Mozilla/5.0 (compatible; WTA-Edge-Board/1.0)',
  PLAYER_STATS_FETCH_TIMEOUT_MS: '30000',
  PLAYER_STATS_FETCH_MAX_RETRIES: '3',
  PLAYER_STATS_FETCH_BACKOFF_BASE_MS: '300',
  PLAYER_STATS_FETCH_BACKOFF_MAX_MS: '4000',
  PLAYER_STATS_TA_REQUEST_DELAY_MS: '300',
  PLAYER_STATS_TA_REQUEST_JITTER_MS: '250',
  PLAYER_STATS_MIN_ACCEPTABLE_COVERAGE_RATIO: '0',
  PLAYER_STATS_MAX_ROWS_PER_RUN: '200',
  PLAYER_STATS_ENABLE_H2H: 'true',
  H2H_BUMP_ENABLED: 'true',
  H2H_MIN_MATCHES: '3',
  H2H_MAX_ABS_BUMP: '0.02',
  PLAYER_STATS_ENABLE_RICH_STATS: 'true',
  PLAYER_STATS_CACHE_TTL_MIN: '10',
  PLAYER_STATS_REFRESH_MIN: '5',
  PLAYER_STATS_FORCE_REFRESH: 'false',
  PLAYER_STATS_COHORT_MODE: 'leadersource',
  PLAYER_STATS_TOP_RANK_MAX: '100',
  PLAYER_STATS_ALLOW_OUT_OF_COHORT_FALLBACK: 'true',
  RUN_HEALTH_CONSECUTIVE_RUN_DEGRADED_TRIGGER: '3',
  RUN_HEALTH_SINGLE_RUN_CRITICAL_TRIGGER: 'true',
  EXPORT_AUTO_ZIP_RETENTION_DAYS: '30',
};

const CONFIG_PRESETS = {
  FREE_TIER_CONSERVATION: {
    PIPELINE_TRIGGER_EVERY_MIN: '30',
    ODDS_REFRESH_TIER_HIGH_INTERVAL_MIN: '15',
    ODDS_REFRESH_TIER_MED_INTERVAL_MIN: '30',
    ODDS_REFRESH_TIER_LOW_INTERVAL_MIN: '60',
    ODDS_WINDOW_PRE_FIRST_MIN: '60',
    ODDS_WINDOW_POST_LAST_MIN: '60',
    ODDS_BOOTSTRAP_LOOKAHEAD_HOURS: '6',
    ODDS_MIN_CREDITS_SOFT_LIMIT: '150',
    ODDS_MIN_CREDITS_HARD_LIMIT: '75',
  },
};


const RUN_LOG_HEADERS = [
  'row_type', 'run_id', 'stage', 'started_at', 'ended_at', 'status', 'reason_code', 'message',
  'fetched_odds', 'fetched_schedule', 'allowed_tournaments', 'matched', 'unmatched', 'signals_found',
  'feature_completeness', 'edge_volatility', 'matched_events', 'scored_signals',
  'no_hit_no_events_from_source_count', 'no_hit_events_outside_time_window_count',
  'no_hit_tournament_filter_excluded_count', 'no_hit_odds_present_but_match_failed_count',
  'no_hit_schema_invalid_metrics_count', 'no_hit_terminal_reason_code', 'terminal_no_hit_diagnostics',
  'feature_completeness_detail', 'reason_alias_payload', 'schema_violation', 'field_type_error',
  'stake_mode_used', 'raw_risk_mxn', 'raw_target_win_mxn', 'final_risk_mxn', 'final_units',
  'stake_adjustment_reason', 'min_bet_mxn', 'bucket_step_mxn', 'unit_size_mxn',
  'signals_scored', 'signal_decision_summary', 'rejection_codes', 'cooldown_suppressed', 'duplicate_suppressed',
  'lock_event', 'debounce_event', 'trigger_event', 'exception', 'stack', 'stage_summaries',
];

const PROPS = {
  PIPELINE_TRIGGER_SIGNATURE: 'PIPELINE_TRIGGER_SIGNATURE',
  LAST_PIPELINE_RUN_TS: 'LAST_PIPELINE_RUN_TS',
  LAST_PIPELINE_START_TS: 'LAST_PIPELINE_START_TS',
  PIPELINE_RUNTIME_EWMA_MS: 'PIPELINE_RUNTIME_EWMA_MS',
  PIPELINE_RUNTIME_SAMPLE_COUNT: 'PIPELINE_RUNTIME_SAMPLE_COUNT',
  DUPLICATE_PREVENTED_COUNT: 'DUPLICATE_PREVENTED_COUNT',
  WORKBOOK_RESET_IN_PROGRESS: 'WORKBOOK_RESET_IN_PROGRESS',
};

const TIMESTAMP_TIMEZONE = {
  ID: 'America/Hermosillo',
  OFFSET: '-07:00',
};

const REASON_CODE_ALIAS_SCHEMA_ID = 'reason_code_alias_v1';
const REASON_CODE_ALIAS_DICTIONARIES = {
  [REASON_CODE_ALIAS_SCHEMA_ID]: {
    competition_allowed: 'CMP_ALLOW',
    long_reason_code_key_for_schedule_enrichment_h2h_missing: 'SCH_H2H_MISS',
    market_h2h: 'MKT_H2H',
    matched_count: 'MATCH_CT',
    matched_exact: 'MATCH_EXACT',
    odds_actionable: 'ODDS_ACT',
    odds_api_success: 'ODDS_API_OK',
    odds_fetched: 'ODDS_FETCH',
    odds_non_actionable: 'ODDS_NACT',
    odds_no_active_wta_keys: 'ODDS_NO_WTA',
    opening_lag_fallback_exempted: 'OPEN_EXEMPT',
    provider_returned_null_features: 'PROV_NULL',
    rejected_count: 'REJ_CT',
    schedule_fetched: 'SCH_FETCH',
    schedule_api_success: 'SCH_API_OK',
    schedule_no_games_in_window: 'SCH_NO_GAME',
    schedule_no_active_wta_keys: 'SCH_NO_WTA',
    signal_edge_above_threshold: 'SIG_EDGE',
    signals_generated: 'SIG_GEN',
    stats_loaded: 'STATS_LOAD',
    schedule_missing_player_identity: 'SCH_MISS_PID',
    very_long_reason_code_key_player_stats_incomplete_profile: 'STATS_INCOMP',
    odds_refresh_skipped_outside_window: 'OR_OUT_WIN',
    odds_refresh_cache_hit_within_window: 'OR_HIT_WIN',
    odds_refresh_cache_hit_outside_window: 'OR_HIT_OUT',
    odds_refresh_skipped_credits_soft_limit: 'OR_SKIP_SOFT',
    odds_refresh_skipped_credits_hard_limit: 'OR_SKIP_HARD',
    odds_refresh_skipped_no_games: 'OR_SKIP_NOGAME',
    odds_refresh_executed_in_window: 'OR_EXEC_INWIN',
    odds_refresh_bootstrap_fetch: 'OR_BOOT_FETCH',
    odds_refresh_bootstrap_inactive: 'OR_BOOT_OFF',
    odds_refresh_bootstrap_blocked_by_credit_limit: 'OR_BOOT_CREDIT',
    odds_refresh_no_eligible_matches: 'OR_NO_ELIG',
    odds_refresh_fetched_success: 'OR_FETCH_OK',
    schedule_fetch_skipped_outside_window_credit_saver: 'SCH_SKIP_OUT_CRED',
    schedule_fetch_skipped_outside_window_credit_saver_cache_expired: 'SCH_SKIP_OUT_EXP',
    productive_output_empty_streak_detected: 'PO_EMPTY_STK',
    schedule_only_streak_detected: 'SCH_ONLY_STK',
    bootstrap_empty_cycle_detected: 'BOOT_EMPTY_STK',
    opening_lag_within_limit: 'OPEN_LAG_OK',
    opening_lag_exceeded: 'OPEN_LAG_HI',
    opening_lag_blocked: 'OPEN_LAG_BLOCK',
    missing_open_timestamp: 'OPEN_TS_MISS',
    run_health_no_matches_from_odds: 'RH_NO_MATCH',
    run_health_no_matches_from_odds_consecutive: 'RH_NO_MATCH_STK',
    run_health_no_matches_from_odds_waiting: 'RH_NO_MATCH_WAIT',
    run_health_single_run_critical_triggered: 'RH_CRIT_1RUN',
    source_entity_domain_mismatch_non_tennis_sport_slug_football: 'SRC_DM_FOOT',
    source_entity_domain_mismatch: 'SRC_DM',
    stake_policy_disabled: 'STAKE_OFF',
    stake_missing_unscored: 'STAKE_MISS',
    stake_below_min_suppressed: 'STAKE_SUP',
    stake_rounded_to_min: 'STAKE_RND',
    stake_policy_pass: 'STAKE_OK',
    stake_policy_config_error: 'STAKE_CFG',
    player_stats_out_of_cohort_only: 'PSTATS_OUT_COH',
    player_stats_unknown_rank_only: 'PSTATS_UNK_RNK',
    match_map_diagnostic_records_written: 'MM_DIAG_WR',
    match_map_upserts: 'MM_UPS',
    match_map_upserts_matched: 'MM_UPS_MT',
    match_map_upserts_rejected: 'MM_UPS_RJ',
    productive_output_mitigation_activated: 'PO_MIT_ON',
    odds_api_failure_no_stale_fallback: 'ODDS_NO_STALE',
    odds_cache_hit: 'ODDS_CACHE_HIT',
    schedule_cache_hit: 'SCH_CACHE_HIT',
    stats_cache_hit: 'STATS_CACHE_HIT',
    odds_stale_fallback: 'ODDS_STALE_FB',
    schedule_stale_fallback: 'SCH_STALE_FB',
    stats_stale_fallback: 'STATS_STALE_FB',
    odds_cache_stale_refresh_throttled: 'ODDS_CACHE_THR',
    schedule_cache_stale_refresh_throttled: 'SCH_CACHE_THR',
    stats_cache_stale_refresh_throttled: 'STATS_CACHE_THR',
    raw_odds_upserts: 'ODDS_UPS',
    raw_player_stats_upserts: 'PSTATS_UPS',
    raw_schedule_upserts: 'SCH_UPS',
    signals_upserts: 'SIG_UPS',

    bookmakers_without_h2h_market: 'ODDS_NO_H2H_BM',
    cooldown_suppressed: 'COOL_SUP',
    duplicate_suppressed: 'DUP_SUP',
    edge_below_threshold: 'EDGE_LOW',
    events_missing_h2h_outcomes: 'ODDS_NO_H2H_EVT',
    fallback_short_circuit: 'MATCH_FB_SC',
    h2h_unavailable: 'H2H_UNAV',
    low_edge_suppressed: 'EDGE_SUP',
    missing_open_timestamp_fallback: 'OPEN_TS_FB',
    no_odds_candidates: 'NO_ODDS_CAND',
    no_player_match: 'NO_P_MATCH',
    schedule_missing_player_identity: 'SCH_MISS_PID',
    no_schedule_candidates: 'NO_SCH_CAND',
    notify_disabled: 'NOTIFY_OFF',
    notify_missing_config: 'NOTIFY_CFG',
    null_features_fallback_scored: 'NULL_FB_SCORE',
    odds_rows_emitted: 'ODDS_ROWS',
    outside_window: 'OUT_WIN',
    outside_window_idle_skip: 'OUT_WIN_IDLE',
    missing_b: 'MISS_B',
    missing_repeat: 'MISS_REPEAT',
    provider_returned_empty: 'PROV_EMPTY',
    runtime_mode_soft_degraded: 'RT_SOFT',
    schedule_seed_no_odds: 'SCH_SEED_NO_ODDS',
    schedule_only_seed: 'SCH_ONLY_SEED',
    schedule_unavailable: 'SCH_UNAV',
    schedule_window_empty: 'SCH_WIN_EMPTY',
    schedule_enrichment_no_schedule_events: 'SCH_ENR_NONE',
    schedule_enrichment_no_upcoming_players: 'SCH_ENR_NOPLY',
    schedule_enrichment_ta_completed: 'SCH_ENR_TA_OK',
    schedule_enrichment_ta_failed_non_fatal: 'SCH_ENR_TA_WARN',
    schedule_api_success_sport_key_fallback: 'SCH_API_SK_FB',
    sent: 'NOTIFY_SENT',
    skipped_no_matched_events: 'SKIP_NO_MATCHED',
    skipped_no_player_keys: 'SKIP_NO_KEYS',
    skipped_schedule_only_no_odds: 'SKIP_SCH_ONLY',
    stale_fallback_bypassed: 'STALE_BYPASS',
    state_stale_payload_write_failed_non_fatal: 'STALE_WRITE_WARN',
    stale_odds_skip: 'STALE_SKIP',
    stats_enriched: 'STATS_ENR',
    stats_fallback_model_used: 'STATS_FB_MODEL',
    stats_missing_player_a: 'STATS_MISS_A',
    stats_missing_player_b: 'STATS_MISS_B',
    stats_out_of_cohort: 'STATS_OOC',
    stats_rank_unknown: 'STATS_RANK_UNK',
    stats_top100_filter_excluded: 'STATS_T100_EXCL',
    stats_top100_fallback_applied: 'STATS_T100_FB',
    stats_zero_coverage: 'STATS_ZERO',
    bounded_stage_counter_invariant_exceeded: 'INV_BOUNDED_CNT',
    run_exception: 'RUN_EXC',
    run_success: 'RUN_OK',
    run_disabled_skip: 'RUN_SKIP_OFF',
    run_locked_skip: 'RUN_SKIP_LOCK',
    run_debounced_skip: 'RUN_SKIP_DEB',
    run_idempotency_overlap_skip: 'RUN_SKIP_IDEMP',
    run_rollup_emitted: 'RUN_ROLLUP',
    run_mode_gates: 'RUN_MODE',
    started: 'RUN_START',
    completed: 'RUN_DONE',
    trigger_noop: 'TRIG_NOOP',
    trigger_reinstalled: 'TRIG_REINST',
    trigger_removed: 'TRIG_REM',
    trigger_post_install_health: 'TRIG_HEALTH',
    reason_code_map_mutated_after_snapshot: 'RC_MUTATE_SNAP',
    reason_code_counter_exceeds_stage_max: 'RC_GT_STAGE_MAX',
    reason_code_alias_missing_fallback_emitted: 'RC_ALIAS_FALLBACK',
    opening_lag_fallback_exemption_allowed: 'OPEN_FB_ALLOW',
    run_health_expected_temporary_no_odds: 'RH_TMP_NO_ODDS',
    opening_lag_fallback_exemption_denied_source: 'OPEN_FB_DENY_SRC',
    opening_lag_fallback_exemption_denied_age: 'OPEN_FB_DENY_AGE',
    opening_lag_fallback_exemption_denied_cap: 'OPEN_FB_DENY_CAP',
    odds_api_success_sport_key_fallback: 'ODDS_API_SK_FB',
    ta_matchmx_ok: 'TA_MX_OK',
    ta_matchmx_parse_failed: 'TA_MX_PARSE',
    ta_matchmx_coverage_miss: 'TA_MX_COVMISS',
    ta_matchmx_coverage_ratio_low: 'TA_MX_COVLOW',
    ta_parse_coverage_mismatch: 'TA_MX_COVMM',
    ta_matchmx_overlap_low: 'TA_MX_OVLP',
    ta_matchmx_feature_coverage_low: 'TA_MX_FEAT',
    ta_matchmx_rows_low: 'TA_MX_ROWS',
    ta_matchmx_distinct_players_low: 'TA_MX_DIST',
    ta_matchmx_name_quality_low: 'TA_MX_NAME',
    ta_matchmx_unusable_payload: 'TA_MX_UNUSE',
    ta_matchmx_stale_fallback: 'TA_MX_STALE_FB',
    too_close_to_start_skip: 'TOO_CLOSE_SKIP',
    invalid_time_window: 'TIME_WIN_BAD',
    invalid_time_window_retry_failed: 'TIME_WIN_RETRY',
    invalid_time_window_recovered_relaxed_query: 'TIME_WIN_RELAX',
    credit_hard_limit_skip_odds: 'CREDIT_HARD_ODDS',
    source_credit_saver_skip: 'SRC_CRED_SKIP',
    schedule_window_fallback_no_odds: 'SCH_WIN_FB_NO',
    credit_header_missing: 'CREDIT_HDR',
    within_window: 'IN_WIN',
  },
};

function getConfig_() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  let sh = ss.getSheetByName(SHEETS.CONFIG);
  if (!sh) {
    ensureTabsAndConfig_();
    sh = ss.getSheetByName(SHEETS.CONFIG);
  }
  if (!sh) {
    throw new Error(
      '[config_sheet_missing_preflight] Config sheet is missing. '
      + 'Run "Setup / Verify Tabs" (or "Re-create / Reset Workbook") once, then retry.'
    );
  }
  const values = sh.getDataRange().getValues();
  const parsed = parseConfigRows_(values, {
    mode: 'error',
    context: 'getConfig_',
  });
  const config = parsed.config;
  const resolvedLogProfile = normalizeLogProfile_(config.LOG_PROFILE || DEFAULT_CONFIG.LOG_PROFILE);
  const resolvedVerboseLogging = toBoolean_(config.VERBOSE_LOGGING, resolvedLogProfile === 'verbose');
  const defaultLogVerbosityLevel = resolvedLogProfile === 'verbose'
    ? (resolvedVerboseLogging ? 2 : 1)
    : 1;

  return {
    RUN_ENABLED: toBoolean_(config.RUN_ENABLED, true),
    LOOKAHEAD_HOURS: toNumber_(config.LOOKAHEAD_HOURS, 36),
    ODDS_SPORT_KEY: String(config.ODDS_SPORT_KEY || ''),
    ODDS_API_BASE_URL: String(config.ODDS_API_BASE_URL || 'https://api.the-odds-api.com/v4'),
    ODDS_API_KEY: String(config.ODDS_API_KEY || ''),
    ODDS_MARKETS: String(config.ODDS_MARKETS || 'h2h'),
    ODDS_REGIONS: String(config.ODDS_REGIONS || 'us'),
    ODDS_ODDS_FORMAT: String(config.ODDS_ODDS_FORMAT || 'american'),
    ODDS_CACHE_TTL_SEC: toNumber_(config.ODDS_CACHE_TTL_SEC, 300),
    ODDS_WINDOW_CACHE_TTL_MIN: toNumber_(config.ODDS_WINDOW_CACHE_TTL_MIN, 5),
    ODDS_WINDOW_REFRESH_MIN: toNumber_(config.ODDS_WINDOW_REFRESH_MIN, 5),
    ODDS_WINDOW_FORCE_REFRESH: toBoolean_(config.ODDS_WINDOW_FORCE_REFRESH, false),
    ODDS_API_MAX_RETRIES: Math.max(0, toNumber_(config.ODDS_API_MAX_RETRIES, 3)),
    ODDS_API_BACKOFF_BASE_MS: Math.max(0, toNumber_(config.ODDS_API_BACKOFF_BASE_MS, 250)),
    ODDS_API_BACKOFF_MAX_MS: Math.max(0, toNumber_(config.ODDS_API_BACKOFF_MAX_MS, 3000)),
    ODDS_WINDOW_PRE_FIRST_MIN: toNumber_(config.ODDS_WINDOW_PRE_FIRST_MIN, 120),
    ODDS_WINDOW_POST_LAST_MIN: toNumber_(config.ODDS_WINDOW_POST_LAST_MIN, 180),
    ODDS_REFRESH_TIER_LOW_UPPER_MIN: Math.max(0, toNumber_(config.ODDS_REFRESH_TIER_LOW_UPPER_MIN, 240)),
    ODDS_REFRESH_TIER_MED_UPPER_MIN: Math.max(0, toNumber_(config.ODDS_REFRESH_TIER_MED_UPPER_MIN, 180)),
    ODDS_REFRESH_TIER_HIGH_UPPER_MIN: Math.max(0, toNumber_(config.ODDS_REFRESH_TIER_HIGH_UPPER_MIN, 90)),
    ODDS_REFRESH_TIER_LOW_INTERVAL_MIN: Math.max(1, toNumber_(config.ODDS_REFRESH_TIER_LOW_INTERVAL_MIN, 20)),
    ODDS_REFRESH_TIER_MED_INTERVAL_MIN: Math.max(1, toNumber_(config.ODDS_REFRESH_TIER_MED_INTERVAL_MIN, 10)),
    ODDS_REFRESH_TIER_HIGH_INTERVAL_MIN: Math.max(1, toNumber_(config.ODDS_REFRESH_TIER_HIGH_INTERVAL_MIN, 5)),
    ODDS_NO_GAMES_BEHAVIOR: String(config.ODDS_NO_GAMES_BEHAVIOR || 'SKIP').toUpperCase(),
    ODDS_BOOTSTRAP_LOOKAHEAD_HOURS: toNumber_(config.ODDS_BOOTSTRAP_LOOKAHEAD_HOURS, 18),
    BOOTSTRAP_EMPTY_CYCLE_THRESHOLD: Math.max(1, toNumber_(config.BOOTSTRAP_EMPTY_CYCLE_THRESHOLD, 3)),
    EMPTY_PRODUCTIVE_OUTPUT_THRESHOLD: Math.max(1, toNumber_(config.EMPTY_PRODUCTIVE_OUTPUT_THRESHOLD, 3)),
    SCHEDULE_ONLY_STREAK_NOTICE_THRESHOLD: Math.max(1, toNumber_(config.SCHEDULE_ONLY_STREAK_NOTICE_THRESHOLD, 3)),
    PRODUCTIVE_OUTPUT_MITIGATION_ENABLED: toBoolean_(config.PRODUCTIVE_OUTPUT_MITIGATION_ENABLED, toBoolean_(DEFAULT_CONFIG.PRODUCTIVE_OUTPUT_MITIGATION_ENABLED, false)),
    PRODUCTIVE_OUTPUT_MITIGATION_OPENING_LAG_WIDEN_ENABLED: toBoolean_(
      config.PRODUCTIVE_OUTPUT_MITIGATION_OPENING_LAG_WIDEN_ENABLED,
      toBoolean_(DEFAULT_CONFIG.PRODUCTIVE_OUTPUT_MITIGATION_OPENING_LAG_WIDEN_ENABLED, false)
    ),
    PRODUCTIVE_OUTPUT_MITIGATION_OPENING_LAG_EXTRA_MINUTES: Math.max(
      0,
      toNumber_(
        config.PRODUCTIVE_OUTPUT_MITIGATION_OPENING_LAG_EXTRA_MINUTES,
        toNumber_(DEFAULT_CONFIG.PRODUCTIVE_OUTPUT_MITIGATION_OPENING_LAG_EXTRA_MINUTES, 30)
      )
    ),
    PRODUCTIVE_OUTPUT_MITIGATION_FORCE_FRESH_ODDS_PROBE_ENABLED: toBoolean_(
      config.PRODUCTIVE_OUTPUT_MITIGATION_FORCE_FRESH_ODDS_PROBE_ENABLED,
      toBoolean_(DEFAULT_CONFIG.PRODUCTIVE_OUTPUT_MITIGATION_FORCE_FRESH_ODDS_PROBE_ENABLED, false)
    ),
    PRODUCTIVE_OUTPUT_MITIGATION_VERBOSE_DIAGNOSTICS_ENABLED: toBoolean_(
      config.PRODUCTIVE_OUTPUT_MITIGATION_VERBOSE_DIAGNOSTICS_ENABLED,
      toBoolean_(DEFAULT_CONFIG.PRODUCTIVE_OUTPUT_MITIGATION_VERBOSE_DIAGNOSTICS_ENABLED, false)
    ),
    REASON_CODE_ALIAS_ALLOW_CANONICAL_PASSTHROUGH: toBoolean_(
      config.REASON_CODE_ALIAS_ALLOW_CANONICAL_PASSTHROUGH,
      toBoolean_(DEFAULT_CONFIG.REASON_CODE_ALIAS_ALLOW_CANONICAL_PASSTHROUGH, false)
    ),
    ROLLUP_EVERY_N_RUNS: Math.max(1, toNumber_(config.ROLLUP_EVERY_N_RUNS, 10)),
    SCHEDULE_BUFFER_BEFORE_MIN: toNumber_(config.SCHEDULE_BUFFER_BEFORE_MIN, 180),
    SCHEDULE_BUFFER_AFTER_MIN: toNumber_(config.SCHEDULE_BUFFER_AFTER_MIN, 180),
    MATCH_TIME_TOLERANCE_MIN: toNumber_(config.MATCH_TIME_TOLERANCE_MIN, 45),
    MATCH_FALLBACK_EXPANSION_MIN: toNumber_(config.MATCH_FALLBACK_EXPANSION_MIN, 120),
    MATCH_FALLBACK_HARD_MAX_DELTA_MIN: toNumber_(config.MATCH_FALLBACK_HARD_MAX_DELTA_MIN, 1440),
    MATCH_NEAREST_CANDIDATE_MIN_SIMILARITY: Math.max(0, Math.min(1, toNumber_(
      config.MATCH_NEAREST_CANDIDATE_MIN_SIMILARITY,
      toNumber_(DEFAULT_CONFIG.MATCH_NEAREST_CANDIDATE_MIN_SIMILARITY, 0.55)
    ))),
    ALLOW_WTA_125: toBoolean_(config.ALLOW_WTA_125, false),
    // Keep fallback aligned with DEFAULT_CONFIG to avoid runtime/default drift.
    ALLOW_WTA_250: toBoolean_(config.ALLOW_WTA_250, toBoolean_(DEFAULT_CONFIG.ALLOW_WTA_250, true)),
    COMPETITION_SOURCE_FIELDS_JSON: String(config.COMPETITION_SOURCE_FIELDS_JSON || DEFAULT_CONFIG.COMPETITION_SOURCE_FIELDS_JSON),
    GRAND_SLAM_ALIASES_JSON: String(config.GRAND_SLAM_ALIASES_JSON || DEFAULT_CONFIG.GRAND_SLAM_ALIASES_JSON),
    WTA_1000_ALIASES_JSON: String(config.WTA_1000_ALIASES_JSON || DEFAULT_CONFIG.WTA_1000_ALIASES_JSON),
    WTA_500_ALIASES_JSON: String(config.WTA_500_ALIASES_JSON || DEFAULT_CONFIG.WTA_500_ALIASES_JSON),
    COMPETITION_DENY_ALIASES_JSON: String(config.COMPETITION_DENY_ALIASES_JSON || DEFAULT_CONFIG.COMPETITION_DENY_ALIASES_JSON),
    GRAND_SLAM_ALIAS_MAP_JSON: String(config.GRAND_SLAM_ALIAS_MAP_JSON || DEFAULT_CONFIG.GRAND_SLAM_ALIAS_MAP_JSON),
    WTA_500_ALIAS_MAP_JSON: String(config.WTA_500_ALIAS_MAP_JSON || DEFAULT_CONFIG.WTA_500_ALIAS_MAP_JSON),
    WTA_1000_ALIAS_MAP_JSON: String(config.WTA_1000_ALIAS_MAP_JSON || DEFAULT_CONFIG.WTA_1000_ALIAS_MAP_JSON),
    COMPETITION_DENY_ALIAS_MAP_JSON: String(config.COMPETITION_DENY_ALIAS_MAP_JSON || DEFAULT_CONFIG.COMPETITION_DENY_ALIAS_MAP_JSON),
    LOG_PROFILE: resolvedLogProfile,
    INVARIANT_ENFORCEMENT_LEVEL: normalizeInvariantEnforcementLevel_(
      config.INVARIANT_ENFORCEMENT_LEVEL || DEFAULT_CONFIG.INVARIANT_ENFORCEMENT_LEVEL
    ),
    VERBOSE_LOGGING: resolvedVerboseLogging,
    LOG_VERBOSITY_LEVEL: Math.max(0, Math.min(3, toNumber_(config.LOG_VERBOSITY_LEVEL, defaultLogVerbosityLevel))),
    DUPLICATE_DEBOUNCE_MS: toNumber_(config.DUPLICATE_DEBOUNCE_MS, 90000),
    PIPELINE_TRIGGER_EVERY_MIN: Math.max(1, toNumber_(config.PIPELINE_TRIGGER_EVERY_MIN, 15)),
    PLAYER_ALIAS_MAP_JSON: String(config.PLAYER_ALIAS_MAP_JSON || '{}'),
    MODEL_VERSION: String(config.MODEL_VERSION || 'wta_mvp_playerstats_v3'),
    EDGE_THRESHOLD_MICRO: toNumber_(config.EDGE_THRESHOLD_MICRO, 0.015),
    EDGE_THRESHOLD_SMALL: toNumber_(config.EDGE_THRESHOLD_SMALL, 0.03),
    EDGE_THRESHOLD_MED: toNumber_(config.EDGE_THRESHOLD_MED, 0.05),
    EDGE_THRESHOLD_STRONG: toNumber_(config.EDGE_THRESHOLD_STRONG, 0.08),
    STAKE_UNITS_MICRO: toNumber_(config.STAKE_UNITS_MICRO, 0.25),
    STAKE_UNITS_SMALL: toNumber_(config.STAKE_UNITS_SMALL, 0.5),
    STAKE_UNITS_MED: toNumber_(config.STAKE_UNITS_MED, 1),
    STAKE_UNITS_STRONG: toNumber_(config.STAKE_UNITS_STRONG, 1.5),
    STAKE_POLICY_MODE: String(config.STAKE_POLICY_MODE || DEFAULT_CONFIG.STAKE_POLICY_MODE || 'strict_suppress_below_min').toLowerCase(),
    ACCOUNT_CURRENCY: String(config.ACCOUNT_CURRENCY || DEFAULT_CONFIG.ACCOUNT_CURRENCY || 'MXN').toUpperCase(),
    DISPLAY_CURRENCY: String(config.DISPLAY_CURRENCY || config.ACCOUNT_CURRENCY || DEFAULT_CONFIG.DISPLAY_CURRENCY || 'MXN').toUpperCase(),
    MIN_STAKE_PER_CURRENCY_JSON: String(
      config.MIN_STAKE_PER_CURRENCY_JSON
      || DEFAULT_CONFIG.MIN_STAKE_PER_CURRENCY_JSON
      || '{"MXN":20}'
    ),
    SIGNAL_COOLDOWN_MIN: toNumber_(config.SIGNAL_COOLDOWN_MIN, 150),
    MINUTES_BEFORE_START_CUTOFF: toNumber_(config.MINUTES_BEFORE_START_CUTOFF, 60),
    MINUTES_BEFORE_START_CUTOFF_H2H: toNumber_(config.MINUTES_BEFORE_START_CUTOFF_H2H, toNumber_(DEFAULT_CONFIG.MINUTES_BEFORE_START_CUTOFF_H2H, 45)),
    MIN_START_CUTOFF_RELAXED_STATS_CONFIDENCE: toNumber_(
      config.MIN_START_CUTOFF_RELAXED_STATS_CONFIDENCE,
      toNumber_(DEFAULT_CONFIG.MIN_START_CUTOFF_RELAXED_STATS_CONFIDENCE, 0.85)
    ),
    STALE_ODDS_WINDOW_MIN: toNumber_(config.STALE_ODDS_WINDOW_MIN, 60),
    SUPPRESSION_ANALYTICS_RUN_WINDOW: Math.max(1, toNumber_(config.SUPPRESSION_ANALYTICS_RUN_WINDOW, toNumber_(DEFAULT_CONFIG.SUPPRESSION_ANALYTICS_RUN_WINDOW, 20))),
    MAX_CURRENT_VS_OPEN_LINE_DELTA: Math.max(0, toNumber_(config.MAX_CURRENT_VS_OPEN_LINE_DELTA, 0)),
    MAX_MINUTES_SINCE_OPEN_SNAPSHOT: Math.max(0, toNumber_(config.MAX_MINUTES_SINCE_OPEN_SNAPSHOT, 0)),
    MIN_BOOK_COUNT: Math.max(0, toNumber_(config.MIN_BOOK_COUNT, 0)),
    MIN_LIQUIDITY: Math.max(0, toNumber_(config.MIN_LIQUIDITY, 0)),
    ODDS_MIN_CREDITS_SOFT_LIMIT: toNumber_(config.ODDS_MIN_CREDITS_SOFT_LIMIT, 50),
    ODDS_MIN_CREDITS_HARD_LIMIT: toNumber_(config.ODDS_MIN_CREDITS_HARD_LIMIT, 10),
    ODDS_BURN_RATE_NOTIFY_ENABLED: toBoolean_(config.ODDS_BURN_RATE_NOTIFY_ENABLED, toBoolean_(DEFAULT_CONFIG.ODDS_BURN_RATE_NOTIFY_ENABLED, false)),
    DISCORD_WEBHOOK: String(config.DISCORD_WEBHOOK || ''),
    NOTIFY_ENABLED: toBoolean_(config.NOTIFY_ENABLED, true),
    NOTIFY_TEST_MODE: toBoolean_(config.NOTIFY_TEST_MODE, false),
    PLAYER_STATS_API_BASE_URL: String(config.PLAYER_STATS_API_BASE_URL || ''),
    PLAYER_STATS_ITF_ENDPOINT: String(config.PLAYER_STATS_ITF_ENDPOINT || DEFAULT_CONFIG.PLAYER_STATS_ITF_ENDPOINT),
    PLAYER_STATS_API_KEY: String(config.PLAYER_STATS_API_KEY || ''),
    PLAYER_STATS_SCRAPE_URLS: String(config.PLAYER_STATS_SCRAPE_URLS || ''),
    PLAYER_STATS_PROVIDER_MODE: String(config.PLAYER_STATS_PROVIDER_MODE || DEFAULT_CONFIG.PLAYER_STATS_PROVIDER_MODE),
    MODEL_MODE: String(config.MODEL_MODE || DEFAULT_CONFIG.MODEL_MODE).toLowerCase(),
    DISABLE_SOFASCORE: toBoolean_(config.DISABLE_SOFASCORE, toBoolean_(DEFAULT_CONFIG.DISABLE_SOFASCORE, true)),
    REQUIRE_OPENING_LINE_PROXIMITY: toBoolean_(config.REQUIRE_OPENING_LINE_PROXIMITY, toBoolean_(DEFAULT_CONFIG.REQUIRE_OPENING_LINE_PROXIMITY, true)),
    MAX_OPENING_LAG_MINUTES: Math.max(0, toNumber_(config.MAX_OPENING_LAG_MINUTES, toNumber_(DEFAULT_CONFIG.MAX_OPENING_LAG_MINUTES, 60))),
    OPENING_LAG_FALLBACK_EXEMPTION_MAX_AGE_MINUTES: Math.max(0, toNumber_(
      config.OPENING_LAG_FALLBACK_EXEMPTION_MAX_AGE_MINUTES,
      toNumber_(DEFAULT_CONFIG.OPENING_LAG_FALLBACK_EXEMPTION_MAX_AGE_MINUTES, 240)
    )),
    OPENING_LAG_FALLBACK_EXEMPTION_MAX_ROWS_PER_RUN: Math.max(0, toNumber_(
      config.OPENING_LAG_FALLBACK_EXEMPTION_MAX_ROWS_PER_RUN,
      toNumber_(DEFAULT_CONFIG.OPENING_LAG_FALLBACK_EXEMPTION_MAX_ROWS_PER_RUN, 0)
    )),
    OPENING_LAG_FALLBACK_KEY_MATCH_WINDOW_MINUTES: Math.max(0, toNumber_(
      config.OPENING_LAG_FALLBACK_KEY_MATCH_WINDOW_MINUTES,
      toNumber_(DEFAULT_CONFIG.OPENING_LAG_FALLBACK_KEY_MATCH_WINDOW_MINUTES, 120)
    )),
    OPENING_LAG_FALLBACK_KEY_MATCH_MAX_AGE_MINUTES: Math.max(0, toNumber_(
      config.OPENING_LAG_FALLBACK_KEY_MATCH_MAX_AGE_MINUTES,
      toNumber_(DEFAULT_CONFIG.OPENING_LAG_FALLBACK_KEY_MATCH_MAX_AGE_MINUTES, 240)
    )),
    OPENING_LAG_FALLBACK_EXEMPTION_ALLOWED_SOURCES_JSON: String(
      config.OPENING_LAG_FALLBACK_EXEMPTION_ALLOWED_SOURCES_JSON
      || DEFAULT_CONFIG.OPENING_LAG_FALLBACK_EXEMPTION_ALLOWED_SOURCES_JSON
      || '["fallback_cached_stale","fallback_cached_stale_bounded_window"]'
    ),
    OPENING_LAG_FALLBACK_EXEMPTION_DENIED_SOURCES_JSON: String(
      config.OPENING_LAG_FALLBACK_EXEMPTION_DENIED_SOURCES_JSON
      || DEFAULT_CONFIG.OPENING_LAG_FALLBACK_EXEMPTION_DENIED_SOURCES_JSON
      || '[]'
    ),
    OPENING_LAG_FALLBACK_EXEMPTION_CAP_MODE: String(
      config.OPENING_LAG_FALLBACK_EXEMPTION_CAP_MODE
      || DEFAULT_CONFIG.OPENING_LAG_FALLBACK_EXEMPTION_CAP_MODE
      || 'unlimited_when_zero'
    ).toLowerCase(),
    PLAYER_STATS_TA_LEADERS_URL: String(config.PLAYER_STATS_TA_LEADERS_URL || DEFAULT_CONFIG.PLAYER_STATS_TA_LEADERS_URL),
    PLAYER_STATS_TA_H2H_URL: String(config.PLAYER_STATS_TA_H2H_URL || DEFAULT_CONFIG.PLAYER_STATS_TA_H2H_URL),
    PLAYER_STATS_FETCH_USER_AGENT: String(config.PLAYER_STATS_FETCH_USER_AGENT || DEFAULT_CONFIG.PLAYER_STATS_FETCH_USER_AGENT),
    PLAYER_STATS_FETCH_TIMEOUT_MS: toNumber_(config.PLAYER_STATS_FETCH_TIMEOUT_MS, 30000),
    PLAYER_STATS_FETCH_MAX_RETRIES: Math.max(0, toNumber_(config.PLAYER_STATS_FETCH_MAX_RETRIES, 3)),
    PLAYER_STATS_FETCH_BACKOFF_BASE_MS: Math.max(0, toNumber_(config.PLAYER_STATS_FETCH_BACKOFF_BASE_MS, 300)),
    PLAYER_STATS_FETCH_BACKOFF_MAX_MS: Math.max(0, toNumber_(config.PLAYER_STATS_FETCH_BACKOFF_MAX_MS, 4000)),
    PLAYER_STATS_TA_REQUEST_DELAY_MS: Math.max(0, toNumber_(config.PLAYER_STATS_TA_REQUEST_DELAY_MS, 300)),
    PLAYER_STATS_TA_REQUEST_JITTER_MS: Math.max(0, toNumber_(config.PLAYER_STATS_TA_REQUEST_JITTER_MS, 250)),
    PLAYER_STATS_MIN_ACCEPTABLE_COVERAGE_RATIO: Math.min(1, Math.max(0, toNumber_(
      config.PLAYER_STATS_MIN_ACCEPTABLE_COVERAGE_RATIO,
      toNumber_(DEFAULT_CONFIG.PLAYER_STATS_MIN_ACCEPTABLE_COVERAGE_RATIO, 0)
    ))),
    PLAYER_STATS_MAX_ROWS_PER_RUN: Math.max(1, toNumber_(config.PLAYER_STATS_MAX_ROWS_PER_RUN, 200)),
    PLAYER_STATS_ENABLE_H2H: toBoolean_(config.PLAYER_STATS_ENABLE_H2H, true),
    H2H_BUMP_ENABLED: toBoolean_(config.H2H_BUMP_ENABLED, true),
    H2H_MIN_MATCHES: Math.max(1, toNumber_(config.H2H_MIN_MATCHES, 3)),
    H2H_MAX_ABS_BUMP: Math.max(0, toNumber_(config.H2H_MAX_ABS_BUMP, 0.02)),
    PLAYER_STATS_ENABLE_RICH_STATS: toBoolean_(config.PLAYER_STATS_ENABLE_RICH_STATS, true),
    PLAYER_STATS_CACHE_TTL_MIN: toNumber_(config.PLAYER_STATS_CACHE_TTL_MIN, 10),
    PLAYER_STATS_REFRESH_MIN: toNumber_(config.PLAYER_STATS_REFRESH_MIN, 5),
    PLAYER_STATS_FORCE_REFRESH: toBoolean_(config.PLAYER_STATS_FORCE_REFRESH, false),
    PLAYER_STATS_COHORT_MODE: normalizePlayerStatsCohortMode_(config.PLAYER_STATS_COHORT_MODE || DEFAULT_CONFIG.PLAYER_STATS_COHORT_MODE),
    PLAYER_STATS_TOP_RANK_MAX: Math.max(1, toNumber_(
      config.PLAYER_STATS_TOP_RANK_MAX,
      toNumber_(DEFAULT_CONFIG.PLAYER_STATS_TOP_RANK_MAX, 100)
    )),
    PLAYER_STATS_ALLOW_OUT_OF_COHORT_FALLBACK: toBoolean_(
      config.PLAYER_STATS_ALLOW_OUT_OF_COHORT_FALLBACK,
      toBoolean_(DEFAULT_CONFIG.PLAYER_STATS_ALLOW_OUT_OF_COHORT_FALLBACK, true)
    ),
    RUN_HEALTH_CONSECUTIVE_RUN_DEGRADED_TRIGGER: Math.max(1, toNumber_(
      config.RUN_HEALTH_CONSECUTIVE_RUN_DEGRADED_TRIGGER,
      toNumber_(DEFAULT_CONFIG.RUN_HEALTH_CONSECUTIVE_RUN_DEGRADED_TRIGGER, 3)
    )),
    RUN_HEALTH_SINGLE_RUN_CRITICAL_TRIGGER: toBoolean_(
      config.RUN_HEALTH_SINGLE_RUN_CRITICAL_TRIGGER,
      toBoolean_(DEFAULT_CONFIG.RUN_HEALTH_SINGLE_RUN_CRITICAL_TRIGGER, true)
    ),
    EXPORT_AUTO_ZIP_RETENTION_DAYS: Math.max(0, toNumber_(
      config.EXPORT_AUTO_ZIP_RETENTION_DAYS,
      toNumber_(DEFAULT_CONFIG.EXPORT_AUTO_ZIP_RETENTION_DAYS, 30)
    )),
  };
}

function preflightConfigUniqueness_(context) {
  const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(SHEETS.CONFIG);
  if (!sheet) {
    return {
      ok: false,
      reason_code: 'config_sheet_missing_preflight',
      duplicate_keys: [],
      message: 'Config sheet is missing. Run setup/reset to recreate managed tabs before running pipeline.',
      user_message: 'Config sheet is missing.\n\nRun "Setup / Verify Tabs" (or "Re-create / Reset Workbook") once, then retry.',
    };
  }

  const values = sheet.getDataRange().getValues();
  const parsed = parseConfigRows_(values, {
    mode: 'warn_last_wins',
    context: context || 'preflightConfigUniqueness_',
    logger: function () {},
  });

  if (!parsed.duplicate_keys.length) {
    return {
      ok: true,
      reason_code: 'config_unique_ok',
      message: 'Config uniqueness preflight passed.',
      duplicate_keys: [],
    };
  }

  return {
    ok: false,
    reason_code: 'config_duplicate_keys_preflight',
    duplicate_keys: parsed.duplicate_keys.slice().sort(),
    message: 'Config has duplicate keys. Run dedupeConfigSheet_() (or the "Repair Config (dedupe)" menu action) once, then retry.',
    user_message: 'Config has duplicate keys, so runtime behavior is ambiguous.\n\n'
      + 'Recommended fix: run "Repair Config (dedupe)" once, then run pipeline again.',
  };
}

function parseConfigRows_(values, options) {
  const opts = options || {};
  const mode = String(opts.mode || 'error').toLowerCase();
  const context = String(opts.context || 'config');
  const logger = typeof opts.logger === 'function' ? opts.logger : function () {};
  const config = {};
  const duplicateRowsByKey = {};
  const firstRowByKey = {};

  for (let i = 1; i < (values || []).length; i += 1) {
    const key = String(values[i][0] || '').trim();
    if (!key) continue;

    if (!Object.prototype.hasOwnProperty.call(firstRowByKey, key)) {
      firstRowByKey[key] = i + 1;
    }

    if (Object.prototype.hasOwnProperty.call(config, key)) {
      if (!duplicateRowsByKey[key]) duplicateRowsByKey[key] = [];
      duplicateRowsByKey[key].push(i + 1);
      if (mode === 'warn_last_wins') {
        logger('[Config] Duplicate key "' + key + '" detected at row ' + (i + 1) + '; applying last-wins precedence.');
      }
    }

    if (!Object.prototype.hasOwnProperty.call(config, key) || mode === 'warn_last_wins') {
      config[key] = values[i][1];
    }
  }

  const duplicateKeys = Object.keys(duplicateRowsByKey);
  if (duplicateKeys.length && mode === 'error') {
    throw new Error(formatDuplicateConfigKeysError_(context, duplicateRowsByKey, firstRowByKey));
  }

  return {
    config: config,
    duplicate_keys: duplicateKeys,
    duplicate_rows_by_key: duplicateRowsByKey,
  };
}

function formatDuplicateConfigKeysError_(context, duplicateRowsByKey, firstRowByKey) {
  const duplicateKeyCount = Object.keys(duplicateRowsByKey).length;
  const details = Object.keys(duplicateRowsByKey)
    .sort()
    .map(function (key) {
      return key
        + ' (first row: ' + Number((firstRowByKey || {})[key] || 0)
        + '; duplicate rows: ' + duplicateRowsByKey[key].join(', ')
        + ')';
    })
    .join('; ');
  return '[Config] Duplicate keys detected while ' + context
    + ' (' + duplicateKeyCount + ' duplicate key(s)): ' + details + '. '
    + 'Why this fails: duplicate keys make runtime config resolution ambiguous. '
    + 'How to fix safely: run dedupeConfigSheet_() exactly once from the menu or Apps Script editor.';
}

function normalizePlayerStatsCohortMode_(raw) {
  const mode = String(raw || '').trim().toLowerCase();
  if (mode === 'leadersource' || mode === 'top100' || mode === 'all') return mode;
  return String(DEFAULT_CONFIG.PLAYER_STATS_COHORT_MODE || 'leadersource').toLowerCase();
}


function normalizeLogProfile_(value) {
  const normalized = String(value || DEFAULT_CONFIG.LOG_PROFILE || 'compact').toLowerCase();
  return normalized === 'verbose' ? 'verbose' : 'compact';
}

function normalizeInvariantEnforcementLevel_(value) {
  const normalized = String(value || DEFAULT_CONFIG.INVARIANT_ENFORCEMENT_LEVEL || 'warn').toLowerCase();
  return normalized === 'strict' ? 'strict' : 'warn';
}
