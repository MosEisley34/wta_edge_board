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
  SCHEDULE_BUFFER_BEFORE_MIN: '180',
  SCHEDULE_BUFFER_AFTER_MIN: '180',
  MATCH_TIME_TOLERANCE_MIN: '45',
  MATCH_FALLBACK_EXPANSION_MIN: '120',
  ALLOW_WTA_125: 'false',
  ALLOW_WTA_250: 'true',
  COMPETITION_SOURCE_FIELDS_JSON: '["competition","tournament","event_name","sport_title","home_team","away_team"]',
  GRAND_SLAM_ALIASES_JSON: '["australian open","roland garros","french open","wimbledon","us open"]',
  WTA_1000_ALIASES_JSON: '["wta 1000","wta-1000","masters 1000","wta indian wells","indian wells"]',
  WTA_500_ALIASES_JSON: '["wta 500","wta-500"]',
  COMPETITION_DENY_ALIASES_JSON: '["wta 125","wta125","wta 250","wta250","itf"]',
  GRAND_SLAM_ALIAS_MAP_JSON: '{}',
  WTA_500_ALIAS_MAP_JSON: '{}',
  WTA_1000_ALIAS_MAP_JSON: '{"WTA_1000":["wta indian wells","indian wells","wta-1000","wta 1000","masters 1000"]}',
  COMPETITION_DENY_ALIAS_MAP_JSON: '{"WTA_125":["wta 125","wta125"],"WTA_250":["wta 250","wta250"],"ITF":["itf"]}',
  VERBOSE_LOGGING: 'true',
  LOG_VERBOSITY_LEVEL: '2',
  DUPLICATE_DEBOUNCE_MS: '90000',
  PIPELINE_TRIGGER_EVERY_MIN: '15',
  PLAYER_ALIAS_MAP_JSON: '{}',
  MODEL_VERSION: 'wta_mvp_playerstats_v2',
  EDGE_THRESHOLD_MICRO: '0.015',
  EDGE_THRESHOLD_SMALL: '0.03',
  EDGE_THRESHOLD_MED: '0.05',
  EDGE_THRESHOLD_STRONG: '0.08',
  STAKE_UNITS_MICRO: '0.25',
  STAKE_UNITS_SMALL: '0.5',
  STAKE_UNITS_MED: '1',
  STAKE_UNITS_STRONG: '1.5',
  SIGNAL_COOLDOWN_MIN: '180',
  MINUTES_BEFORE_START_CUTOFF: '60',
  STALE_ODDS_WINDOW_MIN: '60',
  ODDS_MIN_CREDITS_SOFT_LIMIT: '50',
  ODDS_MIN_CREDITS_HARD_LIMIT: '10',
  DISCORD_WEBHOOK: '',
  NOTIFY_ENABLED: 'true',
  NOTIFY_TEST_MODE: 'false',
  PLAYER_STATS_API_BASE_URL: '',
  PLAYER_STATS_API_KEY: '',
  PLAYER_STATS_SCRAPE_URLS: '',
  PLAYER_STATS_PROVIDER_MODE: 'tennisabstract_leaders',
  PLAYER_STATS_TA_LEADERS_URL: 'https://www.tennisabstract.com/cgi-bin/leaders_wta.cgi?players=top',
  PLAYER_STATS_TA_H2H_URL: 'https://tennisabstract.com/reports/h2hMatrixWta.html',
  PLAYER_STATS_FETCH_USER_AGENT: 'Mozilla/5.0 (compatible; WTA-Edge-Board/1.0)',
  PLAYER_STATS_FETCH_TIMEOUT_MS: '30000',
  PLAYER_STATS_FETCH_MAX_RETRIES: '3',
  PLAYER_STATS_FETCH_BACKOFF_BASE_MS: '300',
  PLAYER_STATS_FETCH_BACKOFF_MAX_MS: '4000',
  PLAYER_STATS_TA_REQUEST_DELAY_MS: '300',
  PLAYER_STATS_TA_REQUEST_JITTER_MS: '250',
  PLAYER_STATS_MAX_ROWS_PER_RUN: '200',
  PLAYER_STATS_ENABLE_H2H: 'true',
  PLAYER_STATS_ENABLE_RICH_STATS: 'true',
  PLAYER_STATS_CACHE_TTL_MIN: '10',
  PLAYER_STATS_REFRESH_MIN: '5',
  PLAYER_STATS_FORCE_REFRESH: 'false',
};


const RUN_LOG_HEADERS = [
  'row_type', 'run_id', 'stage', 'started_at', 'ended_at', 'status', 'reason_code', 'message',
  'fetched_odds', 'fetched_schedule', 'allowed_tournaments', 'matched', 'unmatched', 'signals_found',
  'rejection_codes', 'cooldown_suppressed', 'duplicate_suppressed',
  'lock_event', 'debounce_event', 'trigger_event', 'exception', 'stack', 'stage_summaries',
];

const PROPS = {
  PIPELINE_TRIGGER_SIGNATURE: 'PIPELINE_TRIGGER_SIGNATURE',
  LAST_PIPELINE_RUN_TS: 'LAST_PIPELINE_RUN_TS',
  DUPLICATE_PREVENTED_COUNT: 'DUPLICATE_PREVENTED_COUNT',
};

const TIMESTAMP_TIMEZONE = {
  ID: 'America/Hermosillo',
  OFFSET: '-07:00',
};

function getConfig_() {
  const sh = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(SHEETS.CONFIG);
  const values = sh.getDataRange().getValues();
  const parsed = parseConfigRows_(values, {
    mode: 'error',
    context: 'getConfig_',
  });
  const config = parsed.config;

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
    SCHEDULE_BUFFER_BEFORE_MIN: toNumber_(config.SCHEDULE_BUFFER_BEFORE_MIN, 180),
    SCHEDULE_BUFFER_AFTER_MIN: toNumber_(config.SCHEDULE_BUFFER_AFTER_MIN, 180),
    MATCH_TIME_TOLERANCE_MIN: toNumber_(config.MATCH_TIME_TOLERANCE_MIN, 45),
    MATCH_FALLBACK_EXPANSION_MIN: toNumber_(config.MATCH_FALLBACK_EXPANSION_MIN, 120),
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
    VERBOSE_LOGGING: toBoolean_(config.VERBOSE_LOGGING, true),
    LOG_VERBOSITY_LEVEL: Math.max(0, Math.min(3, toNumber_(config.LOG_VERBOSITY_LEVEL, toBoolean_(config.VERBOSE_LOGGING, true) ? 2 : 1))),
    DUPLICATE_DEBOUNCE_MS: toNumber_(config.DUPLICATE_DEBOUNCE_MS, 90000),
    PIPELINE_TRIGGER_EVERY_MIN: Math.max(1, toNumber_(config.PIPELINE_TRIGGER_EVERY_MIN, 15)),
    PLAYER_ALIAS_MAP_JSON: String(config.PLAYER_ALIAS_MAP_JSON || '{}'),
    MODEL_VERSION: String(config.MODEL_VERSION || 'wta_mvp_playerstats_v2'),
    EDGE_THRESHOLD_MICRO: toNumber_(config.EDGE_THRESHOLD_MICRO, 0.015),
    EDGE_THRESHOLD_SMALL: toNumber_(config.EDGE_THRESHOLD_SMALL, 0.03),
    EDGE_THRESHOLD_MED: toNumber_(config.EDGE_THRESHOLD_MED, 0.05),
    EDGE_THRESHOLD_STRONG: toNumber_(config.EDGE_THRESHOLD_STRONG, 0.08),
    STAKE_UNITS_MICRO: toNumber_(config.STAKE_UNITS_MICRO, 0.25),
    STAKE_UNITS_SMALL: toNumber_(config.STAKE_UNITS_SMALL, 0.5),
    STAKE_UNITS_MED: toNumber_(config.STAKE_UNITS_MED, 1),
    STAKE_UNITS_STRONG: toNumber_(config.STAKE_UNITS_STRONG, 1.5),
    SIGNAL_COOLDOWN_MIN: toNumber_(config.SIGNAL_COOLDOWN_MIN, 180),
    MINUTES_BEFORE_START_CUTOFF: toNumber_(config.MINUTES_BEFORE_START_CUTOFF, 60),
    STALE_ODDS_WINDOW_MIN: toNumber_(config.STALE_ODDS_WINDOW_MIN, 60),
    ODDS_MIN_CREDITS_SOFT_LIMIT: toNumber_(config.ODDS_MIN_CREDITS_SOFT_LIMIT, 50),
    ODDS_MIN_CREDITS_HARD_LIMIT: toNumber_(config.ODDS_MIN_CREDITS_HARD_LIMIT, 10),
    DISCORD_WEBHOOK: String(config.DISCORD_WEBHOOK || ''),
    NOTIFY_ENABLED: toBoolean_(config.NOTIFY_ENABLED, true),
    NOTIFY_TEST_MODE: toBoolean_(config.NOTIFY_TEST_MODE, false),
    PLAYER_STATS_API_BASE_URL: String(config.PLAYER_STATS_API_BASE_URL || ''),
    PLAYER_STATS_API_KEY: String(config.PLAYER_STATS_API_KEY || ''),
    PLAYER_STATS_SCRAPE_URLS: String(config.PLAYER_STATS_SCRAPE_URLS || ''),
    PLAYER_STATS_PROVIDER_MODE: String(config.PLAYER_STATS_PROVIDER_MODE || DEFAULT_CONFIG.PLAYER_STATS_PROVIDER_MODE),
    PLAYER_STATS_TA_LEADERS_URL: String(config.PLAYER_STATS_TA_LEADERS_URL || DEFAULT_CONFIG.PLAYER_STATS_TA_LEADERS_URL),
    PLAYER_STATS_TA_H2H_URL: String(config.PLAYER_STATS_TA_H2H_URL || DEFAULT_CONFIG.PLAYER_STATS_TA_H2H_URL),
    PLAYER_STATS_FETCH_USER_AGENT: String(config.PLAYER_STATS_FETCH_USER_AGENT || DEFAULT_CONFIG.PLAYER_STATS_FETCH_USER_AGENT),
    PLAYER_STATS_FETCH_TIMEOUT_MS: toNumber_(config.PLAYER_STATS_FETCH_TIMEOUT_MS, 30000),
    PLAYER_STATS_FETCH_MAX_RETRIES: Math.max(0, toNumber_(config.PLAYER_STATS_FETCH_MAX_RETRIES, 3)),
    PLAYER_STATS_FETCH_BACKOFF_BASE_MS: Math.max(0, toNumber_(config.PLAYER_STATS_FETCH_BACKOFF_BASE_MS, 300)),
    PLAYER_STATS_FETCH_BACKOFF_MAX_MS: Math.max(0, toNumber_(config.PLAYER_STATS_FETCH_BACKOFF_MAX_MS, 4000)),
    PLAYER_STATS_TA_REQUEST_DELAY_MS: Math.max(0, toNumber_(config.PLAYER_STATS_TA_REQUEST_DELAY_MS, 300)),
    PLAYER_STATS_TA_REQUEST_JITTER_MS: Math.max(0, toNumber_(config.PLAYER_STATS_TA_REQUEST_JITTER_MS, 250)),
    PLAYER_STATS_MAX_ROWS_PER_RUN: Math.max(1, toNumber_(config.PLAYER_STATS_MAX_ROWS_PER_RUN, 200)),
    PLAYER_STATS_ENABLE_H2H: toBoolean_(config.PLAYER_STATS_ENABLE_H2H, true),
    PLAYER_STATS_ENABLE_RICH_STATS: toBoolean_(config.PLAYER_STATS_ENABLE_RICH_STATS, true),
    PLAYER_STATS_CACHE_TTL_MIN: toNumber_(config.PLAYER_STATS_CACHE_TTL_MIN, 10),
    PLAYER_STATS_REFRESH_MIN: toNumber_(config.PLAYER_STATS_REFRESH_MIN, 5),
    PLAYER_STATS_FORCE_REFRESH: toBoolean_(config.PLAYER_STATS_FORCE_REFRESH, false),
  };
}

function preflightConfigUniqueness_(context) {
  const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(SHEETS.CONFIG);
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
