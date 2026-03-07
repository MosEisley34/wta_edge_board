function testResolveActiveWtaSportKeys_returnsCatalogKeysWhenPresent_() {
  const calls = { cacheSet: null, logs: [] };
  const config = {
    ODDS_CACHE_TTL_SEC: 180,
    ODDS_SPORT_KEY: 'tennis_wta',
    ODDS_API_BASE_URL: 'https://api.the-odds-api.com/v4',
    ODDS_API_KEY: 'test',
  };

  const result = resolveActiveWtaSportKeys_(config, {
    getCachedOddsSportKeys: function () { return []; },
    setCachedOddsSportKeys: function (cacheKey, keys, ttlSec) { calls.cacheSet = { cacheKey: cacheKey, keys: keys, ttlSec: ttlSec }; },
    callOddsApi: function () {
      return {
        ok: true,
        payload: [
          { key: 'tennis_wta_us_open', active: true },
          { key: 'tennis_wta_wimbledon', active: true },
          { key: 'tennis_atp_us_open', active: true },
        ],
      };
    },
    logOddsSportKeyResolution: function (mode, keys, fallback) { calls.logs.push({ mode: mode, keys: keys, fallback: fallback }); },
  });

  assertEquals_('catalog', result.source);
  assertEquals_('none', result.fallback);
  assertArrayEquals_(['tennis_wta_us_open', 'tennis_wta_wimbledon'], result.sport_keys);
  assertEquals_('ODDS_ACTIVE_WTA_SPORT_KEYS', calls.cacheSet.cacheKey);
  assertEquals_(180, calls.cacheSet.ttlSec);
}

function testResolveActiveWtaSportKeys_fallsBackWhenAbsent_() {
  const config = {
    ODDS_CACHE_TTL_SEC: 300,
    ODDS_SPORT_KEY: '',
    ODDS_API_BASE_URL: 'https://api.the-odds-api.com/v4',
    ODDS_API_KEY: 'test',
  };

  const result = resolveActiveWtaSportKeys_(config, {
    getCachedOddsSportKeys: function () { return []; },
    setCachedOddsSportKeys: function () {},
    callOddsApi: function () {
      return {
        ok: true,
        payload: [
          { key: 'soccer_epl', active: true },
          { key: 'tennis_atp_us_open', active: true },
        ],
      };
    },
    logOddsSportKeyResolution: function () {},
  });

  assertEquals_('fallback', result.source);
  assertEquals_('none_active_wta_keys', result.fallback);
  assertArrayEquals_(['UNKNOWN_SPORT'], result.sport_keys);
}

function testResolveActiveWtaSportKeys_ignoresInactiveWtaKeys_() {
  const config = {
    ODDS_CACHE_TTL_SEC: 300,
    ODDS_SPORT_KEY: 'wta_manual_fallback',
    ODDS_API_BASE_URL: 'https://api.the-odds-api.com/v4',
    ODDS_API_KEY: 'test',
  };

  const result = resolveActiveWtaSportKeys_(config, {
    getCachedOddsSportKeys: function () { return []; },
    setCachedOddsSportKeys: function () {},
    callOddsApi: function () {
      return {
        ok: true,
        payload: [
          { key: 'tennis_wta_us_open', active: false },
          { key: 'tennis_wta_wimbledon', active: false },
        ],
      };
    },
    logOddsSportKeyResolution: function () {},
  });

  assertEquals_('fallback', result.source);
  assertEquals_('none_active_wta_keys', result.fallback);
  assertArrayEquals_(['wta_manual_fallback'], result.sport_keys);
}

function assertEquals_(expected, actual) {
  if (expected !== actual) {
    throw new Error('Assertion failed. Expected: ' + expected + ', actual: ' + actual);
  }
}

function assertArrayEquals_(expected, actual) {
  const left = JSON.stringify(expected || []);
  const right = JSON.stringify(actual || []);
  if (left !== right) {
    throw new Error('Assertion failed. Expected array: ' + left + ', actual array: ' + right);
  }
}
