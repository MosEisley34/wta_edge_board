function testEvaluateSignalStakePolicy_appliesPositiveOddsAsRiskMode_() {
  const config = {
    STAKE_POLICY_MODE: 'strict_suppress_below_min',
    ACCOUNT_CURRENCY: 'MXN',
    MIN_STAKE_PER_CURRENCY_JSON: '{"MXN":20}',
  };
  const decision = evaluateSignalStakePolicy_(0.5, 145, config);

  assertEquals_('passed', decision.decision);
  assertEquals_('stake_policy_pass', decision.reason_code);
  assertEquals_(50, decision.final_risk_mxn);
  assertEquals_(0.5, decision.final_units);
  assertEquals_('to_risk', decision.stake_mode);
}

function testEvaluateSignalStakePolicy_appliesNegativeOddsAsToWinModeAndAdjustments_() {
  const config = {
    STAKE_POLICY_MODE: 'strict_suppress_below_min',
    ACCOUNT_CURRENCY: 'MXN',
    MIN_STAKE_PER_CURRENCY_JSON: '{"MXN":20}',
    MAX_BET_MXN: 120,
  };
  const decision = evaluateSignalStakePolicy_(0.31, -137, config);

  assertEquals_('adjusted', decision.decision);
  assertEquals_('stake_bucket_rounded_down', decision.reason_code);
  assertEquals_(40, decision.final_risk_mxn);
  assertEquals_(0.4, decision.final_units);
  assertEquals_(
    JSON.stringify(['stake_bucket_rounded_down']),
    JSON.stringify(decision.reason_codes || [])
  );
  assertEquals_('to_win', decision.stake_mode);
}

function testEvaluateSignalStakePolicy_raisesMinAndCapsAtMax_() {
  const config = {
    STAKE_POLICY_MODE: 'strict_suppress_below_min',
    ACCOUNT_CURRENCY: 'MXN',
    MIN_STAKE_PER_CURRENCY_JSON: '{"MXN":20}',
    MAX_BET_MXN: 20,
  };
  const decision = evaluateSignalStakePolicy_(0.1, -110, config);

  assertEquals_('adjusted', decision.decision);
  assertEquals_(20, decision.final_risk_mxn);
  assertEquals_(0.2, decision.final_units);
  assertEquals_(
    JSON.stringify(['stake_raised_to_min']),
    JSON.stringify(decision.reason_codes || [])
  );
}

function testEvaluateSignalStakePolicy_missingSuggestionMaintainsExistingReasonCode_() {
  const config = {
    STAKE_POLICY_MODE: 'strict_suppress_below_min',
    ACCOUNT_CURRENCY: 'MXN',
    MIN_STAKE_PER_CURRENCY_JSON: '{"MXN":20}',
  };
  const decision = evaluateSignalStakePolicy_(null, 130, config);

  assertEquals_('missing_stake', decision.decision);
  assertEquals_('stake_missing_unscored', decision.reason_code);
  assertEquals_(null, decision.final_risk_mxn);
}

function testEvaluateSignalStakePolicy_appliesCapReasonCodeWhenConfigured_() {
  const config = {
    STAKE_POLICY_MODE: 'strict_suppress_below_min',
    ACCOUNT_CURRENCY: 'MXN',
    MIN_STAKE_PER_CURRENCY_JSON: '{"MXN":20}',
    MAX_BET_MXN: 120,
  };
  const decision = evaluateSignalStakePolicy_(2.0, 130, config);

  assertEquals_('adjusted', decision.decision);
  assertEquals_('stake_capped_to_max', decision.reason_code);
  assertEquals_(120, decision.final_risk_mxn);
  assertEquals_(1.2, decision.final_units);
}
