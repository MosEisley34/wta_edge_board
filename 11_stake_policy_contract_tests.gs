function testEvaluateSignalStakePolicy_strictMode_matchesPythonFixtureExpectations_() {
  const fixtureRows = buildStakePolicyFixtureRowsForGs_();
  const config = {
    STAKE_POLICY_MODE: 'strict_suppress_below_min',
    ACCOUNT_CURRENCY: 'MXN',
    MIN_STAKE_PER_CURRENCY_JSON: '{"MXN":20}',
  };
  const expectedByCaseId = {
    below_min_raw: 'stake_below_min_suppressed',
    decimal_19995: 'stake_below_min_suppressed',
    boundary_2000: 'stake_policy_pass',
    above_min: 'stake_policy_pass',
    message_nested_below_min: 'stake_below_min_suppressed',
    missing_stake: 'stake_missing_unscored',
  };

  const observedByCaseId = {};
  fixtureRows.forEach(function (row) {
    const stakeValue = resolveFixtureStakeValue_(row);
    observedByCaseId[row.case_id] = evaluateSignalStakePolicy_(stakeValue, config).reason_code;
  });

  assertEquals_(JSON.stringify(expectedByCaseId), JSON.stringify(observedByCaseId));
}

function testEvaluateSignalStakePolicy_roundUpMode_matchesPythonFixtureExpectations_() {
  const fixtureRows = buildStakePolicyFixtureRowsForGs_();
  const config = {
    STAKE_POLICY_MODE: 'round_up_to_min',
    ACCOUNT_CURRENCY: 'MXN',
    MIN_STAKE_PER_CURRENCY_JSON: '{"MXN":20}',
  };
  const expectedByCaseId = {
    below_min_raw: 'stake_rounded_to_min',
    decimal_19995: 'stake_rounded_to_min',
    boundary_2000: 'stake_policy_pass',
    above_min: 'stake_policy_pass',
    message_nested_below_min: 'stake_rounded_to_min',
    missing_stake: 'stake_missing_unscored',
  };

  const observedByCaseId = {};
  fixtureRows.forEach(function (row) {
    const stakeValue = resolveFixtureStakeValue_(row);
    observedByCaseId[row.case_id] = evaluateSignalStakePolicy_(stakeValue, config).reason_code;
  });

  assertEquals_(JSON.stringify(expectedByCaseId), JSON.stringify(observedByCaseId));
}

function buildStakePolicyFixtureRowsForGs_() {
  return [
    { case_id: 'below_min_raw', stake_mxn: 19.5 },
    { case_id: 'decimal_19995', proposed_stake_mxn: 19.995 },
    { case_id: 'boundary_2000', recommended_stake_mxn: 20.0 },
    { case_id: 'above_min', stake: 22 },
    { case_id: 'message_nested_below_min', message: '{"suggested_stake_mxn":18.2}' },
    { case_id: 'missing_stake', message: '{"note":"no stake emitted"}' },
  ];
}

function resolveFixtureStakeValue_(row) {
  const payload = Object.assign({}, row || {});
  if (typeof payload.message === 'string' && payload.message) {
    try {
      const parsed = JSON.parse(payload.message);
      if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
        Object.keys(parsed).forEach(function (key) {
          payload[key] = parsed[key];
        });
      }
    } catch (err) {
      // no-op for malformed fixture rows.
    }
  }
  const keys = ['stake_mxn', 'proposed_stake_mxn', 'recommended_stake_mxn', 'suggested_stake_mxn', 'stake'];
  for (let i = 0; i < keys.length; i += 1) {
    const numeric = Number(payload[keys[i]]);
    if (Number.isFinite(numeric)) return numeric;
  }
  return null;
}
