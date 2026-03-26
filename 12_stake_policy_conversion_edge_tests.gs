function testEvaluateSignalStakePolicy_conversionAndAdjustmentEdgeCases_tableDriven_() {
  const baseConfig = {
    STAKE_POLICY_MODE: 'strict_suppress_below_min',
    ACCOUNT_CURRENCY: 'MXN',
    MIN_STAKE_PER_CURRENCY_JSON: '{"MXN":20}',
    MAX_STAKE_PER_CURRENCY_JSON: '{}',
    MAX_BET_MXN: 1000,
  };

  const cases = [
    {
      id: 'positive_120_to_risk_pass',
      stakeUnits: 0.2,
      odds: 120,
      expected: {
        decision: 'passed',
        reason_code: 'stake_policy_pass',
        reason_codes: ['stake_policy_pass'],
        stake_mode: 'to_risk',
        raw_target_win_mxn: null,
        raw_risk_mxn: 20,
        final_risk_mxn: 20,
        final_units: 0.2,
      },
    },
    {
      id: 'positive_180_to_risk_bucket_down',
      stakeUnits: 0.21,
      odds: 180,
      expected: {
        decision: 'adjusted',
        reason_code: 'stake_bucket_rounded_down',
        reason_codes: ['stake_bucket_rounded_down'],
        stake_mode: 'to_risk',
        raw_target_win_mxn: null,
        raw_risk_mxn: 21,
        final_risk_mxn: 20,
        final_units: 0.2,
      },
    },
    {
      id: 'negative_110_to_win_risk_conversion',
      stakeUnits: 0.2,
      odds: -110,
      expected: {
        decision: 'adjusted',
        reason_code: 'stake_bucket_rounded_down',
        reason_codes: ['stake_bucket_rounded_down'],
        stake_mode: 'to_win',
        raw_target_win_mxn: 20,
        raw_risk_mxn: 22,
        final_risk_mxn: 20,
        final_units: 0.2,
      },
    },
    {
      id: 'negative_150_to_win_risk_conversion',
      stakeUnits: 0.2,
      odds: -150,
      expected: {
        decision: 'adjusted',
        reason_code: 'stake_bucket_rounded_up',
        reason_codes: ['stake_bucket_rounded_up'],
        stake_mode: 'to_win',
        raw_target_win_mxn: 20,
        raw_risk_mxn: 30,
        final_risk_mxn: 40,
        final_units: 0.4,
      },
    },
    {
      id: 'negative_250_to_win_risk_conversion',
      stakeUnits: 0.2,
      odds: -250,
      expected: {
        decision: 'adjusted',
        reason_code: 'stake_bucket_rounded_up',
        reason_codes: ['stake_bucket_rounded_up'],
        stake_mode: 'to_win',
        raw_target_win_mxn: 20,
        raw_risk_mxn: 50,
        final_risk_mxn: 60,
        final_units: 0.6,
      },
    },
    {
      id: 'min_bet_floor_20_mxn',
      stakeUnits: 0.1,
      odds: 120,
      expected: {
        decision: 'adjusted',
        reason_code: 'stake_raised_to_min',
        reason_codes: ['stake_raised_to_min'],
        stake_mode: 'to_risk',
        raw_target_win_mxn: null,
        raw_risk_mxn: 10,
        final_risk_mxn: 20,
        final_units: 0.2,
      },
    },
    {
      id: 'bucket_boundary_19',
      stakeUnits: 0.19,
      odds: 120,
      expected: {
        decision: 'adjusted',
        reason_code: 'stake_raised_to_min',
        reason_codes: ['stake_raised_to_min'],
        stake_mode: 'to_risk',
        raw_risk_mxn: 19,
        final_risk_mxn: 20,
        final_units: 0.2,
      },
    },
    {
      id: 'bucket_boundary_20',
      stakeUnits: 0.2,
      odds: 120,
      expected: {
        decision: 'passed',
        reason_code: 'stake_policy_pass',
        reason_codes: ['stake_policy_pass'],
        stake_mode: 'to_risk',
        raw_risk_mxn: 20,
        final_risk_mxn: 20,
        final_units: 0.2,
      },
    },
    {
      id: 'bucket_boundary_21',
      stakeUnits: 0.21,
      odds: 120,
      expected: {
        decision: 'adjusted',
        reason_code: 'stake_bucket_rounded_down',
        reason_codes: ['stake_bucket_rounded_down'],
        stake_mode: 'to_risk',
        raw_risk_mxn: 21,
        final_risk_mxn: 20,
        final_units: 0.2,
      },
    },
    {
      id: 'bucket_boundary_39',
      stakeUnits: 0.39,
      odds: 120,
      expected: {
        decision: 'adjusted',
        reason_code: 'stake_bucket_rounded_up',
        reason_codes: ['stake_bucket_rounded_up'],
        stake_mode: 'to_risk',
        raw_risk_mxn: 39,
        final_risk_mxn: 40,
        final_units: 0.4,
      },
    },
    {
      id: 'bucket_boundary_40',
      stakeUnits: 0.4,
      odds: 120,
      expected: {
        decision: 'passed',
        reason_code: 'stake_policy_pass',
        reason_codes: ['stake_policy_pass'],
        stake_mode: 'to_risk',
        raw_risk_mxn: 40,
        final_risk_mxn: 40,
        final_units: 0.4,
      },
    },
    {
      id: 'bucket_boundary_41',
      stakeUnits: 0.41,
      odds: 120,
      expected: {
        decision: 'adjusted',
        reason_code: 'stake_bucket_rounded_down',
        reason_codes: ['stake_bucket_rounded_down'],
        stake_mode: 'to_risk',
        raw_risk_mxn: 41,
        final_risk_mxn: 40,
        final_units: 0.4,
      },
    },
    {
      id: 'invalid_odds_defaults_to_to_risk',
      stakeUnits: 0.35,
      odds: 'not-a-number',
      expected: {
        decision: 'adjusted',
        reason_code: 'stake_bucket_rounded_up',
        reason_codes: ['stake_bucket_rounded_up'],
        stake_mode: 'to_risk',
        raw_target_win_mxn: null,
        raw_risk_mxn: 35,
        final_risk_mxn: 40,
        final_units: 0.4,
      },
    },
    {
      id: 'missing_odds_defaults_to_to_risk',
      stakeUnits: 0.45,
      odds: null,
      expected: {
        decision: 'adjusted',
        reason_code: 'stake_bucket_rounded_down',
        reason_codes: ['stake_bucket_rounded_down'],
        stake_mode: 'to_risk',
        raw_target_win_mxn: null,
        raw_risk_mxn: 45,
        final_risk_mxn: 40,
        final_units: 0.4,
      },
    },
  ];

  cases.forEach(function (testCase) {
    const decision = evaluateSignalStakePolicy_(testCase.stakeUnits, testCase.odds, baseConfig);
    const expected = testCase.expected;

    assertEquals_(expected.decision, decision.decision, testCase.id + ': decision');
    assertEquals_(expected.reason_code, decision.reason_code, testCase.id + ': reason_code');
    assertEquals_(JSON.stringify(expected.reason_codes), JSON.stringify(decision.reason_codes || []), testCase.id + ': reason_codes');
    assertEquals_(expected.stake_mode, decision.stake_mode, testCase.id + ': stake_mode');
    if (Object.prototype.hasOwnProperty.call(expected, 'raw_target_win_mxn')) {
      assertEquals_(expected.raw_target_win_mxn, decision.raw_target_win_mxn, testCase.id + ': raw_target_win_mxn');
    }
    assertEquals_(expected.raw_risk_mxn, decision.raw_risk_mxn, testCase.id + ': raw_risk_mxn');
    assertEquals_(expected.final_risk_mxn, decision.final_risk_mxn, testCase.id + ': final_risk_mxn');
    assertEquals_(expected.final_units, decision.final_units, testCase.id + ': final_units');

    const expectedUnitsFromRisk = roundHalfUp_(decision.final_risk_mxn / 100, 4);
    assertEquals_(expectedUnitsFromRisk, decision.final_units, testCase.id + ': unit conversion MXN/100');
  });
}

function testEvaluateSignalStakePolicy_missingStakeSuggestion_withOddsVariants_() {
  const config = {
    STAKE_POLICY_MODE: 'strict_suppress_below_min',
    ACCOUNT_CURRENCY: 'MXN',
    MIN_STAKE_PER_CURRENCY_JSON: '{"MXN":20}',
    MAX_STAKE_PER_CURRENCY_JSON: '{}',
    MAX_BET_MXN: 1000,
  };

  const missingCases = [
    { id: 'missing_stake_positive_odds', odds: 130, stakeUnits: undefined, expected_mode: 'to_risk' },
    { id: 'missing_stake_negative_odds', odds: -110, stakeUnits: undefined, expected_mode: 'to_win' },
    { id: 'missing_stake_invalid_odds', odds: 'bad', stakeUnits: undefined, expected_mode: 'to_risk' },
    { id: 'missing_stake_no_odds', odds: null, stakeUnits: undefined, expected_mode: 'to_risk' },
  ];

  missingCases.forEach(function (testCase) {
    const decision = evaluateSignalStakePolicy_(testCase.stakeUnits, testCase.odds, config);
    assertEquals_('missing_stake', decision.decision, testCase.id + ': decision');
    assertEquals_('stake_missing_unscored', decision.reason_code, testCase.id + ': reason_code');
    assertEquals_(
      JSON.stringify(['stake_missing_unscored']),
      JSON.stringify(decision.reason_codes || []),
      testCase.id + ': reason_codes'
    );
    assertEquals_(testCase.expected_mode, decision.stake_mode_used, testCase.id + ': stake_mode_used');
    assertEquals_(null, decision.final_risk_mxn, testCase.id + ': final_risk_mxn');
    assertEquals_(null, decision.final_units, testCase.id + ': final_units');
  });
}
