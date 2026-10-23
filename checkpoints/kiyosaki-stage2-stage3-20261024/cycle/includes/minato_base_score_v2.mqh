//+------------------------------------------------------------------+
//|                                         minato_base_score_v2.mqh |
//| MINATO_BASE_SCORE_V2.0 - isolated scorer for approved migration  |
//+------------------------------------------------------------------+
#property strict

#define MINATO_BASE_SCORE_V2_VERSION "MINATO_BASE_SCORE_V2.0"
#define MINATO_MIN_MARGIN_LEVEL 120.0

// Canonical score-only inputs. Launchers derive these from the authoritative
// [Tester] dates and attest the exact version before any MT5 launch.
input datetime InpMinatoTestStart = D'2025.01.05 00:00';
input datetime InpMinatoTestEnd = D'2026.01.05 00:00';
input string InpMinatoExpectedScoreVersion = "MINATO_BASE_SCORE_V2.0";

struct SMinatoScoreInputsV2
{
   double starting_capital;
   double ending_equity;
   double actual_weeks;
   double actual_months;
   double completed_trades;
   double max_relative_dd_pct;
   double sharpe;
   double profit_factor;
   double gross_loss;
   double minimum_margin_level;
   bool   runtime_margin_breach;
};

struct SMinatoScoreResultV2
{
   string score_version;
   double fitness;
   string diagnostic;
   string secondary_flags;

   double geometric_weekly_return_pct;
   double return_base_points;
   double return_bonus_points;
   double return_confidence;

   double completed_trades;
   double actual_tpm;
   double tpm_base_points;
   double tpm_bonus_points;

   double max_relative_dd_pct;
   double dd_points;

   double sharpe_raw;
   double sharpe_points_before_confidence;
   double profit_factor_raw;
   double pf_points_before_confidence;
   double sample_confidence;
   double adjusted_sharpe_points;
   double adjusted_pf_points;
   double minimum_margin_level;
};

datetime g_minato_v2_first_tick_time = 0;
datetime g_minato_v2_last_tick_time = 0;
double   g_minato_v2_last_tick_equity = 0.0;
double   g_minato_v2_last_tick_balance = 0.0;
bool     g_minato_v2_last_tick_had_position = false;
double   g_minato_v2_min_margin_level = DBL_MAX;
bool     g_minato_v2_runtime_margin_breach = false;
SMinatoScoreResultV2 g_minato_v2_last_score;

// These helpers are unchanged trading/scheduling behavior copied from the
// shared legacy include so migrating this EA does not alter the other 42 EAs.
bool MinatoAllowsProjectedMargin(ENUM_ORDER_TYPE order_type,
                                 double volume,
                                 double price)
{
   if(volume <= 0.0 || price <= 0.0) return false;
   double new_order_margin = 0.0;
   if(!OrderCalcMargin(order_type, _Symbol, volume, price, new_order_margin))
      return false;
   double equity = AccountInfoDouble(ACCOUNT_EQUITY);
   double used_margin = AccountInfoDouble(ACCOUNT_MARGIN) + new_order_margin;
   if(equity <= 0.0 || used_margin <= 0.0) return false;
   return ((equity / used_margin) * 100.0 >= MINATO_MIN_MARGIN_LEVEL);
}

double MinatoRegimeQuantity(double base_quantity, double regime_multiplier)
{
   if(base_quantity <= 0.0 || regime_multiplier <= 0.0) return 0.0;
   return base_quantity * regime_multiplier;
}

double MinatoTakeProfit(double entry_price,
                        double base_distance,
                        double regime_multiplier,
                        bool is_buy)
{
   if(regime_multiplier <= 0.0) return 0.0;
   double distance = base_distance * regime_multiplier;
   if(entry_price <= 0.0 || distance <= 0.0) return 0.0;
   return is_buy ? entry_price + distance : entry_price - distance;
}

int g_prev_bars = 0;

bool NewBarClosed()
{
   int bars = Bars(_Symbol, _Period);
   if(bars == g_prev_bars) return false;
   g_prev_bars = bars;
   return true;
}

bool ShouldComputeSignals()
{
   return NewBarClosed();
}

double MinatoClampV2(double value, double low, double high)
{
   return MathMin(high, MathMax(low, value));
}

void MinatoAddFlagV2(string &flags, string flag)
{
   if(flags == "") flags = flag;
   else flags += "|" + flag;
}

void MinatoInitResultV2(SMinatoScoreInputsV2 &in,
                        SMinatoScoreResultV2 &out)
{
   out.score_version = MINATO_BASE_SCORE_V2_VERSION;
   out.fitness = -1000.0;
   out.diagnostic = "NONFINITE_STAT";
   out.secondary_flags = "";
   out.geometric_weekly_return_pct = 0.0;
   out.return_base_points = 0.0;
   out.return_bonus_points = 0.0;
   out.return_confidence = 0.0;
   out.completed_trades = in.completed_trades;
   out.actual_tpm = 0.0;
   out.tpm_base_points = 0.0;
   out.tpm_bonus_points = 0.0;
   out.max_relative_dd_pct = in.max_relative_dd_pct;
   out.dd_points = 0.0;
   out.sharpe_raw = in.sharpe;
   out.sharpe_points_before_confidence = 0.0;
   out.profit_factor_raw = in.profit_factor;
   out.pf_points_before_confidence = 0.0;
   out.sample_confidence = 0.0;
   out.adjusted_sharpe_points = 0.0;
   out.adjusted_pf_points = 0.0;
   out.minimum_margin_level = in.minimum_margin_level;
}

double MinatoDDPointsV2(double dd_pct)
{
   if(dd_pct <= 5.0) return 15.0;
   if(dd_pct >= 25.0) return 0.0;
   if(dd_pct <= 7.5)  return 15.00 + (dd_pct - 5.0)  / 2.5 * (14.25 - 15.00);
   if(dd_pct <= 10.0) return 14.25 + (dd_pct - 7.5)  / 2.5 * (13.00 - 14.25);
   if(dd_pct <= 12.5) return 13.00 + (dd_pct - 10.0) / 2.5 * (11.50 - 13.00);
   if(dd_pct <= 15.0) return 11.50 + (dd_pct - 12.5) / 2.5 * (9.75 - 11.50);
   if(dd_pct <= 17.5) return 9.75  + (dd_pct - 15.0) / 2.5 * (7.75 - 9.75);
   if(dd_pct <= 20.0) return 7.75  + (dd_pct - 17.5) / 2.5 * (5.50 - 7.75);
   if(dd_pct <= 22.5) return 5.50  + (dd_pct - 20.0) / 2.5 * (2.75 - 5.50);
   return 2.75 + (dd_pct - 22.5) / 2.5 * (0.00 - 2.75);
}

// Approved MINATO_BASE_SCORE_V2.0 scorer. Normal positive range is 0..130.
// Penalty order: loss > insufficient > zero > DD > margin > invalid.
void MinatoScoreV2(SMinatoScoreInputsV2 &in, SMinatoScoreResultV2 &out)
{
   MinatoInitResultV2(in, out);

   if(!MathIsValidNumber(in.starting_capital))
   {
      out.diagnostic = "NONFINITE_STAT|INVALID_STARTING_CAPITAL";
      return;
   }
   if(in.starting_capital <= 0.0)
   {
      out.diagnostic = "INVALID_STARTING_CAPITAL";
      return;
   }
   if(!MathIsValidNumber(in.ending_equity) || in.ending_equity <= 0.0)
   {
      out.diagnostic = "INVALID_EQUITY";
      return;
   }
   if(!MathIsValidNumber(in.actual_weeks) || !MathIsValidNumber(in.actual_months) ||
      in.actual_weeks <= 0.0 || in.actual_months <= 0.0)
   {
      out.diagnostic = "INVALID_DURATION";
      return;
   }
   if(!MathIsValidNumber(in.completed_trades) || in.completed_trades < 0.0)
   {
      out.diagnostic = "NONFINITE_STAT|INVALID_TRADES";
      return;
   }
   if(!MathIsValidNumber(in.max_relative_dd_pct) ||
      in.max_relative_dd_pct < 0.0 || in.max_relative_dd_pct > 100.0)
   {
      out.diagnostic = "INVALID_DD";
      return;
   }

   double equity_ratio = in.ending_equity / in.starting_capital;
   double weekly_growth = MathPow(equity_ratio, 1.0 / in.actual_weeks);
   if(!MathIsValidNumber(equity_ratio) || equity_ratio <= 0.0 ||
      !MathIsValidNumber(weekly_growth) || weekly_growth <= 0.0)
   {
      out.diagnostic = "INVALID_EQUITY";
      return;
   }

   out.geometric_weekly_return_pct = (weekly_growth - 1.0) * 100.0;
   out.actual_tpm = in.completed_trades / in.actual_months;
   if(!MathIsValidNumber(out.geometric_weekly_return_pct) ||
      !MathIsValidNumber(out.actual_tpm) || out.actual_tpm < 0.0)
   {
      out.diagnostic = "NONFINITE_STAT";
      out.fitness = -1000.0;
      return;
   }

   // Non-core statistics never create positive credit when abnormal.
   bool sharpe_valid = MathIsValidNumber(in.sharpe);
   bool pf_valid = MathIsValidNumber(in.profit_factor);
   bool gross_loss_valid = MathIsValidNumber(in.gross_loss);
   bool zero_gross_loss = gross_loss_valid && MathAbs(in.gross_loss) <= 1.0e-12;
   if(!sharpe_valid) MinatoAddFlagV2(out.secondary_flags, "NONFINITE_SHARPE");
   if(!pf_valid) MinatoAddFlagV2(out.secondary_flags, "NONFINITE_PF");
   if(!gross_loss_valid) MinatoAddFlagV2(out.secondary_flags, "NONFINITE_GROSS_LOSS");
   if(zero_gross_loss) MinatoAddFlagV2(out.secondary_flags, "PF_ZERO_GROSS_LOSS");

   if(sharpe_valid)
      out.sharpe_points_before_confidence = 5.0 * MinatoClampV2(in.sharpe / 3.0, 0.0, 1.0);
   if(pf_valid && gross_loss_valid && !zero_gross_loss)
      out.pf_points_before_confidence = 5.0 * MinatoClampV2((in.profit_factor - 1.0) / 2.0, 0.0, 1.0);

   // DD breach outranks zero/insufficient sample if corrupt combinations coexist.
   if(in.max_relative_dd_pct > 25.0)
   {
      double dd_penalty = -150.0 - 2.0 * (in.max_relative_dd_pct - 25.0);
      out.fitness = MathMax(-249.0, dd_penalty);
      out.diagnostic = "DD_BREACH";
      return;
   }

   // Zero trades has no meaningful tester margin statistic.
   if(in.completed_trades == 0.0)
   {
      out.fitness = -120.0;
      out.diagnostic = "ZERO_TRADES";
      return;
   }

   if(!MathIsValidNumber(in.minimum_margin_level))
   {
      out.fitness = -1000.0;
      out.diagnostic = "NONFINITE_STAT|INVALID_MARGIN";
      return;
   }
   if(in.runtime_margin_breach || in.minimum_margin_level < MINATO_MIN_MARGIN_LEVEL)
   {
      double margin_penalty = -250.0 - MathMax(0.0, MINATO_MIN_MARGIN_LEVEL - in.minimum_margin_level);
      out.fitness = MinatoClampV2(margin_penalty, -999.0, -250.0);
      out.diagnostic = "MARGIN_BREACH";
      return;
   }

   if(in.completed_trades < 10.0)
   {
      out.fitness = -110.0 + (in.completed_trades - 1.0) * (9.0 / 8.0);
      out.diagnostic = "INSUFFICIENT_TRADES";
      return;
   }

   if(out.geometric_weekly_return_pct < 0.0)
   {
      double loss_fitness = -10.0 - 10.0 * MathAbs(out.geometric_weekly_return_pct);
      out.fitness = MathMax(-100.0, loss_fitness);
      out.diagnostic = "VALID_LOSS";
      return;
   }
   if(out.geometric_weekly_return_pct == 0.0)
   {
      out.fitness = 0.0;
      out.diagnostic = "VALID_FLAT";
      return;
   }

   out.return_base_points = 45.0 * MinatoClampV2(out.geometric_weekly_return_pct / 10.0, 0.0, 1.0);
   out.return_bonus_points = 20.0 * MinatoClampV2((out.geometric_weekly_return_pct - 10.0) / 10.0, 0.0, 1.0);
   out.return_confidence = MinatoClampV2(out.geometric_weekly_return_pct / 1.0, 0.0, 1.0);

   double tpm_ratio = MinatoClampV2(out.actual_tpm / 35.0, 0.0, 1.0);
   out.tpm_base_points = 30.0 * tpm_ratio * tpm_ratio;
   out.tpm_bonus_points = 10.0 * MinatoClampV2((out.actual_tpm - 35.0) / 65.0, 0.0, 1.0);
   out.dd_points = MinatoDDPointsV2(in.max_relative_dd_pct);

   out.sample_confidence = MinatoClampV2((in.completed_trades - 10.0) / 20.0, 0.0, 1.0);
   out.adjusted_sharpe_points = out.sharpe_points_before_confidence * out.sample_confidence;
   out.adjusted_pf_points = out.pf_points_before_confidence * out.sample_confidence;

   double auxiliary_points = out.tpm_base_points + out.tpm_bonus_points + out.dd_points +
                             out.adjusted_sharpe_points + out.adjusted_pf_points;
   out.fitness = out.return_base_points + out.return_bonus_points +
                 out.return_confidence * auxiliary_points;
   out.diagnostic = "VALID_PROFIT";
}

bool MinatoV2ConfigurationValid()
{
   return InpMinatoExpectedScoreVersion == MINATO_BASE_SCORE_V2_VERSION &&
          InpMinatoTestStart > 0 && InpMinatoTestEnd > InpMinatoTestStart;
}

void MinatoV2ResetCapture()
{
   g_minato_v2_first_tick_time = 0;
   g_minato_v2_last_tick_time = 0;
   g_minato_v2_last_tick_equity = 0.0;
   g_minato_v2_last_tick_balance = 0.0;
   g_minato_v2_last_tick_had_position = false;
   g_minato_v2_min_margin_level = DBL_MAX;
   g_minato_v2_runtime_margin_breach = false;
}

void MinatoV2ObserveTick()
{
   datetime now = TimeCurrent();
   if(g_minato_v2_first_tick_time == 0) g_minato_v2_first_tick_time = now;
   g_minato_v2_last_tick_time = now;
   g_minato_v2_last_tick_equity = AccountInfoDouble(ACCOUNT_EQUITY);
   g_minato_v2_last_tick_balance = AccountInfoDouble(ACCOUNT_BALANCE);
   g_minato_v2_last_tick_had_position = (PositionsTotal() > 0);

   double used_margin = AccountInfoDouble(ACCOUNT_MARGIN);
   double margin_level = AccountInfoDouble(ACCOUNT_MARGIN_LEVEL);
   if(used_margin > 0.0 && MathIsValidNumber(margin_level) && margin_level > 0.0)
   {
      if(margin_level < g_minato_v2_min_margin_level)
         g_minato_v2_min_margin_level = margin_level;
      if(margin_level < MINATO_MIN_MARGIN_LEVEL)
         g_minato_v2_runtime_margin_breach = true;
   }
}

double MinatoV2ScoreFromTester()
{
   SMinatoScoreInputsV2 inputs;
   inputs.starting_capital = TesterStatistics(STAT_INITIAL_DEPOSIT);
   inputs.ending_equity = g_minato_v2_last_tick_equity;
   double elapsed_seconds = (double)(InpMinatoTestEnd - InpMinatoTestStart);
   inputs.actual_weeks = elapsed_seconds / 604800.0;
   inputs.actual_months = elapsed_seconds / (86400.0 * 30.4375);
   inputs.completed_trades = TesterStatistics(STAT_TRADES);
   inputs.max_relative_dd_pct = TesterStatistics(STAT_EQUITY_DDREL_PERCENT);
   inputs.sharpe = TesterStatistics(STAT_SHARPE_RATIO);
   inputs.profit_factor = TesterStatistics(STAT_PROFIT_FACTOR);
   inputs.gross_loss = TesterStatistics(STAT_GROSS_LOSS);

   double tester_min_margin = TesterStatistics(STAT_MIN_MARGINLEVEL);
   inputs.minimum_margin_level = tester_min_margin;
   if(g_minato_v2_min_margin_level < DBL_MAX &&
      (inputs.minimum_margin_level <= 0.0 || g_minato_v2_min_margin_level < inputs.minimum_margin_level))
      inputs.minimum_margin_level = g_minato_v2_min_margin_level;
   inputs.runtime_margin_breach = g_minato_v2_runtime_margin_breach;

   MinatoScoreV2(inputs, g_minato_v2_last_score);
   if(!MinatoV2ConfigurationValid())
   {
      g_minato_v2_last_score.fitness = -1000.0;
      g_minato_v2_last_score.diagnostic = "SCORE_VERSION_OR_DATE_MISMATCH";
   }

   // Never persist a negative Minato fitness value.
   g_minato_v2_last_score.fitness = MathMax(0.0, g_minato_v2_last_score.fitness);

   double ontester_equity = AccountInfoDouble(ACCOUNT_EQUITY);
   double ontester_balance = AccountInfoDouble(ACCOUNT_BALANCE);

   int diagnostic_file = FileOpen("minato_v2_opt_results.csv",
                                  FILE_CSV|FILE_READ|FILE_WRITE|FILE_ANSI|FILE_SHARE_READ|FILE_SHARE_WRITE);
   if(diagnostic_file != INVALID_HANDLE)
   {
      if(FileSize(diagnostic_file) == 0)
         FileWrite(diagnostic_file, "score_version", "ea", "symbol", "fitness", "primary_diagnostic",
                   "secondary_flags", "geometric_weekly_return_pct", "return_base", "return_bonus",
                   "return_confidence", "trades", "tpm", "tpm_base", "tpm_bonus", "dd_pct", "dd_points",
                   "sharpe", "sharpe_points", "pf", "pf_points", "sample_confidence", "min_margin",
                   "tester_start", "tester_end", "first_tick", "last_tick", "ending_equity",
                   "last_tick_balance", "ontester_equity", "ontester_balance", "endpoint_open");
      FileSeek(diagnostic_file, 0, SEEK_END);
      FileWrite(diagnostic_file,
                g_minato_v2_last_score.score_version, MQLInfoString(MQL_PROGRAM_NAME), _Symbol,
                DoubleToString(g_minato_v2_last_score.fitness, 12), g_minato_v2_last_score.diagnostic,
                g_minato_v2_last_score.secondary_flags,
                DoubleToString(g_minato_v2_last_score.geometric_weekly_return_pct, 12),
                DoubleToString(g_minato_v2_last_score.return_base_points, 12),
                DoubleToString(g_minato_v2_last_score.return_bonus_points, 12),
                DoubleToString(g_minato_v2_last_score.return_confidence, 12),
                DoubleToString(g_minato_v2_last_score.completed_trades, 0),
                DoubleToString(g_minato_v2_last_score.actual_tpm, 12),
                DoubleToString(g_minato_v2_last_score.tpm_base_points, 12),
                DoubleToString(g_minato_v2_last_score.tpm_bonus_points, 12),
                DoubleToString(g_minato_v2_last_score.max_relative_dd_pct, 12),
                DoubleToString(g_minato_v2_last_score.dd_points, 12),
                DoubleToString(g_minato_v2_last_score.sharpe_raw, 12),
                DoubleToString(g_minato_v2_last_score.adjusted_sharpe_points, 12),
                DoubleToString(g_minato_v2_last_score.profit_factor_raw, 12),
                DoubleToString(g_minato_v2_last_score.adjusted_pf_points, 12),
                DoubleToString(g_minato_v2_last_score.sample_confidence, 12),
                DoubleToString(g_minato_v2_last_score.minimum_margin_level, 12),
                TimeToString(InpMinatoTestStart, TIME_DATE|TIME_MINUTES),
                TimeToString(InpMinatoTestEnd, TIME_DATE|TIME_MINUTES),
                TimeToString(g_minato_v2_first_tick_time, TIME_DATE|TIME_SECONDS),
                TimeToString(g_minato_v2_last_tick_time, TIME_DATE|TIME_SECONDS),
                DoubleToString(g_minato_v2_last_tick_equity, 12),
                DoubleToString(g_minato_v2_last_tick_balance, 12),
                DoubleToString(ontester_equity, 12), DoubleToString(ontester_balance, 12),
                g_minato_v2_last_tick_had_position ? "1" : "0");
      FileFlush(diagnostic_file);
      FileClose(diagnostic_file);
   }

   PrintFormat("MINATO_V2 version=%s fitness=%.12f status=%s flags=%s weekly=%.12f tpm=%.12f dd=%.12f trades=%.0f dates=%s..%s first_tick=%s last_tick=%s last_eq=%.12f last_bal=%.12f ontester_eq=%.12f ontester_bal=%.12f endpoint_open=%d min_margin=%.12f",
               g_minato_v2_last_score.score_version,
               g_minato_v2_last_score.fitness,
               g_minato_v2_last_score.diagnostic,
               g_minato_v2_last_score.secondary_flags,
               g_minato_v2_last_score.geometric_weekly_return_pct,
               g_minato_v2_last_score.actual_tpm,
               g_minato_v2_last_score.max_relative_dd_pct,
               g_minato_v2_last_score.completed_trades,
               TimeToString(InpMinatoTestStart, TIME_DATE|TIME_MINUTES),
               TimeToString(InpMinatoTestEnd, TIME_DATE|TIME_MINUTES),
               TimeToString(g_minato_v2_first_tick_time, TIME_DATE|TIME_SECONDS),
               TimeToString(g_minato_v2_last_tick_time, TIME_DATE|TIME_SECONDS),
               g_minato_v2_last_tick_equity,
               g_minato_v2_last_tick_balance,
               ontester_equity,
               ontester_balance,
               g_minato_v2_last_tick_had_position ? 1 : 0,
               inputs.minimum_margin_level);
   return g_minato_v2_last_score.fitness;
}
//+------------------------------------------------------------------+
