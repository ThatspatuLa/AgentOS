#property strict

#include <minato_base_score_v2.mqh>

bool FreeMarginAllowsNotional(double volume, double price)
{
   if(volume <= 0.0 || price <= 0.0) return false;
   double notional = volume * price;
   double free_margin = AccountInfoDouble(ACCOUNT_MARGIN_FREE);
   if(!MathIsValidNumber(free_margin) || free_margin <= 0.0) return false;
   return (free_margin >= 2.0 * notional);
}
#include <Trade/Trade.mqh>

input int InpEntryChannelPeriod = 20;
input int InpExitChannelPeriod = 10;
input int InpATRPeriod = 14;
input string InpFromDate = "2025.01.10";
input string InpToDate = "2026.01.10";
input double InpSL_ATR_Mult = 1.5;
input double InpTP_ATR_Mult = 2.5;

input double InpRegime_Greed_Lot_Mult = 1.0; // [lo=0,hi=5,step=0.25]
input double InpRegime_Greed_SL_Mult = 1.0; // [lo=0,hi=5,step=0.25]
input double InpRegime_Greed_TP_Mult = 1.0; // [lo=0,hi=5,step=0.25]
input double InpRegime_Neutral_Lot_Mult = 0.75; // [lo=0,hi=5,step=0.25]
input double InpRegime_Neutral_SL_Mult = 1.0; // [lo=0,hi=5,step=0.25]
input double InpRegime_Neutral_TP_Mult = 1.0; // [lo=0,hi=5,step=0.25]
input double InpRegime_Fear_Lot_Mult = 0.5; // [lo=0,hi=5,step=0.25]
input double InpRegime_Fear_SL_Mult = 1.0; // [lo=0,hi=5,step=0.25]
input double InpRegime_Fear_TP_Mult = 1.0; // [lo=0,hi=5,step=0.25]
input double InpRegime_ExtFear_Lot_Mult = 0.25; // [lo=0,hi=5,step=0.25]
input double InpRegime_ExtFear_SL_Mult = 1.0; // [lo=0,hi=5,step=0.25]
input double InpRegime_ExtFear_TP_Mult = 1.0; // [lo=0,hi=5,step=0.25]
input ulong InpMagicNumber = 261020001; // KCOS execution identity; not optimized

enum ENUM_MINATO_REGIME
{
   REGIME_INVALID = -1,
   REGIME_GREED = 0,
   REGIME_NEUTRAL = 1,
   REGIME_FEAR = 2,
   REGIME_EXTREME_FEAR = 3
};

CTrade g_trade;
int g_atr_handle = INVALID_HANDLE;
datetime g_last_closed_bar = 0;
ENUM_MINATO_REGIME g_closed_bar_regime = REGIME_INVALID;
bool g_features_valid = false;
double g_closed_close = 0.0;
double g_entry_upper = 0.0;
double g_entry_lower = 0.0;
double g_exit_upper = 0.0;
double g_exit_lower = 0.0;
double g_atr_value = 0.0;
double g_true_range = 0.0;
long g_kcos_diag_feature_attempt = 0;
long g_kcos_diag_feature_success = 0;
long g_kcos_diag_signal_long = 0;
long g_kcos_diag_signal_short = 0;
long g_kcos_diag_signal_conflict = 0;
long g_kcos_diag_volume_buy_zero = 0;
long g_kcos_diag_volume_sell_zero = 0;
long g_kcos_diag_volume_buy = 0;
long g_kcos_diag_volume_sell = 0;
long g_kcos_diag_margin_buy_check = 0;
long g_kcos_diag_margin_buy_notional_check = 0;
long g_kcos_diag_margin_sell_check = 0;
long g_kcos_diag_margin_sell_notional_check = 0;
long g_kcos_diag_stoptarget_buy_check = 0;
long g_kcos_diag_stoptarget_sell_check = 0;
long g_kcos_diag_order_attempt_buy = 0;
long g_kcos_diag_order_attempt_sell = 0;
long g_kcos_diag_order_retcode_buy = 0;
long g_kcos_diag_order_retcode_sell = 0;

double NormalizeLot(double requestedVolume)
{
   double minVolume=SymbolInfoDouble(_Symbol,SYMBOL_VOLUME_MIN);
   double maxVolume=SymbolInfoDouble(_Symbol,SYMBOL_VOLUME_MAX);
   double volumeStep=SymbolInfoDouble(_Symbol,SYMBOL_VOLUME_STEP);
   if(requestedVolume<=0.0)return 0.0;
   if(minVolume<=0.0)return 0.0;
   if(maxVolume<minVolume)return 0.0;
   if(volumeStep<=0.0)return 0.0;
   double cappedVolume=MathMin(requestedVolume,maxVolume);
   double flooredVolume=MathFloor(cappedVolume/volumeStep)*volumeStep;
   if(flooredVolume<minVolume)return 0.0;
   double normalizedVolume=NormalizeDouble(flooredVolume,8);
   if(normalizedVolume<minVolume)return 0.0;
   if(normalizedVolume>maxVolume)return 0.0;
   return normalizedVolume;
}

double RegimeLotMultiplier(ENUM_MINATO_REGIME regime)
{
   if(regime == REGIME_GREED) return InpRegime_Greed_Lot_Mult;
   if(regime == REGIME_NEUTRAL) return InpRegime_Neutral_Lot_Mult;
   if(regime == REGIME_FEAR) return InpRegime_Fear_Lot_Mult;
   if(regime == REGIME_EXTREME_FEAR) return InpRegime_ExtFear_Lot_Mult;
   return 0.0;
}

double RegimeSLMult(ENUM_MINATO_REGIME regime)
{
   if(regime == REGIME_GREED) return InpRegime_Greed_SL_Mult;
   if(regime == REGIME_NEUTRAL) return InpRegime_Neutral_SL_Mult;
   if(regime == REGIME_FEAR) return InpRegime_Fear_SL_Mult;
   if(regime == REGIME_EXTREME_FEAR) return InpRegime_ExtFear_SL_Mult;
   return 0.0;
}

double RegimeTPMult(ENUM_MINATO_REGIME regime)
{
   if(regime == REGIME_GREED) return InpRegime_Greed_TP_Mult;
   if(regime == REGIME_NEUTRAL) return InpRegime_Neutral_TP_Mult;
   if(regime == REGIME_FEAR) return InpRegime_Fear_TP_Mult;
   if(regime == REGIME_EXTREME_FEAR) return InpRegime_ExtFear_TP_Mult;
   return 0.0;
}

ENUM_MINATO_REGIME ClassifyRegime(double ratio)
{
   if(!MathIsValidNumber(ratio) || ratio <= 0.0) return REGIME_INVALID;
   if(ratio < 0.75) return REGIME_GREED;
   if(ratio < 1.25) return REGIME_NEUTRAL;
   if(ratio < 2.0) return REGIME_FEAR;
   return REGIME_EXTREME_FEAR;
}

bool InputsValid()
{
   return InpEntryChannelPeriod > 0 &&
          InpExitChannelPeriod > 0 &&
          InpATRPeriod > 0 &&
          InpSL_ATR_Mult > 0.0 &&
          InpTP_ATR_Mult > 0.0 &&
          InpRegime_Greed_Lot_Mult >= 0.0 &&
          InpRegime_Neutral_Lot_Mult >= 0.0 &&
          InpRegime_Fear_Lot_Mult >= 0.0 &&
          InpRegime_ExtFear_Lot_Mult >= 0.0 &&
          InpRegime_Greed_SL_Mult > 0.0 &&
          InpRegime_Neutral_SL_Mult > 0.0 &&
          InpRegime_Fear_SL_Mult > 0.0 &&
          InpRegime_ExtFear_SL_Mult > 0.0 &&
          InpRegime_Greed_TP_Mult > 0.0 &&
          InpRegime_Neutral_TP_Mult > 0.0 &&
          InpRegime_Fear_TP_Mult > 0.0 &&
          InpRegime_ExtFear_TP_Mult > 0.0;
}

bool MarginBreached()
{
   double used=AccountInfoDouble(ACCOUNT_MARGIN);
   double level=AccountInfoDouble(ACCOUNT_MARGIN_LEVEL);
   return used>0.0 && level>0.0 && level<MINATO_MIN_MARGIN_LEVEL;
}

void CloseAllPositions()
{
   CTrade trade;
   trade.SetExpertMagicNumber((ulong)InpMagicNumber);
   for(int i=PositionsTotal()-1;i>=0;i--)
   {
      ulong ticket=PositionGetTicket(i);
      if(ticket==0)continue;
      if(PositionGetString(POSITION_SYMBOL)!=_Symbol)continue;
      if(PositionGetInteger(POSITION_MAGIC) != (long)InpMagicNumber)continue;
      trade.PositionClose(ticket);
   }
}

bool ReadClosedFeatures()
{
   double highs[];
   double lows[];
   double closes[];

   int entryCount = InpEntryChannelPeriod;
   int exitCount = InpExitChannelPeriod;
   int requiredBars = MathMax(entryCount, exitCount) + InpATRPeriod + 20;

   if(Bars(_Symbol, PERIOD_M5) < requiredBars) return false;

   ArraySetAsSeries(highs, true);
   ArraySetAsSeries(lows, true);
   ArraySetAsSeries(closes, true);

   int copiedHigh = CopyHigh(_Symbol, PERIOD_M5, 1, requiredBars, highs);
   int copiedLow = CopyLow(_Symbol, PERIOD_M5, 1, requiredBars, lows);
   int copiedClose = CopyClose(_Symbol, PERIOD_M5, 1, requiredBars, closes);
   if(copiedHigh < requiredBars || copiedLow < requiredBars || copiedClose < requiredBars)
      return false;

   g_entry_upper = highs[1];
   g_entry_lower = lows[1];
   for(int i = 2; i <= entryCount; i++)
   {
      g_entry_upper = MathMax(g_entry_upper, highs[i]);
      g_entry_lower = MathMin(g_entry_lower, lows[i]);
   }

   g_exit_upper = highs[1];
   g_exit_lower = lows[1];
   for(int i = 2; i <= exitCount; i++)
   {
      g_exit_upper = MathMax(g_exit_upper, highs[i]);
      g_exit_lower = MathMin(g_exit_lower, lows[i]);
   }

   g_closed_close = closes[0];
   g_true_range = highs[0] - lows[0];
   g_true_range = MathMax(g_true_range, MathAbs(highs[0] - closes[1]));
   g_true_range = MathMax(g_true_range, MathAbs(lows[0] - closes[1]));

   double atrValues[];
   ArraySetAsSeries(atrValues, true);
   int copiedAtr = CopyBuffer(g_atr_handle, 0, 1, 1, atrValues);
   if(copiedAtr != 1 || !MathIsValidNumber(atrValues[0]) || atrValues[0] <= 0.0)
      return false;
   g_atr_value = atrValues[0];

   if(!MathIsValidNumber(g_entry_upper) || !MathIsValidNumber(g_entry_lower) ||
      !MathIsValidNumber(g_exit_upper) || !MathIsValidNumber(g_exit_lower) ||
      !MathIsValidNumber(g_closed_close) || !MathIsValidNumber(g_true_range) ||
      g_entry_upper <= 0.0 || g_entry_lower <= 0.0 ||
      g_exit_upper <= 0.0 || g_exit_lower <= 0.0)
      return false;

   double ratio = g_true_range / g_atr_value;
   g_closed_bar_regime = ClassifyRegime(ratio);
   if(g_closed_bar_regime == REGIME_INVALID) return false;

   g_features_valid = true;
   g_kcos_diag_feature_success++;
   return true;
}

void ManageChannelExits()
{
   if(!g_features_valid) return;
   for(int i = PositionsTotal() - 1; i >= 0; i--)
   {
      ulong ticket = PositionGetTicket(i);
      if(ticket == 0) continue;
      if(PositionGetString(POSITION_SYMBOL) != _Symbol) continue;
      if(PositionGetInteger(POSITION_MAGIC) != (long)InpMagicNumber) continue;

      ENUM_POSITION_TYPE type = (ENUM_POSITION_TYPE)PositionGetInteger(POSITION_TYPE);
      bool closeLong = (type == POSITION_TYPE_BUY && g_closed_close <= g_exit_lower);
      bool closeShort = (type == POSITION_TYPE_SELL && g_closed_close >= g_exit_upper);
      if(closeLong || closeShort)
         g_trade.PositionClose(ticket);
   }
}

void EnterBuy()
{
   double ask = SymbolInfoDouble(_Symbol, SYMBOL_ASK);
   if(!MathIsValidNumber(ask) || ask <= 0.0) return;

   double volume = MinatoRegimeQuantity(1.0, RegimeLotMultiplier(g_closed_bar_regime));
   if(volume <= 0.0) return;
   volume = NormalizeLot(volume);
   if(volume <= 0.0) g_kcos_diag_volume_buy_zero++;
   if(volume <= 0.0) return;
   g_kcos_diag_volume_buy++;
   g_kcos_diag_margin_buy_check++;
   if(!MinatoAllowsProjectedMargin(ORDER_TYPE_BUY, volume, ask)) return;
   g_kcos_diag_margin_buy_notional_check++;
   if(!FreeMarginAllowsNotional(volume, ask)) return;

   double tp = MinatoTakeProfit(ask, g_atr_value * InpTP_ATR_Mult,
                                RegimeTPMult(g_closed_bar_regime), true);
   double sl = ask - g_atr_value * InpSL_ATR_Mult * RegimeSLMult(g_closed_bar_regime);

   g_kcos_diag_stoptarget_buy_check++;
   if(!MathIsValidNumber(tp) || !MathIsValidNumber(sl) || tp <= 0.0 || sl <= 0.0)
      return;

   g_kcos_diag_order_attempt_buy++;
   g_trade.Buy(volume, _Symbol, ask, sl, tp, "Donchian breakout buy");
   g_kcos_diag_order_retcode_buy = (long)g_trade.ResultRetcode();
}

void EnterSell()
{
   double bid = SymbolInfoDouble(_Symbol, SYMBOL_BID);
   if(!MathIsValidNumber(bid) || bid <= 0.0) return;

   double volume = MinatoRegimeQuantity(1.0, RegimeLotMultiplier(g_closed_bar_regime));
   if(volume <= 0.0) return;
   volume = NormalizeLot(volume);
   if(volume <= 0.0) g_kcos_diag_volume_sell_zero++;
   if(volume <= 0.0) return;
   g_kcos_diag_volume_sell++;
   g_kcos_diag_margin_sell_check++;
   if(!MinatoAllowsProjectedMargin(ORDER_TYPE_SELL, volume, bid)) return;
   g_kcos_diag_margin_sell_notional_check++;
   if(!FreeMarginAllowsNotional(volume, bid)) return;

   double tp = MinatoTakeProfit(bid, g_atr_value * InpTP_ATR_Mult,
                                RegimeTPMult(g_closed_bar_regime), false);
   double sl = bid + g_atr_value * InpSL_ATR_Mult * RegimeSLMult(g_closed_bar_regime);

   g_kcos_diag_stoptarget_sell_check++;
   if(!MathIsValidNumber(tp) || !MathIsValidNumber(sl) || tp <= 0.0 || sl <= 0.0)
      return;

   g_kcos_diag_order_attempt_sell++;
   g_trade.Sell(volume, _Symbol, bid, sl, tp, "Donchian breakout sell");
   g_kcos_diag_order_retcode_sell = (long)g_trade.ResultRetcode();
}

int OnInit()
{
   g_kcos_diag_feature_attempt = 0;
   g_kcos_diag_feature_success = 0;
   g_kcos_diag_signal_long = 0;
   g_kcos_diag_signal_short = 0;
   g_kcos_diag_signal_conflict = 0;
   g_kcos_diag_volume_buy_zero = 0;
   g_kcos_diag_volume_sell_zero = 0;
   g_kcos_diag_volume_buy = 0;
   g_kcos_diag_volume_sell = 0;
   g_kcos_diag_margin_buy_check = 0;
   g_kcos_diag_margin_buy_notional_check = 0;
   g_kcos_diag_margin_sell_check = 0;
   g_kcos_diag_margin_sell_notional_check = 0;
   g_kcos_diag_stoptarget_buy_check = 0;
   g_kcos_diag_stoptarget_sell_check = 0;
   g_kcos_diag_order_attempt_buy = 0;
   g_kcos_diag_order_attempt_sell = 0;
   g_kcos_diag_order_retcode_buy = 0;
   g_kcos_diag_order_retcode_sell = 0;
   MinatoV2ResetCapture();
   if(!MinatoV2ConfigurationValid()) return INIT_PARAMETERS_INCORRECT;

   if(_Period != PERIOD_M5) return INIT_PARAMETERS_INCORRECT;
   if(!InputsValid()) return INIT_PARAMETERS_INCORRECT;

   g_atr_handle = iATR(_Symbol, PERIOD_M5, InpATRPeriod);
   if(g_atr_handle == INVALID_HANDLE) return INIT_FAILED;

   g_trade.SetExpertMagicNumber((ulong)InpMagicNumber);
   g_trade.SetTypeFillingBySymbol(_Symbol);

   g_last_closed_bar = iTime(_Symbol, PERIOD_M5, 1);
   g_features_valid = false;
   g_closed_bar_regime = REGIME_INVALID;
   return INIT_SUCCEEDED;
}

void OnDeinit(const int reason)
{
   if(g_atr_handle != INVALID_HANDLE)
      IndicatorRelease(g_atr_handle);
}

void OnTick()
{
   MinatoV2ObserveTick();
   if(MarginBreached()){CloseAllPositions();return;}

   if(!NewBarClosed()) return;

   datetime closedBar = iTime(_Symbol, PERIOD_M5, 1);
   if(closedBar == 0 || closedBar == g_last_closed_bar) return;
   g_last_closed_bar = closedBar;
   g_features_valid = false;
   g_closed_bar_regime = REGIME_INVALID;

   g_kcos_diag_feature_attempt++;
   if(!ReadClosedFeatures()) return;

   ManageChannelExits();

   bool hasPosition = false;
   for(int i = PositionsTotal() - 1; i >= 0; i--)
   {
      ulong ticket = PositionGetTicket(i);
      if(ticket == 0) continue;
      if(PositionGetString(POSITION_SYMBOL) == _Symbol &&
         PositionGetInteger(POSITION_MAGIC) == (long)InpMagicNumber)
      {
         hasPosition = true;
         break;
      }
   }
   if(hasPosition) return;

   bool longSignal = g_closed_close > g_entry_upper;
   bool shortSignal = g_closed_close < g_entry_lower;
   if(longSignal && shortSignal) g_kcos_diag_signal_conflict++;
   if(longSignal && shortSignal) return;

   if(longSignal) g_kcos_diag_signal_long++;
   if(longSignal) EnterBuy();
   if(shortSignal) g_kcos_diag_signal_short++;
   if(shortSignal) EnterSell();
}

double OnTester()
{
   PrintFormat("[KCOS_DIAG:v1]|InpATRPeriod=%d|InpEntryChannelPeriod=%d|InpExitChannelPeriod=%d|InpRegime_ExtFear_Lot_Mult=%.17g|InpRegime_ExtFear_SL_Mult=%.17g|InpRegime_ExtFear_TP_Mult=%.17g|InpRegime_Fear_Lot_Mult=%.17g|InpRegime_Fear_SL_Mult=%.17g|InpRegime_Fear_TP_Mult=%.17g|InpRegime_Greed_Lot_Mult=%.17g|InpRegime_Greed_SL_Mult=%.17g|InpRegime_Greed_TP_Mult=%.17g|InpRegime_Neutral_Lot_Mult=%.17g|InpRegime_Neutral_SL_Mult=%.17g|InpRegime_Neutral_TP_Mult=%.17g|InpSL_ATR_Mult=%.17g|InpTP_ATR_Mult=%.17g|feature_attempt=%I64d|feature_success=%I64d|signal_long=%I64d|signal_short=%I64d|signal_conflict=%I64d|volume_buy_zero=%I64d|volume_sell_zero=%I64d|volume_buy=%I64d|volume_sell=%I64d|margin_buy_check=%I64d|margin_buy_notional_check=%I64d|margin_sell_check=%I64d|margin_sell_notional_check=%I64d|stoptarget_buy_check=%I64d|stoptarget_sell_check=%I64d|order_attempt_buy=%I64d|order_attempt_sell=%I64d|order_retcode_buy=%I64d|order_retcode_sell=%I64d", InpATRPeriod, InpEntryChannelPeriod, InpExitChannelPeriod, InpRegime_ExtFear_Lot_Mult, InpRegime_ExtFear_SL_Mult, InpRegime_ExtFear_TP_Mult, InpRegime_Fear_Lot_Mult, InpRegime_Fear_SL_Mult, InpRegime_Fear_TP_Mult, InpRegime_Greed_Lot_Mult, InpRegime_Greed_SL_Mult, InpRegime_Greed_TP_Mult, InpRegime_Neutral_Lot_Mult, InpRegime_Neutral_SL_Mult, InpRegime_Neutral_TP_Mult, InpSL_ATR_Mult, InpTP_ATR_Mult, g_kcos_diag_feature_attempt, g_kcos_diag_feature_success, g_kcos_diag_signal_long, g_kcos_diag_signal_short, g_kcos_diag_signal_conflict, g_kcos_diag_volume_buy_zero, g_kcos_diag_volume_sell_zero, g_kcos_diag_volume_buy, g_kcos_diag_volume_sell, g_kcos_diag_margin_buy_check, g_kcos_diag_margin_buy_notional_check, g_kcos_diag_margin_sell_check, g_kcos_diag_margin_sell_notional_check, g_kcos_diag_stoptarget_buy_check, g_kcos_diag_stoptarget_sell_check, g_kcos_diag_order_attempt_buy, g_kcos_diag_order_attempt_sell, g_kcos_diag_order_retcode_buy, g_kcos_diag_order_retcode_sell);
   return MinatoV2ScoreFromTester();
}
