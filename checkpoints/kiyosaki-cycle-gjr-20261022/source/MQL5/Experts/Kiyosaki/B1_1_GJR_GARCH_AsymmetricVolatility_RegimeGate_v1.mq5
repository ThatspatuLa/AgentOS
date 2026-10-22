//+------------------------------------------------------------------+
//|         B1_1_GJR_GARCH_AsymmetricVolatility_RegimeGate_v1.mq5    |
//| KCOS Stage 2 - GJR-GARCH asymmetric volatility regime gate EA    |
//+------------------------------------------------------------------+
#property strict

#include <Trade\Trade.mqh>
#include <minato_base_score_v2.mqh>


bool FreeMarginAllowsNotional(double volume, double price)
{
   if(volume <= 0.0 || price <= 0.0) return false;
   double notional = volume * price;
   double free_margin = AccountInfoDouble(ACCOUNT_MARGIN_FREE);
   if(!MathIsValidNumber(free_margin) || free_margin <= 0.0) return false;
   return (free_margin >= 2.0 * notional);
}
//--- EA-owned strategy inputs (declared with MQL types and approved defaults)
input int    InpGJRLookback          = 256;
input double InpGJRAlpha             = 0.12;
input double InpGJRGamma             = 0.08;
input double InpGJRBeta              = 0.82;
input double InpBaseLot = 0.01; // Stage 2 base size before regime multiplier [lo=0.01,hi=0.20,step=0.01]
input double InpVolRegimeThreshold   = 1.35;
input double InpExtremeVolThreshold  = 2.25;
input double InpNeutralReturnBand    = 0.0015;
input int    InpATRPeriod            = 21;
input double InpStopATRMult          = 2.2;
input double InpTakeProfitATRMult    = 3.4;
input int    InpMagicNumber          = 26091042;

input double InpRegime_Greed_Lot_Mult    = 1.0; // [lo=0,hi=5,step=0.25]
input double InpRegime_Greed_SL_Mult     = 1.0; // [lo=0,hi=5,step=0.25]
input double InpRegime_Greed_TP_Mult     = 1.0; // [lo=0,hi=5,step=0.25]
input double InpRegime_Neutral_Lot_Mult  = 1.0; // [lo=0,hi=5,step=0.25]
input double InpRegime_Neutral_SL_Mult   = 1.0; // [lo=0,hi=5,step=0.25]
input double InpRegime_Neutral_TP_Mult   = 1.0; // [lo=0,hi=5,step=0.25]
input double InpRegime_Fear_Lot_Mult     = 1.0; // [lo=0,hi=5,step=0.25]
input double InpRegime_Fear_SL_Mult      = 1.0; // [lo=0,hi=5,step=0.25]
input double InpRegime_Fear_TP_Mult      = 1.0; // [lo=0,hi=5,step=0.25]
input double InpRegime_ExtFear_Lot_Mult  = 1.0; // [lo=0,hi=5,step=0.25]
input double InpRegime_ExtFear_SL_Mult   = 1.0; // [lo=0,hi=5,step=0.25]
input double InpRegime_ExtFear_TP_Mult   = 1.0; // [lo=0,hi=5,step=0.25]

//--- Tester-window metadata only (do not gate trading/calculations)
input string InpFromDate = "2025.01.10";
input string InpToDate   = "2026.01.10";

CTrade trade;

//--- Regime enum and rolling state
enum ENUM_REGIME
{
   REGIME_NEUTRAL      = 0,
   REGIME_GREED        = 1,
   REGIME_FEAR         = 2,
   REGIME_EXTREME_FEAR = 3
};

ENUM_REGIME g_active_regime = REGIME_NEUTRAL;
double      g_sigma2        = 0.0;
double      g_epsilon_prev  = 0.0;
double      g_sigma_ema     = 0.0;
int         g_lookback_seed_count = 0;
bool        g_warmup_complete = false;

//--- ATR via iATR + CopyBuffer at shift 1
int g_atr_handle = INVALID_HANDLE;

bool ReadATRClosed(double &outATR)
{
   if(g_atr_handle == INVALID_HANDLE)
      return false;
   double buf[1];
   if(CopyBuffer(g_atr_handle, 0, 1, 1, buf) != 1)
      return false;
   outATR = buf[0];
   return true;
}

//--- Rolling log-return buffer for warmup seeding
double g_logret[];
int    g_logret_max = 0;

void PushLogReturn(double r)
{
   int n = g_lookback_seed_count;
   if(n >= g_logret_max)
   {
      g_logret_max = (g_logret_max <= 0) ? InpGJRLookback + 2 : g_logret_max * 2;
      ArrayResize(g_logret, g_logret_max);
   }
   g_logret[n] = r;
}

//--- NormalizeLot canonical helper (exact required body)
double NormalizeLot(double requestedVolume)
{
   double minVolume = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_MIN);
   double maxVolume = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_MAX);
   double volumeStep = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_STEP);
   if(requestedVolume <= 0.0) return 0.0;
   if(minVolume <= 0.0) return 0.0;
   if(maxVolume < minVolume) return 0.0;
   if(volumeStep <= 0.0) return 0.0;
   double cappedVolume = MathMin(requestedVolume, maxVolume);
   double flooredVolume = MathFloor(cappedVolume / volumeStep) * volumeStep;
   if(flooredVolume < minVolume) return 0.0;
   double normalizedVolume = NormalizeDouble(flooredVolume, 8);
   if(normalizedVolume < minVolume) return 0.0;
   if(normalizedVolume > maxVolume) return 0.0;
   return normalizedVolume;
}

//--- Regime helpers (mutually exclusive priority EXTREME_FEAR > GREED/FEAR > NEUTRAL)
double RegimeLotMult(ENUM_REGIME r)
{
   switch(r)
   {
      case REGIME_GREED:        return InpRegime_Greed_Lot_Mult;
      case REGIME_FEAR:         return InpRegime_Fear_Lot_Mult;
      case REGIME_EXTREME_FEAR: return InpRegime_ExtFear_Lot_Mult;
      default:                  return InpRegime_Neutral_Lot_Mult;
   }
}

double RegimeSLMult(ENUM_REGIME r)
{
   switch(r)
   {
      case REGIME_GREED:        return InpRegime_Greed_SL_Mult;
      case REGIME_FEAR:         return InpRegime_Fear_SL_Mult;
      case REGIME_EXTREME_FEAR: return InpRegime_ExtFear_SL_Mult;
      default:                  return InpRegime_Neutral_SL_Mult;
   }
}

double RegimeTPMult(ENUM_REGIME r)
{
   switch(r)
   {
      case REGIME_GREED:        return InpRegime_Greed_TP_Mult;
      case REGIME_FEAR:         return InpRegime_Fear_TP_Mult;
      case REGIME_EXTREME_FEAR: return InpRegime_ExtFear_TP_Mult;
      default:                  return InpRegime_Neutral_TP_Mult;
   }
}

bool HasOpenPosition()
{
   for(int i = PositionsTotal() - 1; i >= 0; i--)
   {
      ulong ticket = PositionGetTicket(i);
      if(ticket == 0) continue;
      if(PositionGetString(POSITION_SYMBOL) != _Symbol) continue;
      if(PositionGetInteger(POSITION_MAGIC) != (long)InpMagicNumber) continue;
      return true;
   }
   return false;
}

//--- Canonical margin wrapper (read-only margin admission against 120% include constant)
bool MarginBreached()
{
   double used = AccountInfoDouble(ACCOUNT_MARGIN);
   double level = AccountInfoDouble(ACCOUNT_MARGIN_LEVEL);
   return (used > 0.0 && level > 0.0 && level < MINATO_MIN_MARGIN_LEVEL);
}

void CloseAllPositions()
{
   CTrade ct;
   ct.SetExpertMagicNumber((ulong)InpMagicNumber);
   for(int i = PositionsTotal() - 1; i >= 0; i--)
   {
      ulong ticket = PositionGetTicket(i);
      if(ticket == 0) continue;
      if(PositionGetString(POSITION_SYMBOL) != _Symbol) continue;
      if(PositionGetInteger(POSITION_MAGIC) != (long)InpMagicNumber) continue;
      ct.PositionClose(ticket);
   }
   g_active_regime = REGIME_NEUTRAL;
}

int OnInit()
{
   MinatoV2ResetCapture();
   if(!MinatoV2ConfigurationValid()) return INIT_PARAMETERS_INCORRECT;

   trade.SetExpertMagicNumber((ulong)InpMagicNumber);
   trade.SetDeviationInPoints(10);
   trade.SetTypeFillingBySymbol(_Symbol);

   g_atr_handle = iATR(_Symbol, _Period, InpATRPeriod);

   ArrayResize(g_logret, InpGJRLookback + 4);
   g_logret_max = ArraySize(g_logret);

   g_sigma2              = 0.0;
   g_epsilon_prev        = 0.0;
   g_sigma_ema           = 0.0;
   g_lookback_seed_count = 0;
   g_warmup_complete     = false;
   g_active_regime       = REGIME_NEUTRAL;

   return INIT_SUCCEEDED;
}

void OnDeinit(const int reason)
{
   if(g_atr_handle != INVALID_HANDLE)
   {
      IndicatorRelease(g_atr_handle);
      g_atr_handle = INVALID_HANDLE;
   }
}

//--- TryBuy: request, refuse, normalize, reject, then refuse on margin and submit
void TryBuy(double ask, double atrValue)
{
   if(ask <= 0.0 || atrValue <= 0.0) return;

   double minVolume = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_MIN);
   if(minVolume <= 0.0) return;
   double baseQty = MathMax(InpBaseLot, minVolume);

   double volume = MinatoRegimeQuantity(baseQty, RegimeLotMult(g_active_regime));
   if(volume <= 0.0) return;
   volume = NormalizeLot(volume);
   if(volume <= 0.0) return;
   if(!MinatoAllowsProjectedMargin(ORDER_TYPE_BUY, volume, ask)) return;
   if(!FreeMarginAllowsNotional(volume, ask)) return;

   double tp = MinatoTakeProfit(ask, atrValue * InpTakeProfitATRMult, RegimeTPMult(g_active_regime), true);
   double sl = ask - atrValue * InpStopATRMult * RegimeSLMult(g_active_regime);
   if(sl <= 0.0 || tp <= 0.0) return;

   trade.Buy(volume, _Symbol, ask, sl, tp, "GJR_BUY");
}

//--- TrySell: request, refuse, normalize, reject, then refuse on margin and submit
void TrySell(double bid, double atrValue)
{
   if(bid <= 0.0 || atrValue <= 0.0) return;

   double minVolume = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_MIN);
   if(minVolume <= 0.0) return;
   double baseQty = MathMax(InpBaseLot, minVolume);

   double volume = MinatoRegimeQuantity(baseQty, RegimeLotMult(g_active_regime));
   if(volume <= 0.0) return;
   volume = NormalizeLot(volume);
   if(volume <= 0.0) return;
   if(!MinatoAllowsProjectedMargin(ORDER_TYPE_SELL, volume, bid)) return;
   if(!FreeMarginAllowsNotional(volume, bid)) return;

   double tp = MinatoTakeProfit(bid, atrValue * InpTakeProfitATRMult, RegimeTPMult(g_active_regime), false);
   double sl = bid + atrValue * InpStopATRMult * RegimeSLMult(g_active_regime);
   if(sl <= 0.0 || tp <= 0.0) return;

   trade.Sell(volume, _Symbol, bid, sl, tp, "GJR_SELL");
}

void OnTick()
{
   MinatoV2ObserveTick();
   if(MarginBreached())
   {
      CloseAllPositions();
      return;
   }

   if(!NewBarClosed()) return;

   datetime t0 = iTime(_Symbol, _Period, 1);
   if(t0 == 0) return;

   double open1  = iOpen(_Symbol, _Period, 1);
   double high1  = iHigh(_Symbol, _Period, 1);
   double low1   = iLow(_Symbol, _Period, 1);
   double close1 = iClose(_Symbol, _Period, 1);
   double prevC  = iClose(_Symbol, _Period, 2);
   if(open1 <= 0.0 || high1 <= 0.0 || low1 <= 0.0 || close1 <= 0.0 || prevC <= 0.0) return;

   double atrValue = 0.0;
   if(!ReadATRClosed(atrValue)) return;
   if(atrValue <= 0.0) return;

   double r_t = MathLog(close1 / prevC);
   double epsilon_t = r_t;

   int required = InpGJRLookback + 2;
   if(g_lookback_seed_count < required)
   {
      PushLogReturn(r_t);
      g_lookback_seed_count++;

      if(g_lookback_seed_count == InpGJRLookback)
      {
         double mean = 0.0;
         for(int i = 0; i < InpGJRLookback; i++) mean += g_logret[i];
         mean /= InpGJRLookback;
         double var = 0.0;
         for(int i = 0; i < InpGJRLookback; i++)
            var += (g_logret[i] - mean) * (g_logret[i] - mean);
         var /= InpGJRLookback;
         if(var <= 0.0) var = 1e-12;
         g_sigma2      = var;
         g_sigma_ema   = MathSqrt(g_sigma2);
         g_epsilon_prev = epsilon_t;
      }
      return;
   }

   double persist = InpGJRAlpha + InpGJRBeta + 0.5 * InpGJRGamma;
   if(persist >= 0.999) persist = 0.999;
   double omega = g_sigma2 * (1.0 - persist);
   if(omega <= 0.0) omega = 1e-12;

   double epsPrevSq = g_epsilon_prev * g_epsilon_prev;
   double indicator = (g_epsilon_prev < 0.0) ? 1.0 : 0.0;
   double sigma2_new = omega
                       + InpGJRAlpha * epsPrevSq
                       + InpGJRGamma * epsPrevSq * indicator
                       + InpGJRBeta * g_sigma2;
   if(sigma2_new <= 0.0) sigma2_new = 1e-12;
   g_sigma2 = sigma2_new;
   double sigma_t = MathSqrt(sigma2_new);

   double alphaEma = 2.0 / (InpGJRLookback + 1.0);
   g_sigma_ema = alphaEma * sigma_t + (1.0 - alphaEma) * g_sigma_ema;
   if(g_sigma_ema <= 0.0) g_sigma_ema = 1e-12;

   double volRatio = sigma_t / g_sigma_ema;

   //--- Mutually exclusive regime classification by priority
   // Priority: EXTREME_FEAR (high vol + negative return + above extreme threshold),
   //           FEAR (high vol + negative return + below extreme threshold),
   //           GREED (high vol + positive return),
   //           NEUTRAL (low vol + |r| within InpNeutralReturnBand).
   ENUM_REGIME newRegime = REGIME_NEUTRAL;
   if(volRatio >= InpVolRegimeThreshold && r_t < 0.0)
   {
      if(volRatio >= InpExtremeVolThreshold) newRegime = REGIME_EXTREME_FEAR;
      else                                    newRegime = REGIME_FEAR;
   }
   else if(volRatio >= InpVolRegimeThreshold && r_t > 0.0)
   {
      newRegime = REGIME_GREED;
   }
   else
   {
      // Low-vol regime uses the neutral return band to distinguish quiet
      // NEUTRAL closes from inconclusive data that should be skipped.
      if(MathAbs(r_t) <= InpNeutralReturnBand)
         newRegime = REGIME_NEUTRAL;
      else
         newRegime = REGIME_NEUTRAL; // Conflict policy: skip non-band low-vol moves.
   }

   //--- Opposite-signal close on existing owned position
   if(HasOpenPosition())
   {
      bool wasLong  = (g_active_regime == REGIME_GREED || g_active_regime == REGIME_NEUTRAL);
      bool wasShort = (g_active_regime == REGIME_FEAR  || g_active_regime == REGIME_EXTREME_FEAR);
      bool wantLong  = (r_t > 0.0);
      bool wantShort = (r_t < 0.0);
      if((wasLong && wantShort) || (wasShort && wantLong))
      {
         CloseAllPositions();
         g_active_regime = newRegime;
         g_epsilon_prev = epsilon_t;
         return;
      }
   }

   g_active_regime = newRegime;
   g_epsilon_prev  = epsilon_t;

   if(HasOpenPosition()) return;

   //--- Conflict policy: when return is exactly zero, neither long nor short fires.
   if(r_t > 0.0)
   {
      double ask = SymbolInfoDouble(_Symbol, SYMBOL_ASK);
      if(ask > 0.0) TryBuy(ask, atrValue);
   }
   else if(r_t < 0.0)
   {
      double bid = SymbolInfoDouble(_Symbol, SYMBOL_BID);
      if(bid > 0.0) TrySell(bid, atrValue);
   }
}

double OnTester()
{
   return MinatoV2ScoreFromTester();
}

//+------------------------------------------------------------------+