//+------------------------------------------------------------------+

//|                  B1_1_OBV_Divergence_v1.mq5                       |

//|         Copyright 2026, Project Zen - Kiyosaki Swarm              |

//| v1 - Participation edge: OBV divergence vs price                 |

//|                                                                  |

//|  Mechanism:                                                       |

//|    Tier 1 (LTF): OBV divergence detection                        |

//|      - OBV = cumulative sum of signed tick-volume                 |

//|        up-bar:   +volume        (close > open)                   |

//|        down-bar: -volume        (close < open)                   |

//|        doji-bar:  0             (close == open)                  |

//|      - Detect pivot highs/lows (fractal-style, InpPivotStrength   |

//|        bars either side) on the LAST CLOSED bar (shift >= 2)      |

//|      - DIVERGENCE: price makes Higher High but OBV makes Lower    |

//|        High over InpObvDivLookback window (bearish distribution). |

//|        Inverse: price Lower Low + OBV Higher Low (bullish).       |

//|    Tier 2: EMA(InpEmaTrendPeriod) trend filter                    |

//|      - SHORT only if close < EMA (downtrend)                      |

//|      - LONG only if close > EMA (uptrend)                         |

//|    Tier 3: ATR-based SL/TP                                        |

//|      - SL = ATR * InpAtrSlMult * RegimeSLMult                     |

//|      - TP = ATR * InpAtrTpMult * RegimeTPMult                     |

//|    Tier 4: regime-aware lot / SL / TP multipliers (H1 regime)    |

//|                                                                  |

//|  Distinct from other Participation primitives:                    |

//|    - CMF zero-cross:  per-bar oscillator continuation             |

//|    - A/D divergence:  CLV-weighted cumulative, wick-sensitive     |

//|    - VWMA Cross:       level crossover (price vs volume-weighted MA)|

//|    - OBV divergence:  unweighted cumulative ±tick-volume vs price |

//|                        Coarser than A/D (no CLV range-normalize)  |

//|                        More wick-robust than CLV on ETH 5m        |

//|                                                                  |

//|  Risk:                                                            |

//|    - Margin kill at 90% (InpMarginKillLevel, fixed)               |

//|    - Position tracked via HasOpenPosition() (Lesson 27)           |

//|    - Bar-close-first via NewBarClosed() (Lesson 4b)               |

//|                                                                  |

//|  OnTester returns Minato Base score from minato_base_score.mqh.   |

//+------------------------------------------------------------------+

#property copyright "Project Zen - Kiyosaki Swarm"

#property link      ""

#property version   "1.00"

#property strict



#include <kiyosaki_regime.mqh>

#include <minato_base_score_v2.mqh>

#include <Trade/Trade.mqh>



CTrade trade;



//------------------------------------------------------------------

// Inputs - strategy

//------------------------------------------------------------------

input int  InpObvDivLookback = 25;         // OBV divergence lookback (bars)  [lo=19,hi=31,step=2]
input int  InpPivotStrength = 2;          // Pivot strength: bars on each side  [lo=1,hi=3,step=1]
input int  InpEmaTrendPeriod = 50;         // EMA trend filter period  [lo=38,hi=62,step=4]
input int  InpAtrPeriod = 14;         // ATR period for SL/TP  [lo=11,hi=17,step=1]
input double  InpAtrSlMult = 2.0;        // SL = ATR * mult (then x regime SLMult)  [lo=1.2,hi=3.0,step=0.36]
input double  InpAtrTpMult = 3.0;        // TP = ATR * mult (then x regime TPMult)  [lo=1.8,hi=4.5,step=0.54]


input double  InpLots = 0.5;        // Base lot (multiplied by regime Lot)  [lo=0.25,hi=1.0,step=0.15]
input double InpMarginKillLevel        = 120.0;     // Margin kill threshold (%)



input ulong  InpMagicNumber            = 260814014;// Magic (prefix 14 = OBV_Divergence v1)



//------------------------------------------------------------------

// Inputs - regime multipliers (Lesson 24 - all 12 scanned with ||Y)

//------------------------------------------------------------------

input double  InpRegime_GreedLot = 1.0;        // Lot mult in GREED  [lo=0.6,hi=1.5,step=0.18]
input double  InpRegime_GreedSLMult = 1.0;        // SL mult in GREED  [lo=0.6,hi=1.5,step=0.18]
input double  InpRegime_GreedTPMult = 1.0;        // TP mult in GREED  [lo=0.6,hi=1.5,step=0.18]
input double  InpRegime_NeutralLot = 0.5;        // Lot mult in NEUTRAL  [lo=0.25,hi=1.0,step=0.15]
input double  InpRegime_NeutralSLMult = 1.0;        // SL mult in NEUTRAL  [lo=0.6,hi=1.5,step=0.18]
input double  InpRegime_NeutralTPMult = 1.0;        // TP mult in NEUTRAL  [lo=0.6,hi=1.5,step=0.18]
input double  InpRegime_FearLot = 0.25;       // Lot mult in FEAR  [lo=0.125,hi=0.5,step=0.075]
input double  InpRegime_FearSLMult = 1.0;        // SL mult in FEAR  [lo=0.6,hi=1.5,step=0.18]
input double  InpRegime_FearTPMult = 1.0;        // TP mult in FEAR  [lo=0.6,hi=1.5,step=0.18]
input double  InpRegime_ExtFearLot = 0.25;       // Lot mult in EXTREME_FEAR  [lo=0.125,hi=0.5,step=0.075]
input double  InpRegime_ExtFearSLMult = 1.0;        // SL mult in EXTREME_FEAR  [lo=0.6,hi=1.5,step=0.18]
input double  InpRegime_ExtFearTPMult = 1.0;        // TP mult in EXTREME_FEAR  [lo=0.6,hi=1.5,step=0.18]


//------------------------------------------------------------------

// Globals

//------------------------------------------------------------------

int g_atr_handle = INVALID_HANDLE;

int g_ema_handle = INVALID_HANDLE;



CKiyosakiRegime g_regime;



double g_atr_buf[];

double g_close_buf[];

double g_ema_buf[];



// Pre-computed arrays

// g_obv[shift] = cumulative OBV value at bar (shift). shift=0 is current.

// g_is_pivot_high[shift] / g_is_pivot_low[shift] = true if bar is a fractal pivot.

double g_obv[];

bool   g_is_pivot_high[];

bool   g_is_pivot_low[];

int    g_obv_size = 0;  // bounded rolling window size



datetime g_last_trade_time = 0;

double   g_min_margin_level = 999.0;

bool     g_margin_breach    = false;



bool g_init_ok = false;



//------------------------------------------------------------------

// Compute body-signed volume and pivots for a bounded rolling window.

// Called once at OnInit and once per closed chart bar.

// Work and memory: O(window), independent of total tester history.

//------------------------------------------------------------------

bool BuildPrecomputedArrays()
{
   // A common cumulative prefix cancels in every compared OBV pivot pair.
   // Keep both lookback searches plus pivot neighbours, never full history.
   if (InpObvDivLookback < 1 || InpPivotStrength < 1 ||
       InpEmaTrendPeriod < 1 || InpAtrPeriod < 1) return false;
   int window = MathMax(100, 2 * InpObvDivLookback + 2 * InpPivotStrength + 8);
   window = MathMax(window, MathMax(InpEmaTrendPeriod + 3, InpAtrPeriod + 3));
   if (Bars(_Symbol, PERIOD_CURRENT) < window) return false;
   int n = window;
   double o[], c[], hi_s[], lo_s[];
   long v[];
   ArraySetAsSeries(o, false);
   ArraySetAsSeries(c, false);
   ArraySetAsSeries(v, false);
   ArraySetAsSeries(hi_s, true);
   ArraySetAsSeries(lo_s, true);
   // All copies must describe the same window before any indexed access.
   if (CopyOpen(_Symbol, PERIOD_CURRENT, 0, n, o) != n) return false;
   if (CopyClose(_Symbol, PERIOD_CURRENT, 0, n, c) != n) return false;
   if (CopyTickVolume(_Symbol, PERIOD_CURRENT, 0, n, v) != n) return false;
   if (CopyHigh(_Symbol, PERIOD_CURRENT, 0, n, hi_s) != n) return false;
   if (CopyLow(_Symbol, PERIOD_CURRENT, 0, n, lo_s) != n) return false;
   ArraySetAsSeries(g_obv, false);
   if (ArrayResize(g_obv, n) != n) return false;
   long cum = 0;
   for (int i = 0; i < n; i++)
   {
      double op = o[i];
      double cl = c[i];
      long vt = v[i];
      if (cl > op) cum += vt;
      else if (cl < op) cum -= vt;
      g_obv[i] = (double)cum;
   }
   // Physical oldest-first storage, logical shift access. No second reversal.
   ArraySetAsSeries(g_obv, true);
   // These ordinary arrays are explicitly keyed by shift, not copy destinations.
   ArraySetAsSeries(g_is_pivot_high, false);
   ArraySetAsSeries(g_is_pivot_low, false);
   if (ArrayResize(g_is_pivot_high, n) != n) return false;
   if (ArrayResize(g_is_pivot_low, n) != n) return false;
   ArrayInitialize(g_is_pivot_high, false);
   ArrayInitialize(g_is_pivot_low, false);
   int len = InpPivotStrength;
   for (int shift = len + 1; shift < n - len - 1; shift++)
   {
      bool is_h = true, is_l = true;
      double pivot_h = hi_s[shift];
      double pivot_l = lo_s[shift];
      for (int j = 1; j <= len; j++)
      {
         if (hi_s[shift - j] > pivot_h) is_h = false;
         if (hi_s[shift + j] > pivot_h) is_h = false;
         if (lo_s[shift - j] < pivot_l) is_l = false;
         if (lo_s[shift + j] < pivot_l) is_l = false;
      }
      g_is_pivot_high[shift] = is_h;
      g_is_pivot_low[shift] = is_l;
   }
   g_obv_size = n;
   return true;
}



//------------------------------------------------------------------

// Refresh the latest bar's OBV contribution (call on new bar close)

//------------------------------------------------------------------

bool RefreshLatestBar()
{
   // OnTick calls this once behind NewBarClosed, even at a Bars-count plateau.
   return BuildPrecomputedArrays();
}



//------------------------------------------------------------------

// Expert initialization

//------------------------------------------------------------------

int OnInit()

{
   MinatoV2ResetCapture();
   if(!MinatoV2ConfigurationValid())
      return INIT_PARAMETERS_INCORRECT;


   PrintFormat("B1_1_OBV_Divergence v1 init OK: divLookback=%d pivotStr=%d emaTrend=%d atr=%d sl=%.2f tp=%.2f magic=%I64u", InpObvDivLookback, InpPivotStrength, InpEmaTrendPeriod, InpAtrPeriod, InpAtrSlMult, InpAtrTpMult, InpMagicNumber);



   g_atr_handle = iATR(_Symbol, PERIOD_CURRENT, InpAtrPeriod);

   g_ema_handle = iMA(_Symbol, PERIOD_CURRENT, InpEmaTrendPeriod, 0, MODE_EMA, PRICE_CLOSE);



   if (g_atr_handle == INVALID_HANDLE || g_ema_handle == INVALID_HANDLE)

   {

      PrintFormat("B1_1_OBV_Divergence v1 init FAIL: handles invalid (atr=%d ema=%d)",

                  g_atr_handle, g_ema_handle);

      return INIT_FAILED;

   }



   ArraySetAsSeries(g_atr_buf,  true);

   ArraySetAsSeries(g_close_buf, true);

   ArraySetAsSeries(g_ema_buf,   true);



   // Build OBV + pivots once (O(window) at init and each closed bar)

   if (!BuildPrecomputedArrays())

   {

      PrintFormat("B1_1_OBV_Divergence v1 init FAIL: precompute failed (bars=%d)", Bars(_Symbol, PERIOD_CURRENT));

      return INIT_FAILED;

   }

   PrintFormat("B1_1_OBV_Divergence v1 precompute OK: bars=%d obv_size=%d", Bars(_Symbol, PERIOD_CURRENT), g_obv_size);



   // Regime init (H1)

   g_regime.Reset();

   if (!g_regime.Init(_Symbol, PERIOD_H1))

   {

      PrintFormat("B1_1_OBV_Divergence v1 init WARN: regime init failed, defaulting to NEUTRAL");

   }



   trade.SetExpertMagicNumber(InpMagicNumber);

   g_init_ok = true;

   return INIT_SUCCEEDED;

}



void OnDeinit(const int reason)

{

   if (g_atr_handle != INVALID_HANDLE) IndicatorRelease(g_atr_handle);

   if (g_ema_handle != INVALID_HANDLE) IndicatorRelease(g_ema_handle);

   g_regime.Deinit();

}



//------------------------------------------------------------------

// HasOpenPosition - poll PositionsTotal (Lesson 27)

//------------------------------------------------------------------

bool HasOpenPosition()

{

   for (int i = PositionsTotal() - 1; i >= 0; i--)

   {

      ulong ticket = PositionGetTicket(i);

      if (ticket == 0) continue;

      if (PositionGetInteger(POSITION_MAGIC) != (long)InpMagicNumber) continue;

      if (PositionGetString(POSITION_SYMBOL) != _Symbol) continue;

      return true;

   }

   return false;

}



//------------------------------------------------------------------

// Get pivot high shift within (from_shift+1 .. from_shift+lookback).

// Returns true and sets out_shift, out_price, out_obv.

//------------------------------------------------------------------

bool FindPrevPivotHigh(int from_shift, int lookback, int &out_shift, double &out_price, double &out_obv)

{

   int max_shift = MathMin(from_shift + lookback, g_obv_size - 1);

   for (int s = from_shift + 1; s <= max_shift; s++)

   {

      if (g_is_pivot_high[s])

      {

         double h[];

         ArraySetAsSeries(h, true);

         if (CopyHigh(_Symbol, PERIOD_CURRENT, s, 1, h) > 0)

         {

            out_shift = s;

            out_price = h[0];

            out_obv   = g_obv[s];

            return true;

         }

      }

   }

   return false;

}



bool FindPrevPivotLow(int from_shift, int lookback, int &out_shift, double &out_price, double &out_obv)

{

   int max_shift = MathMin(from_shift + lookback, g_obv_size - 1);

   for (int s = from_shift + 1; s <= max_shift; s++)

   {

      if (g_is_pivot_low[s])

      {

         double l[];

         ArraySetAsSeries(l, true);

         if (CopyLow(_Symbol, PERIOD_CURRENT, s, 1, l) > 0)

         {

            out_shift = s;

            out_price = l[0];

            out_obv   = g_obv[s];

            return true;

         }

      }

   }

   return false;

}



//------------------------------------------------------------------

// Find the MAXIMUM of high[] between [from_shift+1 .. from_shift+lookback].

//------------------------------------------------------------------

bool FindMaxHigh(int from_shift, int lookback, int &out_shift, double &out_price)

{

   if (from_shift + lookback > g_obv_size) return false;

   double highs[];

   ArraySetAsSeries(highs, true);

   if (CopyHigh(_Symbol, PERIOD_CURRENT, from_shift + 1, lookback, highs) != lookback) return false;

   int    idx   = 0;

   double max_h = highs[0];

   for (int i = 1; i < lookback; i++)

   {

      if (highs[i] > max_h)

      {

         max_h = highs[i];

         idx   = i;

      }

   }

   out_shift = from_shift + 1 + idx;

   out_price = max_h;

   return true;

}



bool FindMinLow(int from_shift, int lookback, int &out_shift, double &out_price)

{

   if (from_shift + lookback > g_obv_size) return false;

   double lows[];

   ArraySetAsSeries(lows, true);

   if (CopyLow(_Symbol, PERIOD_CURRENT, from_shift + 1, lookback, lows) != lookback) return false;

   int    idx   = 0;

   double min_l = lows[0];

   for (int i = 1; i < lookback; i++)

   {

      if (lows[i] < min_l)

      {

         min_l = lows[i];

         idx   = i;

      }

   }

   out_shift = from_shift + 1 + idx;

   out_price = min_l;

   return true;

}



//------------------------------------------------------------------

// Compute current regime (H1)

//------------------------------------------------------------------

ENUM_REGIME CalcRegime()

{

   double h1high[], h1low[], h1close[];

   ArraySetAsSeries(h1high,  true);

   ArraySetAsSeries(h1low,   true);

   ArraySetAsSeries(h1close, true);

   int h1_total = Bars(_Symbol, PERIOD_H1);

   if (h1_total < 61) return REGIME_NEUTRAL;

   if (CopyHigh(_Symbol, PERIOD_H1, 1, 60, h1high) != 60) return REGIME_NEUTRAL;

   if (CopyLow(_Symbol, PERIOD_H1, 1, 60, h1low) != 60) return REGIME_NEUTRAL;

   if (CopyClose(_Symbol, PERIOD_H1, 1, 60, h1close) != 60) return REGIME_NEUTRAL;

   return g_regime.Calc(h1high, h1low, h1close, 60);

}



double RegimeLotMult(ENUM_REGIME r)

{

   switch (r)

   {

      case REGIME_GREED:        return InpRegime_GreedLot;

      case REGIME_NEUTRAL:      return InpRegime_NeutralLot;

      case REGIME_FEAR:         return InpRegime_FearLot;

      case REGIME_EXTREME_FEAR: return InpRegime_ExtFearLot;

   }

   return 1.0;

}



double RegimeSLMult(ENUM_REGIME r)

{

   switch (r)

   {

      case REGIME_GREED:        return InpRegime_GreedSLMult;

      case REGIME_NEUTRAL:      return InpRegime_NeutralSLMult;

      case REGIME_FEAR:         return InpRegime_FearSLMult;

      case REGIME_EXTREME_FEAR: return InpRegime_ExtFearSLMult;

   }

   return 1.0;

}



double RegimeTPMult(ENUM_REGIME r)

{

   switch (r)

   {

      case REGIME_GREED:        return InpRegime_GreedTPMult;

      case REGIME_NEUTRAL:      return InpRegime_NeutralTPMult;

      case REGIME_FEAR:         return InpRegime_FearTPMult;

      case REGIME_EXTREME_FEAR: return InpRegime_ExtFearTPMult;

   }

   return 1.0;

}



double NormalizeLot(double requestedVolume)
{
   double minVolume = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_MIN);
   double maxVolume = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_MAX);
   double volumeStep = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_STEP);
   if (requestedVolume <= 0.0) return 0.0;
   if (minVolume <= 0.0) return 0.0;
   if (maxVolume < minVolume) return 0.0;
   if (volumeStep <= 0.0) return 0.0;
   double cappedVolume = MathMin(requestedVolume, maxVolume);
   double flooredVolume = MathFloor(cappedVolume / volumeStep) * volumeStep;
   if (flooredVolume < minVolume) return 0.0;
   double normalizedVolume = NormalizeDouble(flooredVolume, 8);
   if (normalizedVolume < minVolume) return 0.0;
   if (normalizedVolume > maxVolume) return 0.0;
   return normalizedVolume;
}



//------------------------------------------------------------------

// Expert tick function (NewBarClosed() from minato_base_score.mqh)

//------------------------------------------------------------------

void OnTick()

{
   MinatoV2ObserveTick();


   if (!g_init_ok) return;



   // Margin kill (Lesson 25)

   double marginLevel = AccountInfoDouble(ACCOUNT_MARGIN_LEVEL);

   if (marginLevel > 0.0 && marginLevel < g_min_margin_level)

      g_min_margin_level = marginLevel;

   if (marginLevel > 0.0 && marginLevel < InpMarginKillLevel)

   {

      g_margin_breach = true;

      for (int i = PositionsTotal() - 1; i >= 0; i--)

      {

         ulong ticket = PositionGetTicket(i);

         if (ticket == 0) continue;

         if (PositionGetInteger(POSITION_MAGIC) != (long)InpMagicNumber) continue;

         if (PositionGetString(POSITION_SYMBOL) != _Symbol) continue;

         trade.PositionClose(ticket);

      }

      return;

   }



   // Bar-close-first

   if (!NewBarClosed()) return;



   // Refresh bounded arrays on every closed bar
   if (!RefreshLatestBar()) return;



   // Read EMA(close) bar 1

   if (CopyBuffer(g_ema_handle, 0, 1, 1, g_ema_buf) != 1) return;

   double ema1 = g_ema_buf[0];

   if (ema1 <= 0.0) return;



   // Read ATR bar 1

   if (CopyBuffer(g_atr_handle, 0, 1, 1, g_atr_buf) != 1) return;

   double atr1 = g_atr_buf[0];

   if (atr1 <= 0.0) return;



   // Read close bar 1

   if (CopyClose(_Symbol, PERIOD_CURRENT, 1, 1, g_close_buf) != 1) return;

   double close1 = g_close_buf[0];

   if (close1 <= 0.0) return;



   // ============================================================

   // DIVERGENCE DETECTION

   // ============================================================

   int pivot_str = InpPivotStrength;

   int lookback  = InpObvDivLookback;



   int required = pivot_str * 2 + lookback + 5;

   if (g_obv_size < required) return;



   // Reference OBV at shift 1 (current closed bar) - O(1) lookup

   double obv_current = g_obv[1];



   // Find most recent pivot HIGH (at shift > 1, within lookback)

   int    prev_h_shift = 0;

   double prev_h_price = 0.0, prev_h_obv = 0.0;

   bool   found_prev_h = FindPrevPivotHigh(1, lookback, prev_h_shift, prev_h_price, prev_h_obv);



   // Find most recent pivot LOW

   int    prev_l_shift = 0;

   double prev_l_price = 0.0, prev_l_obv = 0.0;

   bool   found_prev_l = FindPrevPivotLow(1, lookback, prev_l_shift, prev_l_price, prev_l_obv);



   // Max high and min low in last lookback

   int    recent_h_shift = 0, recent_l_shift = 0;

   double recent_h = 0.0, recent_l = 0.0;

   if (!FindMaxHigh(1, lookback, recent_h_shift, recent_h)) return;

   if (!FindMinLow(1, lookback, recent_l_shift, recent_l)) return;



   // ============================================================

   // BEARISH DIVERGENCE: price makes Higher High BUT OBV makes Lower High

   // ============================================================

   bool short_signal = false;

   if (found_prev_h)

   {

      int older_h_shift = 0;

      double older_h_price = 0.0, older_h_obv = 0.0;

      int search_to = MathMin(prev_h_shift + lookback, g_obv_size - 1);

      bool found_older = FindPrevPivotHigh(prev_h_shift, lookback, older_h_shift, older_h_price, older_h_obv);

      // the second arg to FindPrevPivotHigh is "from_shift" so begin search FROM prev_h_shift.

      // But FindPrevPivotHigh scans from_shift+1..from_shift+lookback. We want older, so that's fine.

      if (found_older && prev_h_price > older_h_price + _Point && prev_h_obv < older_h_obv - _Point)

      {

         short_signal = true;

         PrintFormat("OBV_Div_S: prevH_s=%d p=%.2f o=%.0f | olderH_s=%d p=%.2f o=%.0f | close=%.2f ema=%.2f",

                     prev_h_shift, prev_h_price, prev_h_obv, older_h_shift, older_h_price, older_h_obv, close1, ema1);

      }

   }



   // ============================================================

   // BULLISH DIVERGENCE: price makes Lower Low BUT OBV makes Higher Low

   // ============================================================

   bool long_signal = false;

   if (found_prev_l)

   {

      int older_l_shift = 0;

      double older_l_price = 0.0, older_l_obv = 0.0;

      bool found_older = FindPrevPivotLow(prev_l_shift, lookback, older_l_shift, older_l_price, older_l_obv);

      if (found_older && prev_l_price < older_l_price - _Point && prev_l_obv > older_l_obv + _Point)

      {

         long_signal = true;

         PrintFormat("OBV_Div_L: prevL_s=%d p=%.2f o=%.0f | olderL_s=%d p=%.2f o=%.0f | close=%.2f ema=%.2f",

                     prev_l_shift, prev_l_price, prev_l_obv, older_l_shift, older_l_price, older_l_obv, close1, ema1);

      }

   }



   // ============================================================

   // TRADE FILTERS

   // ============================================================

   if (g_margin_breach) return;

   if (HasOpenPosition()) return;



   // Cooldown

   datetime cur_bar_time = iTime(_Symbol, PERIOD_CURRENT, 1);

   if (cur_bar_time <= g_last_trade_time) return;




   // Trend filter

   if (long_signal && close1 <= ema1)  long_signal = false;

   if (short_signal && close1 >= ema1) short_signal = false;



   if (!long_signal && !short_signal) return;



   // Regime

   ENUM_REGIME r = CalcRegime();

   double lot = MinatoRegimeQuantity(InpLots, RegimeLotMult(r));

   double sl_dist = atr1 * InpAtrSlMult * RegimeSLMult(r);

   double tp_dist = atr1 * InpAtrTpMult * RegimeTPMult(r);

   if (sl_dist <= 0.0 || tp_dist <= 0.0) return;
   if (lot <= 0.0) return;
lot = NormalizeLot(lot);
if (lot <= 0.0) return;




   double ask = SymbolInfoDouble(_Symbol, SYMBOL_ASK);

   double bid = SymbolInfoDouble(_Symbol, SYMBOL_BID);



   if (long_signal)

   {

      double sl = NormalizeDouble(ask - sl_dist, _Digits);

      double tp = MinatoTakeProfit(ask, tp_dist, 1.0, true);
      tp = NormalizeDouble(tp, _Digits);

      if (!MinatoAllowsProjectedMargin(ORDER_TYPE_BUY, lot, ask)) return;

      if (trade.Buy(lot, _Symbol, ask, sl, tp, "OBV_Div_L"))

      {

         g_last_trade_time = cur_bar_time;

         PrintFormat("OBV_Div_L OPEN: lot=%.2f sl=%.2f tp=%.2f regime=%d atr=%.2f",

                     lot, sl_dist, tp_dist, r, atr1);

      }

   }

   else if (short_signal)

   {

      double sl = NormalizeDouble(bid + sl_dist, _Digits);

      double tp = MinatoTakeProfit(bid, tp_dist, 1.0, false);
      tp = NormalizeDouble(tp, _Digits);

      if (!MinatoAllowsProjectedMargin(ORDER_TYPE_SELL, lot, bid)) return;

      if (trade.Sell(lot, _Symbol, bid, sl, tp, "OBV_Div_S"))

      {

         g_last_trade_time = cur_bar_time;

         PrintFormat("OBV_Div_S OPEN: lot=%.2f sl=%.2f tp=%.2f regime=%d atr=%.2f",

                     lot, sl_dist, tp_dist, r, atr1);

      }

   }

}



//------------------------------------------------------------------

// OnTester  - Minato Base score from minato_base_score.mqh

//------------------------------------------------------------------

double OnTester()
{
   return MinatoV2ScoreFromTester();
}

//+------------------------------------------------------------------+