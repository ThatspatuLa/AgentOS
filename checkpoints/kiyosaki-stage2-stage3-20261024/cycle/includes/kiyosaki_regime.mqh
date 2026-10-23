//+------------------------------------------------------------------+
//|                    kiyosaki_regime.mqh                            |
//|         Copyright 2026, Project Zen - Kiyosaki Swarm              |
//|                                                                  |
//|  Port of the PineScript "Kiyosaki MS" indicator to MQL5.          |
//|  8-state base collapsed to 4 deliverable states:                 |
//|    GREED, NEUTRAL, FEAR, EXTREME_FEAR                            |
//|                                                                  |
//|  Uses:                                                           |
//|    CKiyosakiRegime regime;                                       |
//|    regime.Init(...);  regime.Calc(...);  regime.Deinit();        |
//|                                                                  |
//+------------------------------------------------------------------+
#ifndef KIYOSAKI_REGIME_MQH
#define KIYOSAKI_REGIME_MQH

enum ENUM_REGIME
{
   REGIME_GREED         = 1,
   REGIME_NEUTRAL       = 0,
   REGIME_FEAR          = -1,
   REGIME_EXTREME_FEAR  = -2
};

struct CKiyosakiRegimeInputs
{
   int    pivotLen;
   double adxWeakToMedUp;
   double adxMedToStrongUp;
   double adxMedToWeakDown;
   double adxStrongToMedDown;
   int    upUpgradeBars_1;
   int    upUpgradeBars_2;
   int    dnUpgradeBars_1;
   int    dnUpgradeBars_2;
   int    rsiLen;
   double rsiPanic;
   double rsiRecover;
   double rsiDivMax;
   int    divPivotLen;
   bool   extFearRequireBreak;
};

class CKiyosakiRegime
{
public:
   CKiyosakiRegimeInputs m_inp;

   double m_lastHigh;
   double m_prevHigh;
   double m_lastLow;
   double m_prevLow;
   int    m_side;
   double m_bullAnchor;
   double m_bearAnchor;
   int    m_adxTier;
   int    m_intensity;
   int    m_upCount1;
   int    m_upCount2;
   int    m_dnCount1;
   int    m_dnCount2;
   bool   m_panicActive;
   double m_pLast;
   double m_pPrev;
   double m_rLast;
   double m_rPrev;
   int    m_divEffectBars;

   int    m_hADX;
   int    m_hRSI;

   CKiyosakiRegime()
   {
      m_inp.pivotLen           = 10;
      m_inp.adxWeakToMedUp     = 22.0;
      m_inp.adxMedToStrongUp   = 32.0;
      m_inp.adxMedToWeakDown   = 18.0;
      m_inp.adxStrongToMedDown = 28.0;
      m_inp.upUpgradeBars_1    = 5;
      m_inp.upUpgradeBars_2    = 7;
      m_inp.dnUpgradeBars_1    = 3;
      m_inp.dnUpgradeBars_2    = 4;
      m_inp.rsiLen             = 21;
      m_inp.rsiPanic           = 28.0;
      m_inp.rsiRecover         = 32.0;
      m_inp.rsiDivMax          = 30.0;
      m_inp.divPivotLen        = 5;
      m_inp.extFearRequireBreak = true;

      m_hADX = INVALID_HANDLE;
      m_hRSI = INVALID_HANDLE;
      Reset();
   }

   void Reset()
   {
      m_lastHigh = 0.0;
      m_prevHigh = 0.0;
      m_lastLow  = 0.0;
      m_prevLow  = 0.0;
      m_side = 0;
      m_bullAnchor = 0.0;
      m_bearAnchor = 0.0;
      m_adxTier = 0;
      m_intensity = 0;
      m_upCount1 = 0;
      m_upCount2 = 0;
      m_dnCount1 = 0;
      m_dnCount2 = 0;
      m_panicActive = false;
      m_pLast = 0.0;
      m_pPrev = 0.0;
      m_rLast = 0.0;
      m_rPrev = 0.0;
      m_divEffectBars = 0;
   }

   bool Init(string symbol, ENUM_TIMEFRAMES tf)
   {
      m_hADX = iADX(symbol, tf, 14);
      m_hRSI = iRSI(symbol, tf, m_inp.rsiLen, PRICE_CLOSE);
      if (m_hADX == INVALID_HANDLE || m_hRSI == INVALID_HANDLE) return false;
      return true;
   }

   void Deinit()
   {
      if (m_hADX != INVALID_HANDLE) IndicatorRelease(m_hADX);
      if (m_hRSI != INVALID_HANDLE) IndicatorRelease(m_hRSI);
   }

   double ReadRSI(const double &close[], int shift)
   {
      double buf[1];
      if (CopyBuffer(m_hRSI, 0, shift, 1, buf) <= 0) return 50.0;
      return buf[0];
   }

   ENUM_REGIME Calc(const double &high[], const double &low[], const double &close[], int totalBars)
   {
      if (totalBars < m_inp.pivotLen * 2 + 20) return REGIME_NEUTRAL;
      if (m_hADX == INVALID_HANDLE || m_hRSI == INVALID_HANDLE) return REGIME_NEUTRAL;

      double adxb[1], dipb[1], dinb[1];
      if (CopyBuffer(m_hADX, 0, 1, 1, adxb) <= 0) return REGIME_NEUTRAL;
      if (CopyBuffer(m_hADX, 1, 1, 1, dipb) <= 0) return REGIME_NEUTRAL;
      if (CopyBuffer(m_hADX, 2, 1, 1, dinb) <= 0) return REGIME_NEUTRAL;
      double adxVal  = adxb[0];
      double diPlus  = dipb[0];
      double diMinus = dinb[0];

      if (m_adxTier == 0)
      { if (adxVal >= m_inp.adxWeakToMedUp) m_adxTier = 1; }
      else if (m_adxTier == 1)
      {
         if (adxVal >= m_inp.adxMedToStrongUp) m_adxTier = 2;
         else if (adxVal <= m_inp.adxMedToWeakDown) m_adxTier = 0;
      }
      else { if (adxVal <= m_inp.adxStrongToMedDown) m_adxTier = 1; }

      double rsib[1];
      if (CopyBuffer(m_hRSI, 0, 1, 1, rsib) <= 0) return REGIME_NEUTRAL;
      double rsiVal = rsib[0];
      if (!m_panicActive && rsiVal < m_inp.rsiPanic) m_panicActive = true;
      else if (m_panicActive && rsiVal > m_inp.rsiRecover) m_panicActive = false;

      int sh = m_inp.pivotLen;
      double phv = high[sh];
      bool hasPH = (phv > 0.0 && phv != EMPTY_VALUE);
      if (hasPH && phv != m_lastHigh) { m_prevHigh = m_lastHigh; m_lastHigh = phv; }
      double plv = low[sh];
      bool hasPL = (plv > 0.0 && plv != EMPTY_VALUE);
      if (hasPL && plv != m_lastLow) { m_prevLow = m_lastLow; m_lastLow = plv; }

      bool haveHighs = (m_lastHigh != 0.0 && m_prevHigh != 0.0);
      bool haveLows  = (m_lastLow  != 0.0 && m_prevLow  != 0.0);
      double curClose = close[1];
      bool bosUp = (m_side == 0 && haveHighs && haveLows && curClose > m_lastHigh);
      bool bosDn = (m_side == 0 && haveHighs && haveLows && curClose < m_lastLow);
      if (bosUp) { m_side = 1; m_bullAnchor = m_lastLow; m_bearAnchor = 0.0; }
      else if (bosDn) { m_side = -1; m_bearAnchor = m_lastHigh; m_bullAnchor = 0.0; }
      if (m_side == 1 && hasPL && m_lastLow != 0.0) m_bullAnchor = m_lastLow;
      if (m_side == -1 && hasPH && m_lastHigh != 0.0) m_bearAnchor = m_lastHigh;
      bool bullBreak = (m_side == 1 && m_bullAnchor != 0.0 && curClose < m_bullAnchor);
      bool bearBreak = (m_side == -1 && m_bearAnchor != 0.0 && curClose > m_bearAnchor);
      if (bullBreak || bearBreak) { m_side = 0; m_bullAnchor = 0.0; m_bearAnchor = 0.0; }

      int desiredIntensity = m_adxTier;
      if (m_side == 0) { m_intensity = 0; m_upCount1 = m_upCount2 = m_dnCount1 = m_dnCount2 = 0; }
      else if (desiredIntensity < m_intensity) { m_intensity = desiredIntensity; m_upCount1 = m_upCount2 = m_dnCount1 = m_dnCount2 = 0; }
      else if (desiredIntensity == m_intensity) { m_upCount1 = m_upCount2 = m_dnCount1 = m_dnCount2 = 0; }
      else
      {
         if (m_side == 1)
         {
            if (m_intensity == 0 && desiredIntensity >= 1)
            { m_upCount1++; if (m_upCount1 >= m_inp.upUpgradeBars_1) { m_intensity = 1; m_upCount1 = m_upCount2 = m_dnCount1 = m_dnCount2 = 0; } }
            else if (m_intensity == 1 && desiredIntensity >= 2)
            { m_upCount2++; if (m_upCount2 >= m_inp.upUpgradeBars_2) { m_intensity = 2; m_upCount1 = m_upCount2 = m_dnCount1 = m_dnCount2 = 0; } }
         }
         else
         {
            if (m_intensity == 0 && desiredIntensity >= 1)
            { m_dnCount1++; if (m_dnCount1 >= m_inp.dnUpgradeBars_1) { m_intensity = 1; m_upCount1 = m_upCount2 = m_dnCount1 = m_dnCount2 = 0; } }
            else if (m_intensity == 1 && desiredIntensity >= 2)
            { m_dnCount2++; if (m_dnCount2 >= m_inp.dnUpgradeBars_2) { m_intensity = 2; m_upCount1 = m_upCount2 = m_dnCount1 = m_dnCount2 = 0; } }
         }
      }

      int baseState7 = 0;
      if (m_side == 0) baseState7 = 0;
      else if (m_side == 1) baseState7 = (m_intensity == 0 ? 1 : m_intensity == 1 ? 2 : 3);
      else baseState7 = (m_intensity == 0 ? -1 : m_intensity == 1 ? -2 : -3);

      bool divBullConfirmed = false;
      int dsh = m_inp.divPivotLen;
      if (dsh < totalBars - 2)
      {
         double plPivotLow = low[dsh];
         if (plPivotLow > 0.0 && plPivotLow != EMPTY_VALUE)
         {
            m_pPrev = m_pLast;
            m_rPrev = m_rLast;
               m_pLast = plPivotLow;
            m_rLast = ReadRSI(close, dsh);
               if (m_pPrev != 0.0 && m_rPrev != 0.0)
            {
               divBullConfirmed = (m_pLast < m_pPrev) && (m_rLast > m_rPrev) && (m_rLast < m_inp.rsiDivMax) && (m_rPrev < m_inp.rsiDivMax);
            }
         }
      }

      bool priceBreakOK = (!m_inp.extFearRequireBreak) || (m_lastLow != 0.0 && curClose < m_lastLow);
      bool bearPressure = (diMinus > diPlus);
      bool extFearCond = (m_side == -1) && m_panicActive && (m_adxTier == 2) && bearPressure && priceBreakOK;

      int finalState7 = baseState7;
      if (extFearCond) finalState7 = -4;
      if (m_panicActive && !divBullConfirmed)
      {
         if (finalState7 == 3) finalState7 = 2;
         else if (finalState7 == 2) finalState7 = 1;
         else if (finalState7 == 1) finalState7 = 0;
         else if (finalState7 == 0) finalState7 = -1;
         else if (finalState7 == -1) finalState7 = -2;
         else if (finalState7 == -2) finalState7 = -3;
         else if (finalState7 == -3) finalState7 = -4;
      }
      if (divBullConfirmed) m_divEffectBars = 3;
      else if (m_divEffectBars > 0) m_divEffectBars--;
      if (m_divEffectBars > 0) finalState7 = (int)MathMax((double)finalState7 - 2.0, -4.0);

      if (finalState7 >= 2) return REGIME_GREED;
      if (finalState7 >= -1) return REGIME_NEUTRAL;
      if (finalState7 >= -3) return REGIME_FEAR;
      return REGIME_EXTREME_FEAR;
   }
};

#endif // KIYOSAKI_REGIME_MQH
