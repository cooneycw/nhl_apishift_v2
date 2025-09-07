# NHL Shootout Games Analysis

## Overview
This document provides analysis of NHL games that went to shootout, including game identification and data structure information.

## Shootout Game Found

### Game ID: 2024020819
- **Date:** January 30, 2025
- **Teams:** Detroit Red Wings (away) vs Edmonton Oilers (home)
- **Venue:** Rogers Place, Edmonton
- **Final Score:** Detroit Red Wings 3, Edmonton Oilers 2
- **Result:** Detroit Red Wings won in shootout

### Game Progression
1. **Regulation:** 3 periods completed
2. **Overtime:** 1 overtime period (no winner)
3. **Shootout:** Detroit Red Wings scored winning goal

## Data Structure Analysis

### Key Fields in Play-by-Play Data
```json
{
  "id": 2024020819,
  "periodDescriptor": {
    "number": 5,
    "periodType": "SO",
    "maxRegulationPeriods": 3
  },
  "shootoutInUse": true,
  "otInUse": true,
  "gameOutcome": {
    "lastPeriodType": "SO"
  }
}
```

### Key Fields in Boxscore Data
```json
{
  "id": 2024020819,
  "awayTeam": {
    "id": 17,
    "commonName": {"default": "Red Wings"},
    "abbrev": "DET",
    "score": 3
  },
  "homeTeam": {
    "id": 22,
    "commonName": {"default": "Oilers"},
    "abbrev": "EDM",
    "score": 2
  }
}
```

## Search Methodology

### Files Searched
1. **HTML Reports:** `/storage/20242025/html/reports/`
   - Game Summary (GS)
   - Event Summary (ES)
   - Shot Summary (SS)
   - Time on Ice (TH)
   - Play-by-Play (PL)

2. **JSON Data:** `/storage/20242025/json/`
   - Boxscores: `/boxscores/`
   - Play-by-Play: `/playbyplay/`

### Search Patterns Used
```bash
# Find games with shootout capability
grep -r "shootoutInUse.*true" /storage/20242025/json/playbyplay/

# Find games that went to overtime
grep -r "periodType.*OT" /storage/20242025/json/playbyplay/

# Find games that went to shootout
grep -r "periodType.*SO" /storage/20242025/json/playbyplay/
```

## Other Games with Shootout Capability

The following games were found to have `"shootoutInUse": true` but did not actually go to shootout:

1. **Game ID: 2024021171** - Buffalo Sabres 8, Washington Capitals 5 (Regulation win)
2. **Game ID: 2024020343** - Seattle Kraken 3, Anaheim Ducks 2 (Regulation win)
3. **Game ID: 2024020823** - (Not analyzed)
4. **Game ID: 2024020215** - (Not analyzed)

## Games with Overtime (No Shootout)

1. **Game ID: 2024020999** - Chicago Blackhawks 4, Utah Hockey Club 3 (Overtime win)
   - Went to overtime but was decided before shootout

## Data Structure Notes

### Period Types
- `"REG"` - Regulation period
- `"OT"` - Overtime period
- `"SO"` - Shootout period

### Game Outcome Indicators
- `"lastPeriodType": "SO"` - Game decided by shootout
- `"lastPeriodType": "OT"` - Game decided in overtime
- `"lastPeriodType": "REG"` - Game decided in regulation

### Shootout Configuration
- `"shootoutInUse": true` - Game has shootout capability
- `"otInUse": true` - Game has overtime capability
- `"maxPeriods": 5` - Maximum periods (3 regulation + 1 OT + 1 SO)

## Usage for Testing

This game ID (2024020819) can be used for testing shootout-related functionality in the NHL data processing system, as it contains:
- Complete shootout data
- Proper period progression
- Clear winner determination
- Full play-by-play events including shootout attempts

## File Locations

- **Play-by-Play:** `/storage/20242025/json/playbyplay/2024020819.json`
- **Boxscore:** `/storage/20242025/json/boxscores/2024020819.json`
- **HTML Reports:** `/storage/20242025/html/reports/*/2024020819.HTM`

---
*Generated: $(date)*
*Data Source: NHL API 2024-25 Season*
