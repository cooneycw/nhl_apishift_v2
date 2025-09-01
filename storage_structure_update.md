# Updated Storage Structure for Shift Charts Data

## New Directory Structure

```
storage/
├── {season}/              # e.g., 20242025
│   ├── json/              # Raw JSON data from APIs
│   │   ├── boxscores/     # Game boxscore data
│   │   ├── playbyplay/    # Play-by-play data
│   │   ├── shiftcharts/   # NEW: Shift charts JSON data
│   │   │   ├── shiftchart_2024020623.json
│   │   │   ├── shiftchart_2024020624.json
│   │   │   └── ...
│   │   ├── games.json     # Season schedule data
│   │   ├── players.json   # Season player information
│   │   └── teams.json     # Season team data
│   ├── html/reports/      # HTML reports (HTM files)
│   │   ├── GS/            # Game Summary reports
│   │   ├── ES/            # Event Summary reports
│   │   ├── PL/            # Play-by-Play reports
│   │   ├── FS/            # Faceoff Summary reports
│   │   ├── FC/            # Faceoff Comparison reports
│   │   ├── RO/            # Roster reports
│   │   ├── SS/            # Shot Summary reports
│   │   ├── SC/            # Shift Chart HTML reports (existing)
│   │   ├── TV/            # Time on Ice Away reports
│   │   └── TH/            # Time on Ice Home reports
│   └── csv/curate/        # Curated data extraction targets
│       ├── game_summaries/     # Target for GS report extraction
│       ├── event_summaries/    # Target for ES report extraction
│       ├── play_by_play/       # Target for PL report extraction
│       ├── faceoff_summary/    # Target for FS report extraction
│       ├── faceoff_comparison/ # Target for FC report extraction
│       ├── rosters/            # Target for RO report extraction
│       ├── shot_summary/       # Target for SS report extraction
│       ├── time_on_ice_away/   # Target for TV report extraction
│       ├── time_on_ice_home/   # Target for TH report extraction
│       └── shift_charts/       # NEW: Target for shift charts JSON processing
│           ├── player_shifts.csv
│           ├── team_shifts.csv
│           └── game_shifts.csv
├── global/                # Cross-season data
│   ├── seasons.json       # Historical seasons list
│   └── logs/              # Application logs
└── processed/             # Cross-season processed data
```

## Shift Charts Data Structure

### JSON File Format
Each shift chart JSON file (`shiftchart_{gameId}.json`) contains:

```json
{
  "data": [
    {
      "id": 14859341,
      "detailCode": 0,
      "duration": "00:30",
      "endTime": "00:30",
      "eventDescription": null,
      "eventDetails": null,
      "eventNumber": 7,
      "firstName": "Brad",
      "gameId": 2024020623,
      "hexValue": "#111111",
      "lastName": "Marchand",
      "period": 1,
      "playerId": 8473419,
      "shiftNumber": 1,
      "startTime": "00:00",
      "teamAbbrev": "BOS",
      "teamId": 6,
      "teamName": "Boston Bruins",
      "typeCode": 517
    }
  ],
  "total": 845
}
```

### CSV Output Structure

#### player_shifts.csv
```csv
game_id,player_id,player_name,team_abbrev,total_shifts,total_time_on_ice,average_shift_length,longest_shift,shortest_shift,goals,assists,penalties,collection_timestamp
2024020623,8473419,Brad Marchand,BOS,27,19:29,00:43,01:25,00:06,2,0,0,2024-01-15T10:30:00
```

#### team_shifts.csv
```csv
game_id,team_abbrev,team_name,total_players,total_shifts,total_time_on_ice,collection_timestamp
2024020623,BOS,Boston Bruins,20,450,1200:00,2024-01-15T10:30:00
2024020623,TOR,Toronto Maple Leafs,20,445,1195:00,2024-01-15T10:30:00
```

#### game_shifts.csv
```csv
game_id,home_team_abbrev,away_team_abbrev,total_entries,home_team_shifts,away_team_shifts,home_team_toi,away_team_toi,collection_timestamp
2024020623,BOS,TOR,845,450,445,1200:00,1195:00,2024-01-15T10:30:00
```

## Data Collection Process

### 1. JSON Collection
- **Source**: `https://api.nhle.com/stats/rest/en/shiftcharts?cayenneExp=gameId={gameId}`
- **Rate Limiting**: 1 second between requests (conservative)
- **Storage**: `storage/{season}/json/shiftcharts/shiftchart_{gameId}.json`
- **Format**: Raw JSON response from NHL API

### 2. Data Processing
- **Validation**: Pydantic models ensure data integrity
- **Aggregation**: Calculate player and team statistics
- **Summary**: Generate shift summaries and time on ice calculations

### 3. CSV Export
- **Player Level**: Individual player shift statistics
- **Team Level**: Team-wide shift summaries
- **Game Level**: Complete game shift overview

## Integration with Existing Pipeline

### Updated Collection Steps
1. **step_01_collect_json** - Collect JSON data (including shift charts)
2. **step_02_collect_html** - Collect HTML reports
3. **step_03_curate** - Process and curate collected data (including shift charts)
4. **step_04_validate** - Validate data integrity and quality
5. **step_05_transform** - Transform data for analysis
6. **step_06_export** - Export data to various formats

### Configuration Updates
```python
# config/nhl_config.py
SHIFT_CHARTS_CONFIG = {
    'enabled': True,
    'rate_limit_delay': 1.0,
    'max_retries': 3,
    'timeout': 30,
    'storage_path': 'json/shiftcharts'
}
```

## Benefits of This Approach

### 1. **Data Completeness**
- Captures all shift chart data in structured JSON format
- Preserves original API response for data integrity
- Enables detailed analysis of player shifts and time on ice

### 2. **API Respect**
- Conservative rate limiting (1 second between requests)
- Proper error handling and retry logic
- Respects NHL API terms of service

### 3. **Analysis Ready**
- Structured data models for type safety
- Aggregated statistics for easy analysis
- CSV export for compatibility with analysis tools

### 4. **Integration**
- Fits seamlessly into existing project structure
- Follows established patterns and conventions
- Maintains consistency with other data sources

## Usage Examples

### Collect Shift Charts Data
```bash
# Run the shift charts collector
python src/collect/shift_charts_collector.py

# Or integrate into main pipeline
python main.py --mode step --step step_01_collect_json --seasons 20242025
```

### Process Shift Charts Data
```python
from src.model.shift_charts import parse_shift_chart_data, create_game_shift_summary

# Load and parse shift chart data
with open('storage/20242025/json/shiftcharts/shiftchart_2024020623.json', 'r') as f:
    raw_data = json.load(f)

# Parse into structured format
shift_data = parse_shift_chart_data(raw_data)

# Create game summary
game_summary = create_game_shift_summary(shift_data.data)

# Access player statistics
for player in game_summary.home_team.player_summaries:
    print(f"{player.player_name}: {player.total_time_on_ice} TOI")
```

This structure provides a comprehensive solution for collecting, storing, and analyzing NHL shift charts data while maintaining consistency with your existing project architecture.
