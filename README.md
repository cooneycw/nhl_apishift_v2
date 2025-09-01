# NHL API Data Retrieval System

A comprehensive Python-based system for collecting, processing, and storing NHL statistical data from official NHL API endpoints and supplementary HTML reports. This system focuses exclusively on **regular season games** and implements responsible API usage with rate limiting and error handling.

## 🏒 Overview

The NHL API Data Retrieval System provides a complete pipeline for extracting hockey statistics and game data from the NHL's official APIs. The system is designed with modularity, reliability, and API respect in mind, ensuring sustainable data collection practices.

### Key Features

- **Regular Season Focus**: Collects only regular season games (gameType == 2)
- **Comprehensive Data Coverage**: JSON APIs, HTML reports, and shift charts
- **Rate Limiting**: Conservative API usage with built-in rate limiting
- **Error Handling**: Robust retry logic and error recovery
- **Modular Architecture**: Step-based processing pipeline
- **Multiple Output Formats**: JSON, CSV, and structured data storage
- **High Success Rates**: 94.1% average collection success rate

## 📊 Data Sources

### JSON APIs (Primary)
- **Base URL**: `https://api-web.nhle.com`
- **Boxscores**: Detailed game statistics
- **Play-by-Play**: Complete game events and shifts
- **Team Data**: Rosters, standings, schedules
- **Player Data**: Statistics and information

### HTML Reports (Secondary)
- **Base URL**: `https://www.nhl.com/scores/htmlreports/{season}/`
- **Available Reports**: GS, ES, PL, FS, FC, RO, SS, TV, TH (9 types)
- **Time on Ice**: TV (Away team) and TH (Home team) reports
- **Storage**: HTM files in `storage/html/reports/{season}/`

### Shift Charts (Time on Ice Data)
- **URL**: `https://api.nhle.com/stats/rest/en/shiftcharts?cayenneExp=gameId={gameId}`
- **Content**: Detailed player shift information in JSON format
- **Storage**: `storage/{season}/json/shiftcharts/shiftchart_{gameId}.json`
- **Success Rate**: 100% availability
- **Current Status**: 206 files collected

## 🚀 Quick Start

### Prerequisites

- **Python**: 3.8+ (recommended: 3.10+)
- **Dependencies**: See requirements below
- **Internet**: Stable connection for API access

### Installation

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd nhl_apishift_v2
   ```

2. **Create virtual environment**:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

4. **Verify installation**:
   ```bash
   python main.py --help
   ```

### Basic Usage

#### Full Pipeline (Recommended)
```bash
# Collect all data for 2024-2025 season
python main.py --mode full --seasons 20242025 --verbose
```

#### Individual Steps
```bash
# Step 1: Collect JSON data
python main.py --mode step --step step_01_collect_json --seasons 20242025

# Step 2: Collect HTML reports
python main.py --mode step --step step_02_collect_html --seasons 20242025

# Step 3: Collect shift charts (Time on Ice data)
python src/collect/shift_charts_collector.py
```

#### Check Status
```bash
# Check collection status
python main.py --mode status --seasons 20242025
```

## 📁 Project Structure

```
nhl_apishift_v2/
├── main.py                     # Main entry point
├── collect_shift_charts.py     # Shift charts collector
├── config/                     # Configuration files
│   ├── nhl_config.py          # NHL API configuration
│   └── config.py              # Enhanced configuration system
├── src/                       # Source code modules
│   ├── collect/               # Data collection modules
│   ├── curate/                # Data processing and curation
│   ├── model/                 # Data models and schemas
│   ├── transform/             # Data transformation utilities
│   └── utils/                 # Common utilities
├── storage/                   # Data storage directory (season-first structure)
│   ├── 20242025/              # 2024-2025 season data (13,123 files)
│   │   ├── json/              # Raw JSON data from APIs (2,627+ files)
│   │   │   ├── boxscores/     # Game boxscore data (1,312 files)
│   │   │   ├── playbyplay/    # Play-by-play data (1,312 files)
│   │   │   ├── shiftcharts/   # Shift charts data (206 files)
│   │   │   ├── games.json     # Season schedule data
│   │   │   ├── players.json   # Season player information
│   │   │   └── teams.json     # Season team data (Utah, no Arizona)
│   │   ├── html/reports/      # HTML reports (HTM files) (10,496 files)
│   │   │   ├── GS/            # Game Summary (1,312 files)
│   │   │   ├── ES/            # Event Summary (1,312 files)
│   │   │   ├── PL/            # Play-by-Play (1,312 files)
│   │   │   ├── FS/            # Faceoff Summary (1,312 files)
│   │   │   ├── FC/            # Faceoff Comparison (1,312 files)
│   │   │   ├── RO/            # Rosters (1,312 files)
│   │   │   ├── SS/            # Shot Summary (1,312 files)
│   │   │   ├── SC/            # Shift Charts (1,312 files)
│   │   │   ├── TV/            # Time on Ice Away (0 files - ready for collection)
│   │   │   └── TH/            # Time on Ice Home (0 files - ready for collection)
│   │   └── csv/curate/        # Curated data extraction targets (empty - ready for processing)
│   │       ├── game_summaries/     # Target for GS report extraction
│   │       ├── event_summaries/    # Target for ES report extraction
│   │       ├── play_by_play/       # Target for PL report extraction
│   │       ├── faceoff_summary/    # Target for FS report extraction
│   │       ├── faceoff_comparison/ # Target for FC report extraction
│   │       ├── rosters/            # Target for RO report extraction
│   │       ├── shot_summary/       # Target for SS report extraction
│   │       ├── time_on_ice_away/   # Target for TV report extraction
│   │       ├── time_on_ice_home/   # Target for TH report extraction
│   │       └── shift_charts/       # Target for shift charts JSON processing
│   ├── 20232024/              # 2023-2024 season data (when collected)
│   ├── 20252026/              # 2025-2026 season data (when collected)
│   ├── global/                # Cross-season data
│   │   ├── seasons.json       # Historical seasons list
│   │   └── logs/              # Application logs
│   └── processed/             # Cross-season processed data
├── nhl_api_datastructure.mdc  # Comprehensive API documentation
├── project_specifications.mdc # Technical specifications
└── README.md                  # This file
```

## 📊 Data Management

### ⚠️ Important: Data Files Are Not Tracked

This repository **does not include data files** in version control. The `storage/` directory and all data files are excluded from git tracking for the following reasons:

- **Repository Size**: Data files can be very large (GBs)
- **Privacy**: Avoid exposing potentially sensitive data
- **Performance**: Keep repository fast to clone and work with
- **Best Practices**: Separate code from data

### Data Storage Structure

Data is stored locally in the `storage/` directory with the following structure:

```
storage/
├── {season}/              # e.g., 20242025
│   ├── csv/              # Processed CSV files
│   │   ├── curate/       # Curated data
│   │   └── ...           # Other processed data
│   ├── html/             # HTML reports
│   │   └── reports/      # NHL HTML reports
│   └── json/             # JSON API responses
│       ├── boxscores/    # Game boxscores
│       ├── playbyplay/   # Play-by-play data
│       └── ...           # Other JSON data
└── global/               # Global configuration
    ├── logs/             # Log files
    └── seasons.json      # Season metadata
```

### Data Collection

To collect data, run the collection scripts:

```bash
# Collect all data for a season
python main.py --mode full --seasons 20242025

# Or collect specific data types
python main.py --mode step --step step_01_collect_json --seasons 20242025
python main.py --mode step --step step_02_collect_html --seasons 20242025
python collect_shift_charts.py
```

### Data Backup

Since data is not tracked in git, consider:
- Regular backups of the `storage/` directory
- Using cloud storage for data backup
- Documenting data collection procedures

## ⚙️ Configuration

### Environment Variables
```bash
# Development
export NHL_ENV=development
export NHL_LOG_LEVEL=DEBUG
export NHL_MAX_WORKERS=2

# Production
export NHL_ENV=production
export NHL_LOG_LEVEL=INFO
export NHL_MAX_WORKERS=5
```

### Rate Limiting Settings
The system implements conservative rate limiting by default:
- **Request Delay**: 1 second between requests
- **Max Concurrent**: 2 concurrent requests
- **Retry Backoff**: Exponential backoff for failures
- **Timeout**: 30 seconds per request

## 📈 Performance Metrics

Based on testing of the 2024-2025 season (1,312 regular season games):

| Data Type | Success Rate | Notes |
|-----------|--------------|-------|
| Boxscores | 93.2% | Temporary API issues |
| Play-by-Play | 95.0% | Network timeouts |
| HTML Reports | 100% | All available types |
| Shift Charts | 100% | Time on Ice data |
| **Overall** | **94.1%** | **Average success rate** |

## 🔧 Advanced Usage

### Custom Configuration
```python
# config/nhl_config.py
RATE_LIMIT_SETTINGS = {
    'request_delay': 1.0,         # Seconds between requests
    'max_concurrent': 2,          # Max concurrent requests
    'retry_backoff': 5.0,         # Exponential backoff
    'max_retries': 3,             # Max retry attempts
    'timeout': 30,                # Request timeout
}
```

### Data Processing Pipeline
The system follows a step-based processing approach:

1. **step_01_collect_json** - Collect JSON data from NHL API endpoints
2. **step_02_collect_html** - Collect HTML reports from NHL.com
3. **step_03_curate** - Process and curate collected data
4. **step_04_validate** - Validate data integrity and quality
5. **step_05_transform** - Transform data for analysis (optional)
6. **step_06_export** - Export data to various formats (optional)

### Storage Formats

#### JSON (Primary)
- Raw API responses for data integrity
- Organized by season and data type
- Preserves original data structure

#### CSV (Export)
- Human-readable format for analysis
- Organized by data category
- Easy import into analysis tools

#### HTML Reports (HTM Files)
- Comprehensive game reports stored as HTM files
- Detailed statistics and information
- Structured data tables
- Organized by report type in `storage/html/reports/{season}/`

## 🛠️ Development

### Running Tests
```bash
# Run unit tests
python -m pytest tests/

# Run integration tests
python -m pytest tests/integration/

# Run with coverage
python -m pytest --cov=src tests/
```

### Code Quality
```bash
# Format code
black src/ tests/

# Lint code
flake8 src/ tests/

# Type checking
mypy src/
```

### Adding New Data Sources
1. Create new collector in `src/collect/`
2. Add configuration in `config/nhl_config.py`
3. Update pipeline in `main.py`
4. Add tests in `tests/`

## 📚 Documentation

- **[API Documentation](nhl_api_datastructure.mdc)**: Comprehensive NHL API reference
- **[Project Specifications](project_specifications.mdc)**: Technical specifications and requirements
- **[Configuration Guide](config/)**: Configuration options and examples

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Development Guidelines
- Follow PEP 8 style guidelines
- Add tests for new functionality
- Update documentation for API changes
- Ensure all tests pass before submitting

## ⚠️ Important Notes

### API Usage
- **Respect Rate Limits**: The system implements conservative rate limiting
- **Educational Purpose**: Use data for educational/research purposes
- **Attribution**: Always credit NHL as the data source
- **Terms of Service**: Comply with NHL's terms of service

### Data Availability
- **Regular Season Only**: System filters for gameType == 2
- **TV/TH Reports**: Use TV (away) and TH (home) reports for Time on Ice data
- **Success Rates**: Expect 94%+ success rates for most data types
- **Retry Logic**: Failed collections usually succeed on retry

### Troubleshooting

#### Common Issues
1. **Rate Limiting**: Reduce concurrent requests or increase delays
2. **Network Timeouts**: Check internet connection and increase timeout
3. **Missing Data**: Run retry logic for failed collections
4. **Permission Errors**: Ensure write access to storage directory

#### Getting Help
- Check the logs in `storage/logs/`
- Review the [API Documentation](nhl_api_datastructure.mdc)
- Open an issue for bugs or feature requests

## 📄 License

This project is for educational and research purposes. Please respect the NHL's terms of service and data usage policies.

## 🙏 Acknowledgments

- **NHL**: For providing comprehensive hockey data through their APIs
- **Community**: Contributors and users who help improve the system
- **Open Source**: Built on excellent Python libraries and tools

---

**Happy Hockey Data Collection! 🏒📊**
