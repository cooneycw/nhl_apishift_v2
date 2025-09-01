# Configuration Updates for Shift Charts Integration

## Overview

The `config/nhl_config.py` file has been updated to include comprehensive support for shift charts data collection. This is the **correct configuration file** being used by the shift charts collector.

## Updates Made

### 1. **API Endpoints**
Added shift charts endpoint to the `endpoints` dictionary:
```python
"shift_charts": "https://api.nhle.com/stats/rest/en/shiftcharts?cayenneExp=gameId={game_id}"
```

### 2. **File Paths**
Added shift charts storage path:
```python
"shiftcharts": os.path.join(self.storage_root, "json", "shiftcharts")
```

### 3. **Statistics Configuration**
Added shift statistics attributes:
```python
'shift_stats': [
    'total_shifts', 'total_time_on_ice', 'average_shift_length', 'longest_shift',
    'shortest_shift', 'goals', 'assists', 'penalties', 'faceoffs_won', 'faceoffs_lost'
]
```

### 4. **Default Configuration**
Added shift charts configuration to `create_default_config()`:
```python
'shift_charts': {
    'enabled': True,
    'rate_limit_delay': 1.0,  # 1 second between requests
    'max_retries': 3,
    'timeout': 30,
    'storage_path': 'json/shiftcharts'
}
```

### 5. **Helper Methods**
Added two new methods for shift charts:

#### `get_shift_charts_url(game_id: int) -> str`
Returns the complete API URL for shift charts data:
```python
def get_shift_charts_url(self, game_id: int) -> str:
    """Get shift charts API URL for a specific game."""
    return self.endpoints["shift_charts"].format(game_id=game_id)
```

#### `get_shift_charts_file_path(season: str, game_id: int) -> str`
Returns the file path for storing shift charts JSON data:
```python
def get_shift_charts_file_path(self, season: str, game_id: int) -> str:
    """Get file path for shift charts JSON data."""
    return os.path.join(
        self.file_paths["shiftcharts"],
        f"shiftchart_{game_id}.json"
    )
```

## Updated Collector Integration

The `src/collect/shift_charts_collector.py` has been updated to use the configuration:

### 1. **URL Generation**
```python
# Before
url = f"https://api.nhle.com/stats/rest/en/shiftcharts?cayenneExp=gameId={game_id}"

# After
url = self.config.get_shift_charts_url(game_id)
```

### 2. **File Path Generation**
```python
# Before
shift_charts_dir = os.path.join(self.config.file_paths["json"], "shiftcharts")
filename = f"shiftchart_{game_id}.json"
filepath = os.path.join(shift_charts_dir, filename)

# After
filepath = self.config.get_shift_charts_file_path(season, game_id)
```

### 3. **Rate Limiting Configuration**
```python
# Before
self.request_delay = 1.0  # Hard-coded
self.max_retries = 3      # Hard-coded

# After
shift_charts_config = config_dict.get('shift_charts', {})
self.request_delay = shift_charts_config.get('rate_limit_delay', 1.0)
self.max_retries = shift_charts_config.get('max_retries', 3)
```

## Configuration File Hierarchy

### Primary Configuration: `config/nhl_config.py`
- **Status**: ✅ **UPDATED** - This is the correct file
- **Usage**: Used by shift charts collector and main pipeline
- **Features**: Complete shift charts support

### Secondary Configuration: `config/config.py`
- **Status**: ⚠️ **NOT UPDATED** - This appears to be a legacy file
- **Usage**: Not used by shift charts collector
- **Action**: No updates needed

## Usage Examples

### 1. **Using Configuration in Code**
```python
from config.nhl_config import NHLConfig, create_default_config

# Create configuration
config_dict = create_default_config()
config = NHLConfig(config_dict)

# Get shift charts URL
url = config.get_shift_charts_url(2024020623)
# Returns: "https://api.nhle.com/stats/rest/en/shiftcharts?cayenneExp=gameId=2024020623"

# Get file path
filepath = config.get_shift_charts_file_path("20242025", 2024020623)
# Returns: "storage/json/shiftcharts/shiftchart_2024020623.json"
```

### 2. **Custom Configuration**
```python
config_dict = {
    'verbose': True,
    'shift_charts': {
        'enabled': True,
        'rate_limit_delay': 2.0,  # Custom delay
        'max_retries': 5,         # Custom retries
        'timeout': 45             # Custom timeout
    }
}
config = NHLConfig(config_dict)
```

## Benefits of These Updates

### 1. **Centralized Configuration**
- All shift charts settings in one place
- Consistent with existing configuration patterns
- Easy to modify and maintain

### 2. **Flexible Rate Limiting**
- Configurable request delays
- Adjustable retry settings
- API-friendly defaults

### 3. **Consistent File Management**
- Standardized file paths
- Automatic directory creation
- Consistent naming conventions

### 4. **Type Safety**
- Helper methods for URL and file path generation
- Reduced chance of errors
- Better code maintainability

## Verification

To verify the configuration is working correctly:

1. **Test URL Generation**:
   ```python
   config = NHLConfig()
   url = config.get_shift_charts_url(2024020623)
   print(url)  # Should print the correct API URL
   ```

2. **Test File Path Generation**:
   ```python
   filepath = config.get_shift_charts_file_path("20242025", 2024020623)
   print(filepath)  # Should print the correct file path
   ```

3. **Test Configuration Loading**:
   ```python
   config_dict = create_default_config()
   assert 'shift_charts' in config_dict  # Should be True
   ```

The configuration is now fully updated and ready for shift charts data collection!
