#!/usr/bin/env python3
"""
NHL JSON Data Collector
=======================

Step 1 of the NHL data collection pipeline.
Collects JSON data from NHL API endpoints including:
- Boxscores
- Play-by-play data
- Shift charts (NEW)
- Team and player information
"""

import json
import os
import sys
import time
import requests
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime
import logging

# Add the project root to the path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from config.nhl_config import NHLConfig, create_default_config
from src.collect.shift_charts_collector import ShiftChartsCollector

class NHLJSONCollector:
    """Collector for NHL JSON data from API endpoints."""
    
    def __init__(self, config: NHLConfig):
        """Initialize the JSON collector."""
        self.config = config
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Safari/537.36"
        })
        
        # Rate limiting (conservative)
        self.request_delay = 1.0  # 1 second between requests
        self.last_request_time = 0
        self.max_retries = 3
        self.retry_backoff = 2.0
        
        # Progress tracking
        self.total_requests = 0
        self.successful_requests = 0
        self.failed_requests = 0
        
        # Setup logging
        self.logger = logging.getLogger(__name__)
        
        # Initialize shift charts collector
        self.shift_charts_collector = ShiftChartsCollector(config)
    
    def _make_request(self, url: str, timeout: int = 30) -> Optional[Dict[str, Any]]:
        """Make a rate-limited request to fetch JSON data."""
        for attempt in range(self.max_retries + 1):
            # Rate limiting
            current_time = time.time()
            time_since_last = current_time - self.last_request_time
            if time_since_last < self.request_delay:
                sleep_time = self.request_delay - time_since_last
                time.sleep(sleep_time)
            
            try:
                self.total_requests += 1
                response = self.session.get(url, timeout=timeout)
                
                # Handle rate limiting
                if response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', self.retry_backoff * (2 ** attempt)))
                    self.logger.warning(f"Rate limited - waiting {retry_after}s")
                    time.sleep(retry_after)
                    continue
                
                # Handle server errors with retry
                if response.status_code >= 500:
                    if attempt < self.max_retries:
                        wait_time = self.retry_backoff * (1.5 ** attempt)
                        self.logger.warning(f"Server error {response.status_code}, retrying in {wait_time:.1f}s (attempt {attempt + 1}/{self.max_retries})")
                        time.sleep(wait_time)
                        continue
                    else:
                        self.failed_requests += 1
                        self.logger.error(f"Server error {response.status_code} after {self.max_retries} retries")
                        return None
                
                # Raise for other 4xx errors
                response.raise_for_status()
                
                # Parse JSON response
                data = response.json()
                
                self.last_request_time = time.time()
                self.successful_requests += 1
                return data
                
            except requests.exceptions.Timeout:
                if attempt < self.max_retries:
                    wait_time = self.retry_backoff * (1.5 ** attempt)
                    self.logger.warning(f"Request timeout, retrying in {wait_time:.1f}s (attempt {attempt + 1}/{self.max_retries})")
                    time.sleep(wait_time)
                    continue
                else:
                    self.failed_requests += 1
                    self.logger.error(f"Request timeout after {self.max_retries} retries")
                    return None
                    
            except requests.exceptions.ConnectionError:
                if attempt < self.max_retries:
                    wait_time = self.retry_backoff * (1.5 ** attempt)
                    self.logger.warning(f"Connection error, retrying in {wait_time:.1f}s (attempt {attempt + 1}/{self.max_retries})")
                    time.sleep(wait_time)
                    continue
                else:
                    self.failed_requests += 1
                    self.logger.error(f"Connection error after {self.max_retries} retries")
                    return None
                    
            except requests.exceptions.RequestException as e:
                self.failed_requests += 1
                self.logger.error(f"Request failed: {e}")
                return None
            except json.JSONDecodeError as e:
                self.failed_requests += 1
                self.logger.error(f"JSON decode error: {e}")
                return None
        
        return None
    
    def save_json_data(self, data: Dict[str, Any], filepath: str) -> bool:
        """Save JSON data to file."""
        try:
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            return True
        except Exception as e:
            self.logger.error(f"Error saving JSON data: {e}")
            return False
    
    def collect_boxscore(self, game_id: int, season: str) -> bool:
        """Collect boxscore data for a specific game."""
        try:
            url = f"https://api-web.nhle.com/v1/gamecenter/{game_id}/boxscore"
            data = self._make_request(url)
            
            if data:
                filepath = self.config.get_season_file_path(season, "boxscores", str(game_id))
                success = self.save_json_data(data, filepath)
                if success:
                    self.logger.info(f"✅ Boxscore for game {game_id}")
                    return True
                else:
                    self.logger.error(f"❌ Failed to save boxscore for game {game_id}")
                    return False
            else:
                self.logger.error(f"❌ Failed to fetch boxscore for game {game_id}")
                return False
        except Exception as e:
            self.logger.error(f"❌ Error collecting boxscore for game {game_id}: {e}")
            return False
    
    def collect_playbyplay(self, game_id: int, season: str) -> bool:
        """Collect play-by-play data for a specific game."""
        try:
            url = f"https://api-web.nhle.com/v1/gamecenter/{game_id}/play-by-play"
            data = self._make_request(url)
            
            if data:
                filepath = self.config.get_season_file_path(season, "playbyplay", str(game_id))
                success = self.save_json_data(data, filepath)
                if success:
                    self.logger.info(f"✅ Play-by-play for game {game_id}")
                    return True
                else:
                    self.logger.error(f"❌ Failed to save play-by-play for game {game_id}")
                    return False
            else:
                self.logger.error(f"❌ Failed to fetch play-by-play for game {game_id}")
                return False
        except Exception as e:
            self.logger.error(f"❌ Error collecting play-by-play for game {game_id}: {e}")
            return False
    
    def collect_shift_charts(self, game_id: int, season: str) -> bool:
        """Collect shift charts data for a specific game."""
        return self.shift_charts_collector.collect_shift_chart_for_game(season, game_id)
    
    def collect_gamecenter_landing(self, game_id: int, season: str) -> bool:
        """Collect gamecenter landing data for a specific game."""
        try:
            url = self.config.get_endpoint("gamecenter_landing", game_id=game_id)
            data = self._make_request(url)
            
            if data:
                filepath = self.config.get_gamecenter_landing_file_path(season, game_id)
                success = self.save_json_data(data, filepath)
                if success:
                    self.logger.info(f"✅ Gamecenter landing for game {game_id}")
                    return True
                else:
                    self.logger.error(f"❌ Failed to save gamecenter landing for game {game_id}")
                    return False
            else:
                self.logger.error(f"❌ Failed to fetch gamecenter landing for game {game_id}")
                return False
        except Exception as e:
            self.logger.error(f"❌ Error collecting gamecenter landing for game {game_id}: {e}")
            return False
    
    def collect_team_data(self, season: str) -> bool:
        """Collect team data for a season."""
        try:
            url = f"https://api-web.nhle.com/v1/standings/now"
            data = self._make_request(url)
            
            if data:
                filepath = self.config.get_season_file_path(season, "teams")
                success = self.save_json_data(data, filepath)
                if success:
                    self.logger.info(f"✅ Team data for season {season}")
                    return True
                else:
                    self.logger.error(f"❌ Failed to save team data for season {season}")
                    return False
            else:
                self.logger.error(f"❌ Failed to fetch team data for season {season}")
                return False
        except Exception as e:
            self.logger.error(f"❌ Error collecting team data for season {season}: {e}")
            return False
    
    def collect_player_data(self, season: str) -> bool:
        """Collect player data for a season."""
        try:
            # This would need to be implemented based on available player endpoints
            # For now, we'll use a placeholder
            self.logger.info(f"ℹ️  Player data collection not yet implemented for season {season}")
            return True
        except Exception as e:
            self.logger.error(f"❌ Error collecting player data for season {season}: {e}")
            return False
    
    def collect_games_data(self, season: str) -> bool:
        """Collect games schedule data for a season using the existing DataCollector."""
        try:
            self.logger.info(f"🔄 Collecting games data for season {season}")
            
            # Use the existing working DataCollector
            from src.collect.data_collector import DataCollector
            data_collector = DataCollector(self.config)
            
            # First collect teams data
            teams_data = data_collector.collect_teams()
            if not teams_data:
                self.logger.error(f"❌ Failed to collect teams data for season {season}")
                return False
            
            # Collect games using the working method
            games_data = data_collector.collect_games_for_season(season, teams_data)
            if not games_data:
                self.logger.error(f"❌ Failed to collect games data for season {season}")
                return False
            
            # Save games data
            games_file = self.config.get_season_file_path(season, "games")
            os.makedirs(os.path.dirname(games_file), exist_ok=True)
            
            with open(games_file, 'w', encoding='utf-8') as f:
                json.dump(games_data, f, indent=2, ensure_ascii=False)
            
            self.logger.info(f"✅ Saved {len(games_data)} games to {games_file}")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Error collecting games data for season {season}: {e}")
            return False
    
    def collect_all_for_game(self, game_id: int, season: str) -> Dict[str, bool]:
        """Collect all JSON data for a specific game."""
        results = {
            'boxscore': False,
            'playbyplay': False,
            'shift_charts': False,
            'gamecenter_landing': False
        }
        
        # Collect boxscore
        results['boxscore'] = self.collect_boxscore(game_id, season)
        
        # Collect play-by-play
        results['playbyplay'] = self.collect_playbyplay(game_id, season)
        
        # Collect shift charts
        results['shift_charts'] = self.collect_shift_charts(game_id, season)
        
        # Collect gamecenter landing
        results['gamecenter_landing'] = self.collect_gamecenter_landing(game_id, season)
        
        return results
    
    def collect_all_for_season(self, season: str, games: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Collect all JSON data for a season."""
        self.logger.info(f"🔄 Starting JSON collection for season {season}")
        self.logger.info(f"📊 Found {len(games)} games to process")
        
        # Filter for regular season games
        regular_games = [game for game in games if game.get('gameType') == 2]
        self.logger.info(f"📊 Found {len(regular_games)} regular season games")
        
        # Collect season-level data
        self.logger.info("📊 Collecting season-level data...")
        team_success = self.collect_team_data(season)
        player_success = self.collect_player_data(season)
        games_success = self.collect_games_data(season)
        
        # Collect game-level data
        self.logger.info("📊 Collecting game-level data...")
        game_results = {
            'game_level': {
                'boxscore': {'successful': 0, 'failed': 0},
                'gamecenter_landing': {'successful': 0, 'failed': 0}
            },
            'shift_level': {
                'playbyplay': {'successful': 0, 'failed': 0},
                'shift_charts': {'successful': 0, 'failed': 0}
            }
        }
        
        for i, game in enumerate(regular_games, 1):
            game_id = game['id']
            
            # Show progress every 100 games
            if i % 100 == 0 or i == 1:
                self.logger.info(f"📊 Processing game {i}/{len(regular_games)}: {game_id}")
            
            results = self.collect_all_for_game(game_id, season)
            
            # Update counters for nested structure
            for data_type, success in results.items():
                if data_type in ['boxscore', 'gamecenter_landing']:
                    if success:
                        game_results['game_level'][data_type]['successful'] += 1
                    else:
                        game_results['game_level'][data_type]['failed'] += 1
                elif data_type in ['playbyplay', 'shift_charts']:
                    if success:
                        game_results['shift_level'][data_type]['successful'] += 1
                    else:
                        game_results['shift_level'][data_type]['failed'] += 1
            
            # Show progress summary every 100 games
            if i % 100 == 0:
                self.logger.info(f"📈 Progress: {i}/{len(regular_games)} games processed")
                for data_type, counts in game_results['game_level'].items():
                    total = counts['successful'] + counts['failed']
                    success_rate = round((counts['successful'] / total) * 100, 1) if total > 0 else 0
                    self.logger.info(f"   {data_type.title()}: {counts['successful']}/{total} ({success_rate}% success)")
                for data_type, counts in game_results['shift_level'].items():
                    total = counts['successful'] + counts['failed']
                    success_rate = round((counts['successful'] / total) * 100, 1) if total > 0 else 0
                    self.logger.info(f"   {data_type.title()}: {counts['successful']}/{total} ({success_rate}% success)")
        
        # Final summary
        self.logger.info(f"\n{'='*60}")
        self.logger.info("🎯 JSON COLLECTION SUMMARY")
        self.logger.info(f"{'='*60}")
        
        self.logger.info(f"📊 Season-level data:")
        self.logger.info(f"   Teams: {'✅' if team_success else '❌'}")
        self.logger.info(f"   Players: {'✅' if player_success else '❌'}")
        self.logger.info(f"   Games: {'✅' if games_success else '❌'}")
        
        self.logger.info(f"📊 Game-level data:")
        for data_type, counts in game_results['game_level'].items():
            total = counts['successful'] + counts['failed']
            success_rate = round((counts['successful'] / total) * 100, 1) if total > 0 else 0
            self.logger.info(f"   {data_type.title()}: {counts['successful']}/{total} ({success_rate}% success)")
        
        self.logger.info(f"📊 Shift-level data:")
        for data_type, counts in game_results['shift_level'].items():
            total = counts['successful'] + counts['failed']
            success_rate = round((counts['successful'] / total) * 100, 1) if total > 0 else 0
            self.logger.info(f"   {data_type.title()}: {counts['successful']}/{total} ({success_rate}% success)")
        
        self.logger.info(f"🌐 Overall API success rate: {self.get_api_success_rate()}%")
        
        return {
            'season': season,
            'total_games': len(regular_games),
            'season_data': {
                'teams': team_success,
                'players': player_success,
                'games': games_success
            },
            'game_data': game_results,
            'api_stats': {
                'total_requests': self.total_requests,
                'successful_requests': self.successful_requests,
                'failed_requests': self.failed_requests,
                'success_rate': self.get_api_success_rate()
            }
        }
    
    def get_api_success_rate(self) -> float:
        """Get API request success rate."""
        return round((self.successful_requests / max(self.total_requests, 1)) * 100, 1)

def load_games_data(season: str) -> List[Dict[str, Any]]:
    """Load games data for a season."""
    games_file = f"storage/{season}/json/games.json"
    if os.path.exists(games_file):
        with open(games_file, 'r') as f:
            return json.load(f)
    return []

def main():
    """Main JSON collection function."""
    print("📊 NHL JSON Data Collection")
    print("=" * 60)
    print("Step 1: Collecting JSON data from NHL API endpoints")
    print("Includes: Boxscores, Play-by-play, Shift charts, Team/Player data")
    print("=" * 60)
    
    season = "20242025"
    
    # Load games data
    games = load_games_data(season)
    if not games:
        print(f"❌ No games data found for season {season}")
        return
    
    # Create configuration and collector
    config_dict = create_default_config()
    config_dict.update({
        'verbose': True,
        'max_workers': 2,  # Conservative for API respect
        'default_season': season
    })
    
    config = NHLConfig(config_dict)
    collector = NHLJSONCollector(config)
    
    # Create storage directories
    config.create_storage_directories()
    
    # Collect all data
    results = collector.collect_all_for_season(season, games)
    
    # Save results
    results_file = f"json_collection_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(results_file, 'w') as f:
        json.dump(results, f, indent=2)
    
    print(f"\n💾 Results saved to: {results_file}")
    print(f"\n🎉 JSON collection complete!")

if __name__ == "__main__":
    main()
