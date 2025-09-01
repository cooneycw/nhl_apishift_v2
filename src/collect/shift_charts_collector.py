#!/usr/bin/env python3
"""
NHL Shift Charts Collector
==========================

Collects shift charts data from the NHL API endpoint:
https://api.nhle.com/stats/rest/en/shiftcharts?cayenneExp=gameId={gameId}

This provides detailed player shift information including:
- Player identification and team data
- Shift timing (start, end, duration)
- Period and game context
- Event descriptions and details
- Visual color coding (hexValue)
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

class ShiftChartsCollector:
    """Collector for NHL shift charts JSON data from API."""
    
    def __init__(self, config: NHLConfig):
        """Initialize the shift charts collector."""
        self.config = config
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Safari/537.36"
        })
        
        # Rate limiting (use config if available, otherwise use defaults)
        # Get shift charts config from the config object
        shift_charts_config = getattr(config, 'shift_charts', {})
        if not shift_charts_config:
            shift_charts_config = {}
        
        self.request_delay = shift_charts_config.get('rate_limit_delay', 1.0)  # Default 1 second
        self.last_request_time = 0
        self.max_retries = shift_charts_config.get('max_retries', 3)
        self.retry_backoff = 2.0
        
        # Progress tracking
        self.total_requests = 0
        self.successful_requests = 0
        self.failed_requests = 0
        self.failed_charts = []
        
        # Setup logging
        self.logger = logging.getLogger(__name__)
    
    def _make_request(self, url: str, timeout: int = 30) -> Optional[Dict[str, Any]]:
        """Make a rate-limited request to fetch shift chart JSON data."""
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
    
    def fetch_shift_chart(self, game_id: int) -> Optional[Dict[str, Any]]:
        """Fetch shift chart JSON data for a specific game."""
        url = self.config.get_shift_charts_url(game_id)
        return self._make_request(url)
    
    def save_shift_chart(self, season: str, game_id: int, data: Dict[str, Any]) -> bool:
        """Save shift chart JSON data to file."""
        try:
            # Get file path from configuration
            filepath = self.config.get_shift_charts_file_path(season, game_id)
            
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            
            # Save file
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            
            return True
        except Exception as e:
            self.logger.error(f"Error saving shift chart: {e}")
            return False
    
    def collect_shift_chart_for_game(self, season: str, game_id: int) -> bool:
        """Collect shift chart JSON data for a specific game."""
        try:
            data = self.fetch_shift_chart(game_id)
            if data:
                success = self.save_shift_chart(season, game_id, data)
                if success:
                    self.logger.info(f"✅ Shift chart for game {game_id}")
                    return True
                else:
                    self.logger.error(f"❌ Failed to save shift chart for game {game_id}")
                    self.failed_charts.append(game_id)
                    return False
            else:
                self.logger.error(f"❌ Failed to fetch shift chart for game {game_id}")
                self.failed_charts.append(game_id)
                return False
        except Exception as e:
            self.logger.error(f"❌ Error collecting shift chart for game {game_id}: {e}")
            self.failed_charts.append(game_id)
            return False
    
    def get_progress_stats(self) -> Dict[str, Any]:
        """Get current progress statistics."""
        return {
            'total_requests': self.total_requests,
            'successful_requests': self.successful_requests,
            'failed_requests': self.failed_requests,
            'request_success_rate': round((self.successful_requests / max(self.total_requests, 1)) * 100, 1),
            'failed_charts': self.failed_charts
        }

def load_games_data(season: str) -> List[Dict[str, Any]]:
    """Load games data for a season."""
    games_file = f"storage/{season}/json/games.json"
    if os.path.exists(games_file):
        with open(games_file, 'r') as f:
            return json.load(f)
    return []

def main():
    """Main shift chart collection function."""
    print("📊 NHL Shift Charts Collection (JSON API Data)")
    print("=" * 60)
    print("Collecting shift charts JSON data from NHL API")
    print("URL pattern: https://api.nhle.com/stats/rest/en/shiftcharts?cayenneExp=gameId={game_id}")
    print("=" * 60)
    
    season = "20242025"
    
    # Load games data
    games = load_games_data(season)
    if not games:
        print(f"❌ No games data found for season {season}")
        return
    
    # Filter for regular season games
    regular_games = [game for game in games if game.get('gameType') == 2]
    print(f"📊 Found {len(regular_games)} regular season games")
    
    # Create configuration and collector
    config_dict = create_default_config()
    config_dict.update({
        'verbose': True,
        'max_workers': 2,  # Conservative for API respect
        'default_season': season
    })
    
    config = NHLConfig(config_dict)
    collector = ShiftChartsCollector(config)
    
    # Create storage directories
    config.create_storage_directories()
    
    print(f"🔄 Starting shift chart collection...")
    print(f"📈 Expected total shift charts: {len(regular_games)}")
    
    # Test with a few games first
    print(f"\n🧪 Testing with first 3 games...")
    test_games = regular_games[:3]
    
    successful_tests = 0
    for i, game in enumerate(test_games, 1):
        game_id = game['id']
        print(f"\n📊 Testing game {i}/{len(test_games)}: {game_id}")
        
        success = collector.collect_shift_chart_for_game(season, game_id)
        if success:
            successful_tests += 1
    
    print(f"\n{'='*60}")
    print("🧪 TEST COMPLETE")
    print(f"{'='*60}")
    
    stats = collector.get_progress_stats()
    print(f"📊 Test Results:")
    print(f"   Successful tests: {successful_tests}/{len(test_games)}")
    print(f"   API success rate: {stats['request_success_rate']}%")
    print(f"   Failed charts: {len(collector.failed_charts)}")
    
    if successful_tests == len(test_games):
        print(f"\n🎉 Test successful! All shift charts are accessible.")
        print(f"💡 Ready to collect all {len(regular_games)} games.")
        
        # Continue with all games
        print(f"\n🔄 Continuing with all games...")
        
        # Collect shift charts for each game
        total_charts = 0
        successful_charts = 0
        failed_charts = 0
        
        for i, game in enumerate(regular_games, 1):
            game_id = game['id']
            print(f"\n📊 Processing game {i}/{len(regular_games)}: {game_id}")
            
            success = collector.collect_shift_chart_for_game(season, game_id)
            total_charts += 1
            
            if success:
                successful_charts += 1
            else:
                failed_charts += 1
            
            # Show progress every 25 games
            if i % 25 == 0 or i == len(regular_games):
                stats = collector.get_progress_stats()
                success_rate = round((successful_charts / total_charts) * 100, 1) if total_charts > 0 else 0
                print(f"\n📈 Progress: {i}/{len(regular_games)} games processed")
                print(f"   Shift charts: {successful_charts}/{total_charts} ({success_rate}% success)")
                print(f"   API success rate: {stats['request_success_rate']}%")
        
        # Final summary
        print(f"\n{'='*60}")
        print("🎯 SHIFT CHARTS COLLECTION SUMMARY")
        print(f"{'='*60}")
        
        stats = collector.get_progress_stats()
        final_success_rate = round((successful_charts / total_charts) * 100, 1) if total_charts > 0 else 0
        
        print(f"📊 Total games processed: {len(regular_games)}")
        print(f"📊 Total shift charts attempted: {total_charts}")
        print(f"✅ Successful shift charts: {successful_charts}")
        print(f"❌ Failed shift charts: {failed_charts}")
        print(f"📈 Shift chart success rate: {final_success_rate}%")
        print(f"🌐 API success rate: {stats['request_success_rate']}%")
        
        if failed_charts > 0:
            print(f"\n⚠️  Failed shift charts:")
            for failed_chart in collector.failed_charts[:10]:  # Show first 10
                print(f"   - Game {failed_chart}")
            if len(collector.failed_charts) > 10:
                print(f"   ... and {len(collector.failed_charts) - 10} more")
        
        # Save results
        results_file = f"shift_charts_collection_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        results_data = {
            'season': season,
            'total_games': len(regular_games),
            'total_charts': total_charts,
            'successful_charts': successful_charts,
            'failed_charts': failed_charts,
            'success_rate': final_success_rate,
            'api_stats': stats,
            'failed_chart_list': collector.failed_charts
        }
        
        with open(results_file, 'w') as f:
            json.dump(results_data, f, indent=2)
        
        print(f"\n💾 Results saved to: {results_file}")
        
        if failed_charts == 0:
            print(f"\n🎉 SUCCESS! All shift charts collected successfully!")
        else:
            print(f"\n💡 Consider running the collection again for failed charts")
    
    else:
        print(f"\n⚠️  Some test shift charts failed. Check the errors above.")
        print(f"💡 You may want to investigate before collecting all games.")

if __name__ == "__main__":
    main()
