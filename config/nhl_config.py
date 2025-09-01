#!/usr/bin/env python3
"""
Standalone NHL Configuration
===========================

This module provides a standalone configuration system for NHL data retrieval,
inspired by the NHLapiV3 system but completely independent.
"""

import os
import json
from datetime import datetime
from typing import Dict, List, Any, Optional


class NHLConfig:
    """
    NHL configuration class for data retrieval system.
    
    This provides all the necessary configuration for retrieving NHL data
    from the official NHL API endpoints.
    """
    
    def __init__(self, config_dict: Dict[str, Any] = None):
        """Initialize the configuration."""
        if config_dict is None:
            config_dict = {}
        
        # Basic settings
        self.verbose = config_dict.get('verbose', False)
        self.produce_csv = config_dict.get('produce_csv', True)
        self.current_date = datetime.now().date()
        self.max_workers = config_dict.get('max_workers', 5)  # Optimized concurrency for better throughput
        
        # NHL API endpoints
        self.base_url = "https://api-web.nhle.com"
        self.base_url_reports = "https://www.nhl.com"
        
        # Request headers - API-friendly identification
        self.headers = {
            "User-Agent": "NHL-Data-Retrieval-System/1.0 (Educational/Research Purpose)",
            "Accept": "application/json",
            "Accept-Encoding": "gzip, deflate"
        }
        
        # API endpoints
        self.endpoints = {
            # Core data endpoints
            "seasons": "{base_url}/v1/season",
            "standings": "{base_url}/v1/standings/now",
            "schedule": "{base_url}/v1/club-schedule-season/{team}/{season}",
            "boxscore": "{base_url}/v1/gamecenter/{game_id}/boxscore",
            "right_rail": "{base_url}/v1/gamecenter/{game_id}/right-rail",
            "player": "{base_url}/v1/player/{player_id}/landing",
            "roster": "{base_url}/v1/roster/{team}/current",
            "plays": "{base_url}/v1/gamecenter/{game_id}/play-by-play",
            
            # Shift charts endpoint
            "shift_charts": "https://api.nhle.com/stats/rest/en/shiftcharts?cayenneExp=gameId={game_id}",
            
            # HTML report endpoints
            "html_game_summary": "{base_url_reports}/scores/htmlreports/{season}/GS{game_id}.HTM",
            "html_event_summary": "{base_url_reports}/scores/htmlreports/{season}/ES{game_id}.HTM",
            "html_play_summary": "{base_url_reports}/scores/htmlreports/{season}/PL{game_id}.HTM",
            "html_face_off_summary": "{base_url_reports}/scores/htmlreports/{season}/FS{game_id}.HTM",
            "html_face_off_comparison": "{base_url_reports}/scores/htmlreports/{season}/FC{game_id}.HTM",
            "html_roster_report": "{base_url_reports}/scores/htmlreports/{season}/RO{game_id}.HTM",
            "html_shot_summary": "{base_url_reports}/scores/htmlreports/{season}/SS{game_id}.HTM",
            "html_time_on_ice_away": "{base_url_reports}/scores/htmlreports/{season}/TV{game_id}.HTM",
            "html_time_on_ice_home": "{base_url_reports}/scores/htmlreports/{season}/TH{game_id}.HTM"
        }
        
        # Report types mapping
        self.html_report_types = {
            'GS': 'Game Summary',
            'ES': 'Event Summary',
            'PL': 'Play Summary',
            'FS': 'Face-off Summary',
            'FC': 'Face-off Comparison',
            'RO': 'Roster Report',
            'SS': 'Shot Summary',
            'TV': 'Time on Ice Away',
            'TH': 'Time on Ice Home'
        }
        
        # File paths setup
        self.current_path = os.getcwd()
        self.storage_root = config_dict.get('storage_root', os.path.join(self.current_path, "storage"))
        
        self.file_paths = {
            # JSON data storage
            "seasons": os.path.join(self.storage_root, "json", "seasons.json"),
            "teams": os.path.join(self.storage_root, "json", "teams.json"),
            "games": os.path.join(self.storage_root, "json", "games"),
            "boxscores": os.path.join(self.storage_root, "json", "boxscores"),
            "players": os.path.join(self.storage_root, "json", "players"),
            "playbyplay": os.path.join(self.storage_root, "json", "playbyplay"),
            # Shiftcharts uses season-first structure; avoid creating a conflicting json/shiftcharts root
            "shiftcharts": self.storage_root,
            
            # HTML reports storage
            "html_reports": os.path.join(self.storage_root, "html", "reports"),
            
            # CSV exports
            "csv_exports": os.path.join(self.storage_root, "csv"),
            
            # Processed data
            "processed": os.path.join(self.storage_root, "processed"),
            
            # Logs
            "logs": os.path.join(self.storage_root, "logs")
        }
        
        # Season configuration
        self.season_count = config_dict.get('season_count', 10)
        self.default_season = config_dict.get('default_season', '20242025')
        
        # Update flags
        self.full_update = config_dict.get('full_update', False)
        self.update_game_statuses = config_dict.get('update_game_statuses', True)
        
        # Event and shift registries (from NHLapiV3)
        self.event_types = self._get_event_registry()
        self.shift_types = self._get_shift_registry()
        
        # Statistics configuration
        self.stat_attributes = {
            'team_stats': [
                'win', 'loss', 'faceoff_taken', 'faceoff_won', 'shot_attempt', 'shot_missed',
                'shot_blocked', 'shot_on_goal', 'shot_saved', 'shot_missed_shootout',
                'goal', 'goal_against', 'giveaways', 'takeaways', 'hit_another_player',
                'hit_by_player', 'penalties', 'penalties_served', 'penalties_drawn',
                'penalty_shot', 'penalty_shot_goal', 'penalty_shot_saved', 'penalties_duration'
            ],
            'player_stats': [
                'toi', 'faceoff_taken', 'faceoff_won', 'shot_attempt', 'shot_missed',
                'shot_blocked', 'shot_on_goal', 'shot_saved', 'shot_missed_shootout',
                'goal', 'assist', 'point', 'goal_against', 'giveaways', 'takeaways',
                'hit_another_player', 'hit_by_player', 'penalties', 'penalties_served',
                'penalties_drawn', 'penalty_shot', 'penalty_shot_goal', 'penalty_shot_saved',
                'penalties_duration'
            ],
            'shift_stats': [
                'total_shifts', 'total_time_on_ice', 'average_shift_length', 'longest_shift',
                'shortest_shift', 'goals', 'assists', 'penalties', 'faceoffs_won', 'faceoffs_lost'
            ]
        }
    
    def get_endpoint(self, key: str, **kwargs) -> str:
        """
        Construct and return the full URL for a given endpoint key.
        
        Args:
            key: Endpoint key
            **kwargs: Parameters to substitute in the endpoint template
            
        Returns:
            Full URL for the endpoint
        """
        endpoint_template = self.endpoints[key]
        return endpoint_template.format(
            base_url=self.base_url,
            base_url_reports=self.base_url_reports,
            **kwargs
        )
    
    def create_storage_directories(self) -> None:
        """Create core storage directories (season-specific dirs are created lazily)."""
        # Only create truly stable, non-season directories here
        stable_dirs = [
            self.storage_root,
            self.file_paths["processed"],
            self.file_paths["logs"],
        ]
        for directory in stable_dirs:
            os.makedirs(directory, exist_ok=True)
    
    def get_season_file_path(self, season: str, data_type: str, game_id: str = None) -> str:
        """
        Build season-first file paths: storage/{season}/json/{data_type}/... or storage/{season}/json/{data_type}.json
        """
        season_json_root = os.path.join(self.storage_root, season, "json")
        # Datasets stored as a single JSON file per season
        if game_id is None and data_type in {"games", "players", "teams"}:
            return os.path.join(season_json_root, f"{data_type}.json")
        # Game-scoped datasets live under a subdirectory
        if game_id is not None:
            return os.path.join(season_json_root, data_type, f"{game_id}.json")
        # Fallback for other season-scoped resources (rare)
        return os.path.join(season_json_root, f"{data_type}.json")
    
    def get_html_report_path(self, season: str, report_type: str, game_id: str) -> str:
        """
        Get file path for HTML reports.
        
        Args:
            season: Season identifier
            report_type: Report type (GS, ES, PL, etc.)
            game_id: Game ID
            
        Returns:
            File path for the HTML report
        """
        return os.path.join(
            self.storage_root,
            season,
            "html",
            "reports",
            report_type,
            f"{report_type}{game_id}.HTM"
        )
    
    def get_shift_charts_url(self, game_id: int) -> str:
        """
        Get shift charts API URL for a specific game.
        
        Args:
            game_id: Game ID
            
        Returns:
            Shift charts API URL
        """
        return self.endpoints["shift_charts"].format(game_id=game_id)
    
    def get_shift_charts_file_path(self, season: str, game_id: int) -> str:
        """
        Get file path for shift charts JSON data.
        
        Args:
            season: Season identifier
            game_id: Game ID
            
        Returns:
            File path for the shift charts JSON file
        """
        return os.path.join(
            self.storage_root,
            season,
            "json",
            "shiftcharts",
            f"shiftchart_{game_id}.json"
        )
    
    def get_recent_seasons(self, count: int = None) -> List[str]:
        """
        Get list of recent seasons.
        
        Args:
            count: Number of seasons to return (defaults to config season_count)
            
        Returns:
            List of season identifiers in descending order (most recent first)
        """
        if count is None:
            count = self.season_count
            
        current_year = datetime.now().year
        seasons = []
        
        for i in range(count):
            start_year = current_year - i
            season_id = f"{start_year}{start_year + 1}"
            seasons.append(season_id)
        
        return seasons
    
    def format_game_id(self, game_id: int) -> str:
        """
        Format game ID for HTML reports (zero-padded to 6 digits).
        
        Args:
            game_id: Integer game ID
            
        Returns:
            Formatted game ID string
        """
        return f"{game_id:06d}"
    
    def _get_event_registry(self) -> Dict[int, Dict[str, Any]]:
        """Get event type registry."""
        return {
            502: {'event_name': 'faceoff', 'sport_stat': True},
            503: {'event_name': 'hit', 'sport_stat': True},
            504: {'event_name': 'giveaway', 'sport_stat': True},
            505: {'event_name': 'goal', 'sport_stat': True},
            506: {'event_name': 'shot-on-goal', 'sport_stat': True},
            507: {'event_name': 'missed-shot', 'sport_stat': True},
            508: {'event_name': 'blocked-shot', 'sport_stat': True},
            509: {'event_name': 'penalty', 'sport_stat': True},
            510: {'event_name': '', 'sport_stat': True},
            516: {'event_name': 'stoppage', 'sport_stat': True},
            520: {'event_name': 'game-start', 'sport_stat': False},
            521: {'event_name': 'period-end', 'sport_stat': True},
            523: {'event_name': 'shootout-complete', 'sport_stat': False},
            524: {'event_name': 'game-end', 'sport_stat': False},
            525: {'event_name': 'takeaway', 'sport_stat': True},
            535: {'event_name': 'delayed-penalty', 'sport_stat': True},
            537: {'event_name': 'penalty-shot-missed', 'sport_stat': True}
        }
    
    def _get_shift_registry(self) -> Dict[str, Dict[str, Any]]:
        """Get shift type registry."""
        return {
            'PGSTR': {'shift_name': 'pregame-start', 'sport_stat': False},
            'PGEND': {'shift_name': 'pregame-end', 'sport_stat': False},
            'ANTHEM': {'shift_name': 'pregame-anthem', 'sport_stat': False},
            'PSTR': {'shift_name': 'period-start', 'sport_stat': False},
            'FAC': {'shift_name': 'faceoff', 'sport_stat': True},
            'HIT': {'shift_name': 'hit', 'sport_stat': True},
            'GIVE': {'shift_name': 'giveaway', 'sport_stat': True},
            'GOAL': {'shift_name': 'goal', 'sport_stat': True},
            'SHOT': {'shift_name': 'shot-on-goal', 'sport_stat': True},
            'MISS': {'shift_name': 'missed-shot', 'sport_stat': True},
            'BLOCK': {'shift_name': 'blocked-shot', 'sport_stat': True},
            'PENL': {'shift_name': 'penalty', 'sport_stat': True},
            'STOP': {'shift_name': 'stoppage', 'sport_stat': True},
            'CHL': {'shift_name': 'stoppage', 'sport_stat': False},
            'PEND': {'shift_name': 'period-end', 'sport_stat': True},
            'EISTR': {'shift_name': 'game-end', 'sport_stat': False},
            'EIEND': {'shift_name': 'game-end', 'sport_stat': False},
            'SOC': {'shift_name': 'shootout-complete', 'sport_stat': False},
            'TAKE': {'shift_name': 'takeaway', 'sport_stat': True},
            'SPC': {'shift_name': 'unknown', 'sport_stat': False},
            'GEND': {'shift_name': 'game-end', 'sport_stat': False},
            'DELPEN': {'shift_name': 'delayed-penalty', 'sport_stat': True}
        }


def create_default_config() -> Dict[str, Any]:
    """Create default configuration dictionary."""
    return {
        'verbose': False,
        'produce_csv': True,
        'season_count': 10,
        'default_season': '20242025',
        'max_workers': 5,  # API-friendly concurrency
        'full_update': False,
        'update_game_statuses': True,
        'storage_root': os.path.join(os.getcwd(), "storage"),
        
        # Shift charts configuration
        'shift_charts': {
            'enabled': True,
            'rate_limit_delay': 1.0,  # 1 second between requests
            'max_retries': 3,
            'timeout': 30,
            'storage_path': 'json/shiftcharts'
        }
    }
