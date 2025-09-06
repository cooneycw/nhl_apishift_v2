#!/usr/bin/env python3
"""
CSV Storage Manager for NHL Data Retrieval System
================================================

This module provides CSV-based storage management for all NHL datasets,
following the structure outlined in the API documentation. CSV storage
provides human-readable, version-controlled, and easily accessible data.
"""

import csv
import json
import os
import pickle
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional, Union
import pandas as pd
import logging


class CSVStorageManager:
    """
    Manages CSV-based storage for all NHL datasets.
    
    Provides human-readable, version-controlled storage with the following benefits:
    - Human readable: Easy to inspect, debug, and understand data
    - Version control friendly: Can be tracked in Git with diff capabilities
    - Universal compatibility: Works with any programming language or tool
    - Easy import/export: Simple to load into databases, spreadsheets, or analysis tools
    - Incremental updates: Can append new data without full file replacement
    - Data validation: Easy to implement schema validation and data quality checks
    """
    
    def __init__(self, config):
        """Initialize the CSV storage manager."""
        self.config = config
        self.logger = logging.getLogger('CSVStorage')
        
        # Define season-centric storage directory structure roots (actual season dirs created lazily)
        self.base_storage_path = Path("storage")  # we'll always prefix with season/json or season/csv
        self.html_storage_path = Path("storage")
        
        # Create directory structure
        self._create_storage_directories()
        
        # Define CSV file paths
        self.csv_paths = {
            # Seasons data
            'seasons': self.base_storage_path / "seasons" / "seasons.csv",
            'season_metadata': self.base_storage_path / "seasons" / "season_metadata.csv",
            
            # Teams data
            'teams': self.base_storage_path / "teams" / "teams.csv",
            'team_standings': self.base_storage_path / "teams" / "team_standings.csv",
            'team_rosters': self.base_storage_path / "teams" / "team_rosters.csv",
            
            # Games data
            'game_schedule': self.base_storage_path / "games" / "game_schedule.csv",
            'game_results': self.base_storage_path / "games" / "game_results.csv",
            'game_metadata': self.base_storage_path / "games" / "game_metadata.csv",
            
            # Players data
            'player_info': self.base_storage_path / "players" / "player_info.csv",
            'player_stats': self.base_storage_path / "players" / "player_stats.csv",
            'player_game_stats': self.base_storage_path / "players" / "player_game_stats.csv",
            'player_names': self.base_storage_path / "players" / "player_names.csv",
            
            # Events data
            'play_by_play': self.base_storage_path / "events" / "play_by_play.csv",
            'shifts': self.base_storage_path / "events" / "shifts.csv",
            'events': self.base_storage_path / "events" / "events.csv",
            
            # Statistics data
            'team_stats': self.base_storage_path / "statistics" / "team_stats.csv",
            'player_stats_detailed': self.base_storage_path / "statistics" / "player_stats.csv",
            'game_stats': self.base_storage_path / "statistics" / "game_stats.csv",
            
            # Curated data
            'game_curated': self.base_storage_path / "curated" / "game_curated.csv",
            'player_curated': self.base_storage_path / "curated" / "player_curated.csv",
            'team_curated': self.base_storage_path / "curated" / "team_curated.csv",
        }
    
    def _create_storage_directories(self):
        """Create the complete directory structure for data storage."""
        # Only ensure the top-level storage root exists here; season paths are created when saving
        Path("storage").mkdir(parents=True, exist_ok=True)
    
    def save_seasons_data(self, seasons_data: List[Dict[str, Any]]) -> None:
        """Save seasons data to CSV files."""
        if not seasons_data:
            return
        
        # Save basic seasons data
        seasons_df = pd.DataFrame(seasons_data)
        seasons_df['last_updated'] = datetime.now().isoformat()
        seasons_df.to_csv(self.csv_paths['seasons'], index=False)
        
        # Generate and save season metadata
        metadata = []
        for season in seasons_data:
            season_meta = {
                'season_id': season.get('id'),
                'season_name': season.get('name'),
                'season_type': season.get('type'),
                'start_date': None,  # Would need to be calculated
                'end_date': None,    # Would need to be calculated
                'is_active': season.get('type') == 'regular',
                'last_updated': datetime.now().isoformat()
            }
            metadata.append(season_meta)
        
        metadata_df = pd.DataFrame(metadata)
        metadata_df.to_csv(self.csv_paths['season_metadata'], index=False)
        
        self.logger.info(f"Saved {len(seasons_data)} seasons to CSV")
    
    def save_teams_data(self, teams_data: List[Dict[str, Any]]) -> None:
        """Save teams data to CSV files."""
        if not teams_data:
            return
        
        # Save basic teams data
        teams_df = pd.DataFrame(teams_data)
        teams_df['last_updated'] = datetime.now().isoformat()
        teams_df.to_csv(self.csv_paths['teams'], index=False)
        
        self.logger.info(f"Saved {len(teams_data)} teams to CSV")
    
    def save_team_standings(self, standings_data: List[Dict[str, Any]]) -> None:
        """Save team standings data to CSV."""
        if not standings_data:
            return
        
        standings_df = pd.DataFrame(standings_data)
        standings_df['last_updated'] = datetime.now().isoformat()
        standings_df.to_csv(self.csv_paths['team_standings'], index=False)
        
        self.logger.info(f"Saved {len(standings_data)} team standings to CSV")
    
    def save_games_data(self, games_data: List[Dict[str, Any]]) -> None:
        """Save games data to CSV files."""
        if not games_data:
            return
        
        # Separate games into different CSV files based on data type
        schedule_data = []
        results_data = []
        metadata_data = []
        
        for game in games_data:
            # Game schedule data
            schedule_row = {
                'game_id': game.get('id'),
                'season_id': game.get('season_id'),
                'game_date': game.get('game_date'),
                'home_team_id': game.get('home_team_id'),
                'away_team_id': game.get('away_team_id'),
                'home_team_abbrev': game.get('home_team_abbrev'),
                'away_team_abbrev': game.get('away_team_abbrev'),
                'game_type': game.get('game_type'),
                'game_state': game.get('game_state'),
                'venue': game.get('venue'),
                'attendance': game.get('attendance'),
                'last_updated': datetime.now().isoformat()
            }
            schedule_data.append(schedule_row)
            
            # Game results data (if available)
            if game.get('home_goals') is not None:
                results_row = {
                    'game_id': game.get('id'),
                    'home_goals': game.get('home_goals'),
                    'away_goals': game.get('away_goals'),
                    'home_sog': game.get('home_sog'),
                    'away_sog': game.get('away_sog'),
                    'home_pp_goals': game.get('home_pp_goals'),
                    'away_pp_goals': game.get('away_pp_goals'),
                    'home_pp_attempts': game.get('home_pp_attempts'),
                    'away_pp_attempts': game.get('away_pp_attempts'),
                    'home_pim': game.get('home_pim'),
                    'away_pim': game.get('away_pim'),
                    'home_hits': game.get('home_hits'),
                    'away_hits': game.get('away_hits'),
                    'home_blocks': game.get('home_blocks'),
                    'away_blocks': game.get('away_blocks'),
                    'home_faceoffs_won': game.get('home_faceoffs_won'),
                    'away_faceoffs_won': game.get('away_faceoffs_won'),
                    'home_faceoffs_total': game.get('home_faceoffs_total'),
                    'away_faceoffs_total': game.get('away_faceoffs_total'),
                    'last_updated': datetime.now().isoformat()
                }
                results_data.append(results_row)
            
            # Game metadata
            metadata_row = {
                'game_id': game.get('id'),
                'playbyplay_url': game.get('playbyplay'),
                'game_summary_url': game.get('game_summary_url'),
                'event_summary_url': game.get('event_summary_url'),
                'faceoff_summary_url': game.get('faceoff_summary_url'),
                'roster_url': game.get('roster_url'),
                'shot_summary_url': game.get('shot_summary_url'),
                'time_on_ice_url': game.get('time_on_ice_url'),
                'shift_chart_url': game.get('shift_chart_url'),
                'last_updated': datetime.now().isoformat()
            }
            metadata_data.append(metadata_row)
        
        # Save to CSV files
        if schedule_data:
            pd.DataFrame(schedule_data).to_csv(self.csv_paths['game_schedule'], index=False)
        
        if results_data:
            pd.DataFrame(results_data).to_csv(self.csv_paths['game_results'], index=False)
        
        if metadata_data:
            pd.DataFrame(metadata_data).to_csv(self.csv_paths['game_metadata'], index=False)
        
        self.logger.info(f"Saved {len(games_data)} games to CSV files")
    
    def save_players_data(self, players_data: List[Dict[str, Any]]) -> None:
        """Save players data to CSV files."""
        if not players_data:
            return
        
        # Separate players into different CSV files based on data type
        info_data = []
        stats_data = []
        game_stats_data = []
        names_data = []
        
        for player in players_data:
            # Player info data
            info_row = {
                'player_id': player.get('player_id'),
                'first_name': player.get('first_name'),
                'last_name': player.get('last_name'),
                'full_name': player.get('full_name'),
                'position_code': player.get('position_code'),
                'shoots_catches': player.get('shoots_catches'),
                'height_inches': player.get('height_inches'),
                'weight_pounds': player.get('weight_pounds'),
                'birth_date': player.get('birth_date'),
                'birth_city': player.get('birth_city'),
                'birth_country': player.get('birth_country'),
                'current_team_id': player.get('current_team_id'),
                'current_team_abbrev': player.get('current_team_abbrev'),
                'rookie_year': player.get('rookie_year'),
                'last_updated': datetime.now().isoformat()
            }
            info_data.append(info_row)
            
            # Player names data
            names_row = {
                'player_id': player.get('player_id'),
                'first_name': player.get('first_name'),
                'last_name': player.get('last_name'),
                'full_name': player.get('full_name'),
                'last_updated': datetime.now().isoformat()
            }
            names_data.append(names_row)
            
            # Season stats (if available)
            if player.get('season_stats'):
                for season_stat in player['season_stats']:
                    stats_row = {
                        'player_id': player.get('player_id'),
                        'season_id': season_stat.get('season_id'),
                        'team_id': season_stat.get('team_id'),
                        'team_abbrev': season_stat.get('team_abbrev'),
                        'games_played': season_stat.get('games_played'),
                        'goals': season_stat.get('goals'),
                        'assists': season_stat.get('assists'),
                        'points': season_stat.get('points'),
                        'plus_minus': season_stat.get('plus_minus'),
                        'penalty_minutes': season_stat.get('penalty_minutes'),
                        'shots': season_stat.get('shots'),
                        'shooting_pct': season_stat.get('shooting_pct'),
                        'time_on_ice_per_game': season_stat.get('time_on_ice_per_game'),
                        'last_updated': datetime.now().isoformat()
                    }
                    stats_data.append(stats_row)
            
            # Game stats (if available)
            if player.get('game_stats'):
                for game_stat in player['game_stats']:
                    game_stats_row = {
                        'game_id': game_stat.get('game_id'),
                        'player_id': player.get('player_id'),
                        'team_id': game_stat.get('team_id'),
                        'goals': game_stat.get('goals'),
                        'assists': game_stat.get('assists'),
                        'points': game_stat.get('points'),
                        'plus_minus': game_stat.get('plus_minus'),
                        'penalty_minutes': game_stat.get('penalty_minutes'),
                        'hits': game_stat.get('hits'),
                        'power_play_goals': game_stat.get('power_play_goals'),
                        'shots': game_stat.get('shots'),
                        'faceoff_pct': game_stat.get('faceoff_pct'),
                        'time_on_ice': game_stat.get('time_on_ice'),
                        'last_updated': datetime.now().isoformat()
                    }
                    game_stats_data.append(game_stats_row)
        
        # Save to CSV files
        if info_data:
            pd.DataFrame(info_data).to_csv(self.csv_paths['player_info'], index=False)
        
        if names_data:
            pd.DataFrame(names_data).to_csv(self.csv_paths['player_names'], index=False)
        
        if stats_data:
            pd.DataFrame(stats_data).to_csv(self.csv_paths['player_stats'], index=False)
        
        if game_stats_data:
            pd.DataFrame(game_stats_data).to_csv(self.csv_paths['player_game_stats'], index=False)
        
        self.logger.info(f"Saved {len(players_data)} players to CSV files")
    
    def save_events_data(self, events_data: List[Dict[str, Any]]) -> None:
        """Save events data to CSV files."""
        if not events_data:
            return
        
        # Separate events into different CSV files based on data type
        play_by_play_data = []
        shifts_data = []
        events_data_list = []
        
        for event in events_data:
            # Play-by-play data
            if event.get('type') == 'play':
                play_row = {
                    'game_id': event.get('game_id'),
                    'event_id': event.get('event_id'),
                    'period': event.get('period'),
                    'period_type': event.get('period_type'),
                    'time_in_period': event.get('time_in_period'),
                    'time_remaining': event.get('time_remaining'),
                    'type_code': event.get('type_code'),
                    'type_desc_key': event.get('type_desc_key'),
                    'situation_code': event.get('situation_code'),
                    'home_team_defending_side': event.get('home_team_defending_side'),
                    'details_json': json.dumps(event.get('details', {})),
                    'last_updated': datetime.now().isoformat()
                }
                play_by_play_data.append(play_row)
            
            # Shifts data
            elif event.get('type') == 'shift':
                shift_row = {
                    'game_id': event.get('game_id'),
                    'event_id': event.get('event_id'),
                    'period': event.get('period'),
                    'player_count': event.get('player_count'),
                    'elapsed_time': event.get('elapsed_time'),
                    'game_time': event.get('game_time'),
                    'event_type': event.get('event_type'),
                    'description': event.get('description'),
                    'away_players_json': json.dumps(event.get('away_players', [])),
                    'home_players_json': json.dumps(event.get('home_players', [])),
                    'last_updated': datetime.now().isoformat()
                }
                shifts_data.append(shift_row)
            
            # General events data
            else:
                event_row = {
                    'game_id': event.get('game_id'),
                    'event_id': event.get('event_id'),
                    'event_type': event.get('event_type'),
                    'event_data': json.dumps(event),
                    'last_updated': datetime.now().isoformat()
                }
                events_data_list.append(event_row)
        
        # Save to CSV files
        if play_by_play_data:
            pd.DataFrame(play_by_play_data).to_csv(self.csv_paths['play_by_play'], index=False)
        
        if shifts_data:
            pd.DataFrame(shifts_data).to_csv(self.csv_paths['shifts'], index=False)
        
        if events_data_list:
            pd.DataFrame(events_data_list).to_csv(self.csv_paths['events'], index=False)
        
        self.logger.info(f"Saved {len(events_data)} events to CSV files")
    
    def save_html_report(self, season: str, report_type: str, game_id: str, report_data: str) -> None:
        """Save HTML report data to file in the correct HTML reports structure."""
        # Create season-specific directory in HTML reports structure
        report_dir = Path(self.config.storage_root) / season / "html" / "reports" / report_type
        report_dir.mkdir(parents=True, exist_ok=True)
        
        # Save HTML report with .HTM extension
        report_file = report_dir / f"{report_type}{game_id}.HTM"
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(report_data)
        
        self.logger.debug(f"Saved HTML report: {report_file}")
    
    def save_curated_data(self, curated_data: Dict[str, Any], season: str) -> None:
        """Save curated data to CSV files."""
        if not curated_data:
            return
        
        # Save game curated data
        if 'games' in curated_data:
            games_df = pd.DataFrame(curated_data['games'])
            games_df['curation_timestamp'] = datetime.now().isoformat()
            games_df.to_csv(self.csv_paths['game_curated'], index=False)
        
        # Save player curated data
        if 'players' in curated_data:
            players_df = pd.DataFrame(curated_data['players'])
            players_df['curation_timestamp'] = datetime.now().isoformat()
            players_df.to_csv(self.csv_paths['player_curated'], index=False)
        
        # Save team curated data
        if 'teams' in curated_data:
            teams_df = pd.DataFrame(curated_data['teams'])
            teams_df['curation_timestamp'] = datetime.now().isoformat()
            teams_df.to_csv(self.csv_paths['team_curated'], index=False)
        
        self.logger.info(f"Saved curated data for season {season}")
    
    def get_season_status(self, season: str) -> Dict[str, Any]:
        """Get the status of data for a specific season."""
        status = {
            'season': season,
            'available_datasets': [],
            'last_updated': None,
            'completeness_percentage': 0.0
        }
        
        # Check which datasets are available
        expected_datasets = [
            'seasons', 'teams', 'game_schedule', 'game_results',
            'player_info', 'player_stats', 'play_by_play', 'shifts'
        ]
        
        available_count = 0
        for dataset in expected_datasets:
            file_path = self.csv_paths[dataset]
            if file_path.exists():
                status['available_datasets'].append(dataset)
                available_count += 1
                
                # Get last modified time
                mtime = datetime.fromtimestamp(file_path.stat().st_mtime)
                if status['last_updated'] is None or mtime > status['last_updated']:
                    status['last_updated'] = mtime
        
        # Calculate completeness percentage
        status['completeness_percentage'] = (available_count / len(expected_datasets)) * 100
        
        return status
    
    def generate_season_summary(self, season: str) -> Dict[str, Any]:
        """Generate a summary report for a specific season."""
        summary = {
            'season': season,
            'generated_at': datetime.now().isoformat(),
            'data_counts': {},
            'last_updated': {},
            'data_quality': {}
        }
        
        # Count records in each dataset
        for dataset_name, file_path in self.csv_paths.items():
            if file_path.exists():
                try:
                    df = pd.read_csv(file_path)
                    # Filter for the specific season if applicable
                    if 'season_id' in df.columns:
                        season_df = df[df['season_id'] == int(season)]
                        count = len(season_df)
                    else:
                        count = len(df)
                    
                    summary['data_counts'][dataset_name] = count
                    summary['last_updated'][dataset_name] = datetime.fromtimestamp(file_path.stat().st_mtime).isoformat()
                    
                except Exception as e:
                    self.logger.warning(f"Error reading {dataset_name}: {e}")
                    summary['data_counts'][dataset_name] = 0
        
        return summary
    
    def generate_system_summary(self, seasons: List[str]) -> Dict[str, Any]:
        """Generate a system-wide summary report."""
        summary = {
            'generated_at': datetime.now().isoformat(),
            'seasons_processed': len(seasons),
            'total_data_counts': {},
            'system_health': {},
            'storage_usage': {}
        }
        
        # Aggregate data counts across all seasons
        for dataset_name, file_path in self.csv_paths.items():
            if file_path.exists():
                try:
                    df = pd.read_csv(file_path)
                    summary['total_data_counts'][dataset_name] = len(df)
                except Exception as e:
                    self.logger.warning(f"Error reading {dataset_name}: {e}")
                    summary['total_data_counts'][dataset_name] = 0
        
        # Calculate storage usage
        total_size = 0
        for file_path in self.csv_paths.values():
            if file_path.exists():
                total_size += file_path.stat().st_size
        
        summary['storage_usage']['total_size_bytes'] = total_size
        summary['storage_usage']['total_size_mb'] = total_size / (1024 * 1024)
        
        return summary
    
    def save_summary_report(self, season: str, summary: Dict[str, Any], report_type: str) -> None:
        """Save a summary report to file."""
        reports_dir = self.base_storage_path / "reports"
        reports_dir.mkdir(exist_ok=True)
        
        report_file = reports_dir / f"summary_{season}_{report_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        with open(report_file, 'w') as f:
            json.dump(summary, f, indent=2)
        
        self.logger.info(f"Saved summary report: {report_file}")
    
    def save_system_summary(self, summary: Dict[str, Any], report_type: str) -> None:
        """Save a system summary report to file."""
        reports_dir = self.base_storage_path / "reports"
        reports_dir.mkdir(exist_ok=True)
        
        report_file = reports_dir / f"system_summary_{report_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        with open(report_file, 'w') as f:
            json.dump(summary, f, indent=2)
        
        self.logger.info(f"Saved system summary report: {report_file}")
    
    def remove_season_data(self, season: str) -> None:
        """Remove all data for a specific season."""
        self.logger.info(f"Removing data for season {season}")
        
        # Remove CSV files that contain season-specific data
        season_specific_files = [
            'game_schedule', 'game_results', 'player_stats', 'play_by_play', 'shifts'
        ]
        
        for dataset in season_specific_files:
            file_path = self.csv_paths[dataset]
            if file_path.exists():
                try:
                    # Read the file and filter out the season
                    df = pd.read_csv(file_path)
                    if 'season_id' in df.columns:
                        df_filtered = df[df['season_id'] != int(season)]
                        df_filtered.to_csv(file_path, index=False)
                        self.logger.info(f"Removed season {season} data from {dataset}")
                except Exception as e:
                    self.logger.error(f"Error removing season {season} from {dataset}: {e}")
        
        # Remove HTML reports for the season
        html_season_dir = self.html_storage_path / season
        if html_season_dir.exists():
            import shutil
            shutil.rmtree(html_season_dir)
            self.logger.info(f"Removed HTML reports for season {season}")
    
    def append_data(self, dataset_name: str, new_data: List[Dict[str, Any]]) -> None:
        """Append new data to an existing CSV file."""
        if not new_data:
            return
        
        file_path = self.csv_paths.get(dataset_name)
        if not file_path:
            self.logger.error(f"Unknown dataset: {dataset_name}")
            return
        
        # Convert to DataFrame
        new_df = pd.DataFrame(new_data)
        new_df['last_updated'] = datetime.now().isoformat()
        
        # Append to existing file or create new one
        if file_path.exists():
            existing_df = pd.read_csv(file_path)
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
            combined_df.to_csv(file_path, index=False)
            self.logger.info(f"Appended {len(new_data)} records to {dataset_name}")
        else:
            new_df.to_csv(file_path, index=False)
            self.logger.info(f"Created new file {dataset_name} with {len(new_data)} records")
    
    def get_data(self, dataset_name: str, filters: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """Get data from a specific dataset with optional filtering."""
        file_path = self.csv_paths.get(dataset_name)
        if not file_path or not file_path.exists():
            self.logger.warning(f"Dataset {dataset_name} not found")
            return pd.DataFrame()
        
        try:
            df = pd.read_csv(file_path)
            
            # Apply filters if provided
            if filters:
                for column, value in filters.items():
                    if column in df.columns:
                        df = df[df[column] == value]
            
            return df
        except Exception as e:
            self.logger.error(f"Error reading {dataset_name}: {e}")
            return pd.DataFrame()
