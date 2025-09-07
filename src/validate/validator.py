#!/usr/bin/env python3
"""
Data Validator for NHL Data Retrieval System
===========================================

This module provides comprehensive data validation and integrity checking
for all NHL datasets to ensure data quality and consistency.
"""

import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
import pandas as pd
import json


class DataValidator:
    """
    Comprehensive data validator for NHL datasets.
    
    Provides validation for data integrity, consistency, and quality across
    all NHL datasets including seasons, teams, games, players, and events.
    """
    
    def __init__(self, config):
        """Initialize the data validator."""
        self.config = config
        self.logger = logging.getLogger('DataValidator')
    
    def validate_season_data(self, season: str) -> Dict[str, Any]:
        """
        Validate data for a specific season.
        
        Args:
            season: Season identifier
            
        Returns:
            Dictionary containing validation results
        """
        validation_result = {
            'season': season,
            'timestamp': datetime.now().isoformat(),
            'overall_valid': True,
            'errors': [],
            'warnings': [],
            'data_quality_score': 0.0,
            'validation_details': {}
        }
        
        try:
            # Validate different data types
            validation_result['validation_details']['seasons'] = self._validate_seasons_dataset(season)
            validation_result['validation_details']['teams'] = self._validate_teams_dataset(season)
            validation_result['validation_details']['games'] = self._validate_games_dataset(season)
            validation_result['validation_details']['players'] = self._validate_players_dataset(season)
            validation_result['validation_details']['events'] = self._validate_events_dataset(season)
            
            # Calculate overall data quality score
            scores = []
            for dataset, result in validation_result['validation_details'].items():
                if 'quality_score' in result:
                    scores.append(result['quality_score'])
            
            if scores:
                validation_result['data_quality_score'] = sum(scores) / len(scores)
            
            # Determine overall validity
            for dataset, result in validation_result['validation_details'].items():
                if not result.get('valid', True):
                    validation_result['overall_valid'] = False
                    validation_result['errors'].extend(result.get('errors', []))
                validation_result['warnings'].extend(result.get('warnings', []))
            
        except Exception as e:
            validation_result['overall_valid'] = False
            validation_result['errors'].append(f"Validation error: {str(e)}")
            self.logger.error(f"Error validating season {season}: {e}")
        
        return validation_result
    
    def _validate_seasons_dataset(self, season: str) -> Dict[str, Any]:
        """Validate seasons dataset."""
        result = {
            'valid': True,
            'errors': [],
            'warnings': [],
            'quality_score': 0.0
        }
        
        try:
            # Load seasons data
            seasons_df = self._load_dataset('seasons')
            if seasons_df.empty:
                result['valid'] = False
                result['errors'].append("No seasons data found")
                return result
            
            # Check if the season exists
            if 'season_id' in seasons_df.columns:
                season_exists = int(season) in seasons_df['season_id'].values
                if not season_exists:
                    result['warnings'].append(f"Season {season} not found in seasons dataset")
            
            # Validate data types
            if 'id' in seasons_df.columns:
                if not seasons_df['id'].dtype in ['int64', 'int32']:
                    result['warnings'].append("Season ID should be integer type")
            
            # Check for missing values
            missing_counts = seasons_df.isnull().sum()
            if missing_counts.any():
                result['warnings'].append(f"Missing values found: {missing_counts.to_dict()}")
            
            # Calculate quality score
            total_cells = len(seasons_df) * len(seasons_df.columns)
            missing_cells = seasons_df.isnull().sum().sum()
            result['quality_score'] = (total_cells - missing_cells) / total_cells if total_cells > 0 else 0.0
            
        except Exception as e:
            result['valid'] = False
            result['errors'].append(f"Error validating seasons dataset: {str(e)}")
        
        return result
    
    def _validate_teams_dataset(self, season: str) -> Dict[str, Any]:
        """Validate teams dataset."""
        result = {
            'valid': True,
            'errors': [],
            'warnings': [],
            'quality_score': 0.0
        }
        
        try:
            # Load teams data
            teams_df = self._load_dataset('teams')
            if teams_df.empty:
                result['valid'] = False
                result['errors'].append("No teams data found")
                return result
            
            # Check for required fields
            required_fields = ['team_id', 'full_name', 'raw_tricode']
            for field in required_fields:
                if field not in teams_df.columns:
                    result['errors'].append(f"Missing required field: {field}")
            
            # Validate team abbreviations
            if 'raw_tricode' in teams_df.columns:
                invalid_abbrevs = teams_df[teams_df['raw_tricode'].str.len() != 3]
                if not invalid_abbrevs.empty:
                    result['warnings'].append(f"Invalid team abbreviations found: {invalid_abbrevs['raw_tricode'].tolist()}")
            
            # Check for duplicate team IDs
            if 'team_id' in teams_df.columns:
                duplicates = teams_df[teams_df.duplicated(subset=['team_id'], keep=False)]
                if not duplicates.empty:
                    result['errors'].append(f"Duplicate team IDs found: {duplicates['team_id'].tolist()}")
            
            # Calculate quality score
            total_cells = len(teams_df) * len(teams_df.columns)
            missing_cells = teams_df.isnull().sum().sum()
            result['quality_score'] = (total_cells - missing_cells) / total_cells if total_cells > 0 else 0.0
            
        except Exception as e:
            result['valid'] = False
            result['errors'].append(f"Error validating teams dataset: {str(e)}")
        
        return result
    
    def _validate_games_dataset(self, season: str) -> Dict[str, Any]:
        """Validate games dataset."""
        result = {
            'valid': True,
            'errors': [],
            'warnings': [],
            'quality_score': 0.0
        }
        
        try:
            # Load games data
            games_df = self._load_dataset('game_schedule')
            if games_df.empty:
                result['valid'] = False
                result['errors'].append("No games data found")
                return result
            
            # Filter for the specific season
            if 'season_id' in games_df.columns:
                season_games = games_df[games_df['season_id'] == int(season)]
            else:
                season_games = games_df
                result['warnings'].append("No season_id column found, validating all games")
            
            if season_games.empty:
                result['warnings'].append(f"No games found for season {season}")
                return result
            
            # Check for required fields
            required_fields = ['game_id', 'game_date', 'home_team_abbrev', 'away_team_abbrev']
            for field in required_fields:
                if field not in season_games.columns:
                    result['errors'].append(f"Missing required field: {field}")
            
            # Validate game IDs
            if 'game_id' in season_games.columns:
                # Check for duplicate game IDs
                duplicates = season_games[season_games.duplicated(subset=['game_id'], keep=False)]
                if not duplicates.empty:
                    result['errors'].append(f"Duplicate game IDs found: {duplicates['game_id'].tolist()}")
                
                # Check for valid game ID format (should be numeric)
                if not pd.to_numeric(season_games['game_id'], errors='coerce').notna().all():
                    result['warnings'].append("Some game IDs are not numeric")
            
            # Validate game dates
            if 'game_date' in season_games.columns:
                try:
                    pd.to_datetime(season_games['game_date'])
                except:
                    result['warnings'].append("Some game dates are not in valid format")
            
            # Validate team abbreviations
            for team_field in ['home_team_abbrev', 'away_team_abbrev']:
                if team_field in season_games.columns:
                    invalid_abbrevs = season_games[season_games[team_field].str.len() != 3]
                    if not invalid_abbrevs.empty:
                        result['warnings'].append(f"Invalid {team_field} found: {invalid_abbrevs[team_field].tolist()}")
            
            # Calculate quality score
            total_cells = len(season_games) * len(season_games.columns)
            missing_cells = season_games.isnull().sum().sum()
            result['quality_score'] = (total_cells - missing_cells) / total_cells if total_cells > 0 else 0.0
            
        except Exception as e:
            result['valid'] = False
            result['errors'].append(f"Error validating games dataset: {str(e)}")
        
        return result
    
    def _validate_players_dataset(self, season: str) -> Dict[str, Any]:
        """Validate players dataset."""
        result = {
            'valid': True,
            'errors': [],
            'warnings': [],
            'quality_score': 0.0
        }
        
        try:
            # Load players data
            players_df = self._load_dataset('player_info')
            if players_df.empty:
                result['valid'] = False
                result['errors'].append("No players data found")
                return result
            
            # Check for required fields
            required_fields = ['player_id', 'first_name', 'last_name']
            for field in required_fields:
                if field not in players_df.columns:
                    result['errors'].append(f"Missing required field: {field}")
            
            # Validate player IDs
            if 'player_id' in players_df.columns:
                # Check for duplicate player IDs
                duplicates = players_df[players_df.duplicated(subset=['player_id'], keep=False)]
                if not duplicates.empty:
                    result['errors'].append(f"Duplicate player IDs found: {duplicates['player_id'].tolist()}")
                
                # Check for valid player ID format (should be numeric)
                if not pd.to_numeric(players_df['player_id'], errors='coerce').notna().all():
                    result['warnings'].append("Some player IDs are not numeric")
            
            # Validate names
            for name_field in ['first_name', 'last_name']:
                if name_field in players_df.columns:
                    # Check for empty names
                    empty_names = players_df[players_df[name_field].isna() | (players_df[name_field] == '')]
                    if not empty_names.empty:
                        result['warnings'].append(f"Empty {name_field} found for {len(empty_names)} players")
            
            # Validate position codes
            if 'position_code' in players_df.columns:
                valid_positions = ['C', 'L', 'R', 'D', 'G']
                invalid_positions = players_df[~players_df['position_code'].isin(valid_positions)]
                if not invalid_positions.empty:
                    result['warnings'].append(f"Invalid position codes found: {invalid_positions['position_code'].unique().tolist()}")
            
            # Calculate quality score
            total_cells = len(players_df) * len(players_df.columns)
            missing_cells = players_df.isnull().sum().sum()
            result['quality_score'] = (total_cells - missing_cells) / total_cells if total_cells > 0 else 0.0
            
        except Exception as e:
            result['valid'] = False
            result['errors'].append(f"Error validating players dataset: {str(e)}")
        
        return result
    
    def _validate_events_dataset(self, season: str) -> Dict[str, Any]:
        """Validate events dataset."""
        result = {
            'valid': True,
            'errors': [],
            'warnings': [],
            'quality_score': 0.0
        }
        
        try:
            # Load events data
            events_df = self._load_dataset('play_by_play')
            if events_df.empty:
                result['warnings'].append("No events data found")
                return result
            
            # Filter for the specific season if possible
            if 'game_id' in events_df.columns:
                # Try to get games for this season to filter events
                games_df = self._load_dataset('game_schedule')
                if not games_df.empty and 'season_id' in games_df.columns:
                    season_games = games_df[games_df['season_id'] == int(season)]['game_id'].tolist()
                    season_events = events_df[events_df['game_id'].isin(season_games)]
                else:
                    season_events = events_df
            else:
                season_events = events_df
                result['warnings'].append("No game_id column found, validating all events")
            
            if season_events.empty:
                result['warnings'].append(f"No events found for season {season}")
                return result
            
            # Check for required fields
            required_fields = ['game_id', 'event_id', 'period', 'time_in_period']
            for field in required_fields:
                if field not in season_events.columns:
                    result['errors'].append(f"Missing required field: {field}")
            
            # Validate event IDs
            if 'event_id' in season_events.columns:
                # Check for duplicate event IDs within the same game
                duplicates = season_events[season_events.duplicated(subset=['game_id', 'event_id'], keep=False)]
                if not duplicates.empty:
                    result['errors'].append(f"Duplicate event IDs found within games")
            
            # Validate periods
            if 'period' in season_events.columns:
                valid_periods = [1, 2, 3, 4, 5]  # Regular periods + overtime
                invalid_periods = season_events[~season_events['period'].isin(valid_periods)]
                if not invalid_periods.empty:
                    result['warnings'].append(f"Invalid period numbers found: {invalid_periods['period'].unique().tolist()}")
            
            # Validate time format
            if 'time_in_period' in season_events.columns:
                # Check if time format is MM:SS
                time_pattern = r'^\d{1,2}:\d{2}$'
                invalid_times = season_events[~season_events['time_in_period'].str.match(time_pattern, na=False)]
                if not invalid_times.empty:
                    result['warnings'].append(f"Invalid time format found: {invalid_times['time_in_period'].unique().tolist()}")
            
            # Calculate quality score
            total_cells = len(season_events) * len(season_events.columns)
            missing_cells = season_events.isnull().sum().sum()
            result['quality_score'] = (total_cells - missing_cells) / total_cells if total_cells > 0 else 0.0
            
        except Exception as e:
            result['valid'] = False
            result['errors'].append(f"Error validating events dataset: {str(e)}")
        
        return result
    
    def _load_dataset(self, dataset_name: str) -> pd.DataFrame:
        """Load a dataset from CSV file."""
        try:
            # This would need to be implemented based on your storage structure
            # For now, return empty DataFrame
            return pd.DataFrame()
        except Exception as e:
            self.logger.error(f"Error loading dataset {dataset_name}: {e}")
            return pd.DataFrame()
    
    def validate_data_consistency(self, season: str) -> Dict[str, Any]:
        """
        Validate consistency across different datasets.
        
        Args:
            season: Season identifier
            
        Returns:
            Dictionary containing consistency validation results
        """
        consistency_result = {
            'season': season,
            'timestamp': datetime.now().isoformat(),
            'consistent': True,
            'inconsistencies': [],
            'warnings': []
        }
        
        try:
            # Load datasets
            games_df = self._load_dataset('game_schedule')
            players_df = self._load_dataset('player_info')
            events_df = self._load_dataset('play_by_play')
            
            if not games_df.empty and not players_df.empty:
                # Check if all teams in games exist in teams dataset
                teams_df = self._load_dataset('teams')
                if not teams_df.empty:
                    game_teams = set()
                    if 'home_team_abbrev' in games_df.columns:
                        game_teams.update(games_df['home_team_abbrev'].unique())
                    if 'away_team_abbrev' in games_df.columns:
                        game_teams.update(games_df['away_team_abbrev'].unique())
                    
                    available_teams = set(teams_df['raw_tricode'].unique())
                    missing_teams = game_teams - available_teams
                    
                    if missing_teams:
                        consistency_result['inconsistencies'].append(f"Teams in games but not in teams dataset: {missing_teams}")
                        consistency_result['consistent'] = False
            
            if not games_df.empty and not events_df.empty:
                # Check if all games in events exist in games dataset
                if 'game_id' in events_df.columns and 'game_id' in games_df.columns:
                    event_games = set(events_df['game_id'].unique())
                    available_games = set(games_df['game_id'].unique())
                    missing_games = event_games - available_games
                    
                    if missing_games:
                        consistency_result['inconsistencies'].append(f"Games in events but not in games dataset: {len(missing_games)} games")
                        consistency_result['consistent'] = False
            
        except Exception as e:
            consistency_result['consistent'] = False
            consistency_result['inconsistencies'].append(f"Consistency validation error: {str(e)}")
            self.logger.error(f"Error validating data consistency for season {season}: {e}")
        
        return consistency_result
    
    def generate_validation_report(self, season: str) -> Dict[str, Any]:
        """
        Generate a comprehensive validation report for a season.
        
        Args:
            season: Season identifier
            
        Returns:
            Dictionary containing comprehensive validation report
        """
        report = {
            'season': season,
            'timestamp': datetime.now().isoformat(),
            'summary': {},
            'detailed_results': {},
            'recommendations': []
        }
        
        try:
            # Run all validations
            season_validation = self.validate_season_data(season)
            consistency_validation = self.validate_data_consistency(season)
            
            # Compile summary
            report['summary'] = {
                'overall_valid': season_validation['overall_valid'] and consistency_validation['consistent'],
                'data_quality_score': season_validation['data_quality_score'],
                'total_errors': len(season_validation['errors']) + len(consistency_validation['inconsistencies']),
                'total_warnings': len(season_validation['warnings']) + len(consistency_validation['warnings'])
            }
            
            # Detailed results
            report['detailed_results'] = {
                'season_validation': season_validation,
                'consistency_validation': consistency_validation
            }
            
            # Generate recommendations
            if not season_validation['overall_valid']:
                report['recommendations'].append("Data validation failed - review and fix errors before proceeding")
            
            if season_validation['data_quality_score'] < 0.8:
                report['recommendations'].append("Data quality score is low - consider data cleanup")
            
            if not consistency_validation['consistent']:
                report['recommendations'].append("Data consistency issues found - verify cross-dataset relationships")
            
            if len(season_validation['warnings']) > 10:
                report['recommendations'].append("Many warnings found - review data quality")
            
        except Exception as e:
            report['summary'] = {
                'overall_valid': False,
                'error': str(e)
            }
            self.logger.error(f"Error generating validation report for season {season}: {e}")
        
        return report
