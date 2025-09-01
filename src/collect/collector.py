#!/usr/bin/env python3
"""
Enhanced Data Collector for NHL Data Retrieval System
====================================================

This module provides enhanced data collection capabilities that extend the
existing collect_01.py functionality with additional features and improvements.
"""

import requests
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
import time


class EnhancedDataCollector:
    """
    Enhanced data collector that extends the existing collection functionality.
    
    Provides additional methods for data collection, validation, and management
    that complement the existing collect_01.py module.
    """
    
    def __init__(self, config):
        """Initialize the enhanced data collector."""
        self.config = config
        self.logger = logging.getLogger('EnhancedCollector')
        
        # Base URL and headers
        self.base_url = config.base_url
        self.headers = config.headers_lines
    
    def get_all_seasons(self) -> List[Dict[str, Any]]:
        """
        Get all available NHL seasons.
        
        Returns:
            List of season dictionaries
        """
        try:
            url = f"{self.base_url}/v1/season"
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            
            seasons_data = response.json()
            self.logger.info(f"Retrieved {len(seasons_data)} seasons from API")
            return seasons_data
            
        except Exception as e:
            self.logger.error(f"Error retrieving seasons: {e}")
            return []
    
    def get_games_for_season(self, season: str) -> List[Dict[str, Any]]:
        """
        Get all games for a specific season.
        
        Args:
            season: Season identifier
            
        Returns:
            List of game dictionaries
        """
        try:
            # Get all teams first
            teams_url = f"{self.base_url}/v1/standings/now"
            teams_response = requests.get(teams_url, headers=self.headers)
            teams_response.raise_for_status()
            teams_data = teams_response.json()
            
            all_games = []
            unique_game_ids = set()
            
            # Get games for each team
            for team_info in teams_data['standings']:
                team_abbrev = team_info['teamAbbrev']['default']
                
                schedule_url = f"{self.base_url}/v1/club-schedule-season/{team_abbrev}/{season}"
                schedule_response = requests.get(schedule_url, headers=self.headers)
                schedule_response.raise_for_status()
                schedule_data = schedule_response.json()
                
                # Process games
                for game in schedule_data.get('games', []):
                    game_id = game.get('id')
                    if game_id and game_id not in unique_game_ids:
                        unique_game_ids.add(game_id)
                        all_games.append(game)
                
                # Add small delay to be respectful
                time.sleep(0.1)
            
            self.logger.info(f"Retrieved {len(all_games)} unique games for season {season}")
            return all_games
            
        except Exception as e:
            self.logger.error(f"Error retrieving games for season {season}: {e}")
            return []
    
    def get_team_standings(self) -> List[Dict[str, Any]]:
        """
        Get current team standings.
        
        Returns:
            List of team standing dictionaries
        """
        try:
            url = f"{self.base_url}/v1/standings/now"
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            
            standings_data = response.json()
            self.logger.info(f"Retrieved standings for {len(standings_data['standings'])} teams")
            return standings_data['standings']
            
        except Exception as e:
            self.logger.error(f"Error retrieving team standings: {e}")
            return []
    
    def get_player_info(self, player_id: int) -> Optional[Dict[str, Any]]:
        """
        Get detailed player information.
        
        Args:
            player_id: Player ID
            
        Returns:
            Player information dictionary or None if failed
        """
        try:
            url = f"{self.base_url}/v1/player/{player_id}/landing"
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            
            player_data = response.json()
            return player_data
            
        except Exception as e:
            self.logger.error(f"Error retrieving player {player_id}: {e}")
            return None
    
    def get_team_roster(self, team_abbrev: str) -> List[Dict[str, Any]]:
        """
        Get current roster for a team.
        
        Args:
            team_abbrev: Team abbreviation
            
        Returns:
            List of player dictionaries
        """
        try:
            url = f"{self.base_url}/v1/roster/{team_abbrev}/current"
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            
            roster_data = response.json()
            self.logger.info(f"Retrieved roster for {team_abbrev}: {len(roster_data.get('roster', []))} players")
            return roster_data.get('roster', [])
            
        except Exception as e:
            self.logger.error(f"Error retrieving roster for {team_abbrev}: {e}")
            return []
    
    def get_game_boxscore(self, game_id: int) -> Optional[Dict[str, Any]]:
        """
        Get boxscore data for a specific game.
        
        Args:
            game_id: Game ID
            
        Returns:
            Boxscore data dictionary or None if failed
        """
        try:
            # Get both boxscore versions
            boxscore_v1_url = f"{self.base_url}/v1/gamecenter/{game_id}/boxscore"
            boxscore_v2_url = f"{self.base_url}/v1/gamecenter/{game_id}/right-rail"
            
            v1_response = requests.get(boxscore_v1_url, headers=self.headers)
            v2_response = requests.get(boxscore_v2_url, headers=self.headers)
            
            v1_response.raise_for_status()
            v2_response.raise_for_status()
            
            boxscore_v1 = v1_response.json()
            boxscore_v2 = v2_response.json()
            
            # Combine the data
            combined_boxscore = {
                'v1_data': boxscore_v1,
                'v2_data': boxscore_v2,
                'game_id': game_id,
                'timestamp': datetime.now().isoformat()
            }
            
            return combined_boxscore
            
        except Exception as e:
            self.logger.error(f"Error retrieving boxscore for game {game_id}: {e}")
            return None
    
    def get_game_play_by_play(self, game_id: int) -> Optional[Dict[str, Any]]:
        """
        Get play-by-play data for a specific game.
        
        Args:
            game_id: Game ID
            
        Returns:
            Play-by-play data dictionary or None if failed
        """
        try:
            url = f"{self.base_url}/v1/gamecenter/{game_id}/play-by-play"
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            
            play_data = response.json()
            return play_data
            
        except Exception as e:
            self.logger.error(f"Error retrieving play-by-play for game {game_id}: {e}")
            return None
    
    def validate_data_integrity(self, data: Dict[str, Any], data_type: str) -> Dict[str, Any]:
        """
        Validate the integrity of collected data.
        
        Args:
            data: Data to validate
            data_type: Type of data being validated
            
        Returns:
            Validation results dictionary
        """
        validation_result = {
            'valid': True,
            'errors': [],
            'warnings': [],
            'data_type': data_type,
            'timestamp': datetime.now().isoformat()
        }
        
        try:
            if data_type == 'season':
                validation_result = self._validate_season_data(data, validation_result)
            elif data_type == 'game':
                validation_result = self._validate_game_data(data, validation_result)
            elif data_type == 'player':
                validation_result = self._validate_player_data(data, validation_result)
            elif data_type == 'team':
                validation_result = self._validate_team_data(data, validation_result)
            else:
                validation_result['warnings'].append(f"Unknown data type: {data_type}")
            
        except Exception as e:
            validation_result['valid'] = False
            validation_result['errors'].append(f"Validation error: {str(e)}")
        
        return validation_result
    
    def _validate_season_data(self, data: Dict[str, Any], result: Dict[str, Any]) -> Dict[str, Any]:
        """Validate season data."""
        required_fields = ['id', 'name', 'type']
        
        for field in required_fields:
            if field not in data:
                result['valid'] = False
                result['errors'].append(f"Missing required field: {field}")
        
        if 'id' in data and not isinstance(data['id'], int):
            result['warnings'].append("Season ID should be an integer")
        
        return result
    
    def _validate_game_data(self, data: Dict[str, Any], result: Dict[str, Any]) -> Dict[str, Any]:
        """Validate game data."""
        required_fields = ['id', 'gameDate', 'homeTeam', 'awayTeam']
        
        for field in required_fields:
            if field not in data:
                result['valid'] = False
                result['errors'].append(f"Missing required field: {field}")
        
        if 'id' in data and not isinstance(data['id'], int):
            result['warnings'].append("Game ID should be an integer")
        
        return result
    
    def _validate_player_data(self, data: Dict[str, Any], result: Dict[str, Any]) -> Dict[str, Any]:
        """Validate player data."""
        required_fields = ['playerId', 'firstName', 'lastName']
        
        for field in required_fields:
            if field not in data:
                result['valid'] = False
                result['errors'].append(f"Missing required field: {field}")
        
        if 'playerId' in data and not isinstance(data['playerId'], int):
            result['warnings'].append("Player ID should be an integer")
        
        return result
    
    def _validate_team_data(self, data: Dict[str, Any], result: Dict[str, Any]) -> Dict[str, Any]:
        """Validate team data."""
        required_fields = ['teamAbbrev', 'teamName']
        
        for field in required_fields:
            if field not in data:
                result['valid'] = False
                result['errors'].append(f"Missing required field: {field}")
        
        return result
    
    def check_api_status(self) -> Dict[str, Any]:
        """
        Check the status of NHL API endpoints.
        
        Returns:
            Dictionary containing API status information
        """
        status = {
            'timestamp': datetime.now().isoformat(),
            'endpoints': {},
            'overall_status': 'unknown'
        }
        
        endpoints_to_check = [
            ('seasons', f"{self.base_url}/v1/season"),
            ('standings', f"{self.base_url}/v1/standings/now"),
            ('schedule', f"{self.base_url}/v1/club-schedule-season/BOS/20242025"),  # Example
        ]
        
        working_endpoints = 0
        
        for endpoint_name, url in endpoints_to_check:
            try:
                response = requests.get(url, headers=self.headers, timeout=10)
                if response.status_code == 200:
                    status['endpoints'][endpoint_name] = 'working'
                    working_endpoints += 1
                else:
                    status['endpoints'][endpoint_name] = f'error_{response.status_code}'
            except Exception as e:
                status['endpoints'][endpoint_name] = f'error_{str(e)}'
        
        # Determine overall status
        if working_endpoints == len(endpoints_to_check):
            status['overall_status'] = 'healthy'
        elif working_endpoints > 0:
            status['overall_status'] = 'degraded'
        else:
            status['overall_status'] = 'down'
        
        return status
    
    def get_data_statistics(self, season: str) -> Dict[str, Any]:
        """
        Get statistics about available data for a season.
        
        Args:
            season: Season identifier
            
        Returns:
            Dictionary containing data statistics
        """
        stats = {
            'season': season,
            'timestamp': datetime.now().isoformat(),
            'total_games': 0,
            'completed_games': 0,
            'scheduled_games': 0,
            'total_teams': 0,
            'total_players': 0,
            'data_completeness': 0.0
        }
        
        try:
            # Get games for the season
            games = self.get_games_for_season(season)
            stats['total_games'] = len(games)
            
            # Count game states
            for game in games:
                game_state = game.get('gameState', '')
                if game_state == 'OFF':
                    stats['completed_games'] += 1
                elif game_state == 'FUT':
                    stats['scheduled_games'] += 1
            
            # Get team count
            standings = self.get_team_standings()
            stats['total_teams'] = len(standings)
            
            # Calculate data completeness
            if stats['total_games'] > 0:
                stats['data_completeness'] = (stats['completed_games'] / stats['total_games']) * 100
            
        except Exception as e:
            self.logger.error(f"Error calculating statistics for season {season}: {e}")
        
        return stats
