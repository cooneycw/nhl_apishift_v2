#!/usr/bin/env python3
"""
Standalone NHL Data Collector
=============================

This module provides a standalone NHL data collection system that doesn't
depend on external libraries beyond standard Python modules and requests.
"""

import json
import os
import time
import logging
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
import csv

from config.nhl_config import NHLConfig


class DataCollector:
    """
    NHL data collector.
    
    Collects data from NHL API endpoints and stores it in JSON format.
    """
    
    def __init__(self, config: NHLConfig):
        """Initialize the data collector."""
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        self.session = requests.Session()
        self.session.headers.update(config.headers)
        
        # Rate limiting - Optimized for 100% success rate
        self.request_delay = 0.5  # 500ms between requests (more aggressive)
        self.last_request_time = 0
        self.max_retries = 5  # Increased retries for better success rate
        self.retry_backoff = 2.0  # Faster backoff for quicker recovery
        
        # Progress tracking
        self.total_requests = 0
        self.successful_requests = 0
        self.failed_requests = 0
        self.failed_games = []  # Track which games failed to collect
        
    def _make_request(self, url: str, timeout: int = 30) -> Optional[Dict[str, Any]]:
        """
        Make a rate-limited, API-friendly request to the NHL API with retry logic.
        
        Args:
            url: URL to request
            timeout: Request timeout in seconds
            
        Returns:
            JSON response data or None if failed
        """
        for attempt in range(self.max_retries + 1):
            # Rate limiting - be respectful to NHL servers
            current_time = time.time()
            time_since_last = current_time - self.last_request_time
            if time_since_last < self.request_delay:
                sleep_time = self.request_delay - time_since_last
                time.sleep(sleep_time)
            
            try:
                self.total_requests += 1
                response = self.session.get(url, timeout=timeout)
                
                # Handle rate limiting responses
                if response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', self.retry_backoff * (2 ** attempt)))
                    self.logger.warning(f"Rate limited - waiting {retry_after}s")
                    time.sleep(retry_after)
                    continue
                
                # Handle server errors with retry
                if response.status_code >= 500:
                    if attempt < self.max_retries:
                        wait_time = self.retry_backoff * (1.5 ** attempt)  # More gradual backoff
                        self.logger.warning(f"Server error {response.status_code}, retrying in {wait_time:.1f}s (attempt {attempt + 1}/{self.max_retries})")
                        time.sleep(wait_time)
                        continue
                    else:
                        self.failed_requests += 1
                        self.logger.error(f"Server error {response.status_code} after {self.max_retries} retries")
                        return None
                
                # Raise for other 4xx errors (don't retry client errors)
                response.raise_for_status()
                
                self.last_request_time = time.time()
                self.successful_requests += 1
                return response.json()
                
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
                self.logger.error(f"JSON decode failed: {e}")
                return None
        
        return None
    
    def get_progress_stats(self) -> Dict[str, Any]:
        """Get current progress statistics."""
        return {
            'total_requests': self.total_requests,
            'successful_requests': self.successful_requests,
            'failed_requests': self.failed_requests,
            'request_success_rate': round((self.successful_requests / max(self.total_requests, 1)) * 100, 1),
            'failed_games': self.failed_games
        }
    
    def collect_seasons(self) -> List[Dict[str, Any]]:
        """
        Collect season information.
        
        Returns:
            List of season dictionaries
        """
        self.logger.info("Collecting season information...")
        
        url = self.config.get_endpoint("seasons")
        data = self._make_request(url)
        
        if data:
            seasons = data if isinstance(data, list) else data.get('seasons', [])
            self.logger.info(f"Collected {len(seasons)} seasons")
            
            # Save to file
            file_path = self.config.file_paths["seasons"]
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            with open(file_path, 'w') as f:
                json.dump(seasons, f, indent=2)
            
            return seasons
        else:
            self.logger.error("Failed to collect season information")
            return []
    
    def collect_teams(self) -> List[Dict[str, Any]]:
        """
        Collect team information.
        
        Returns:
            List of team dictionaries
        """
        self.logger.info("Collecting team information...")
        
        # Get teams from standings endpoint
        url = self.config.get_endpoint("standings")
        data = self._make_request(url)
        
        teams = []
        if data:
            # Extract teams from standings data
            standings = data.get('standings', [])
            for standing in standings:
                # Create team info from standings data
                team_info = {
                    'id': standing.get('teamAbbrev', {}).get('default'),
                    'abbrev': standing.get('teamAbbrev', {}).get('default'),
                    'name': standing.get('teamName', {}).get('default'),
                    'commonName': standing.get('teamCommonName', {}).get('default'),
                    'placeName': standing.get('placeName', {}).get('default'),
                    'logo': standing.get('teamLogo'),
                    'conference': standing.get('conferenceName'),
                    'division': standing.get('divisionName')
                }
                
                # Only add if we have essential info
                if team_info['abbrev'] and team_info['name']:
                    teams.append(team_info)
            
            self.logger.info(f"Collected {len(teams)} teams")
            
            # Save to file
            file_path = self.config.file_paths["teams"]
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            with open(file_path, 'w') as f:
                json.dump(teams, f, indent=2)
            
            return teams
        else:
            self.logger.error("Failed to collect team information")
            return []
    
    def collect_games_for_season(self, season: str, teams: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Collect games for a specific season.
        
        Args:
            season: Season identifier (e.g., '20242025')
            teams: List of team dictionaries
            
        Returns:
            List of game dictionaries
        """
        self.logger.info(f"Collecting games for season {season}...")
        
        all_games = []
        
        # Use ThreadPoolExecutor for parallel collection
        with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            # Submit tasks for each team
            future_to_team = {}
            for team in teams:
                team_abbrev = team.get('abbrev') or team.get('abbreviation') or team.get('triCode')
                if team_abbrev:
                    future = executor.submit(self._collect_team_schedule, team_abbrev, season)
                    future_to_team[future] = team_abbrev
            
            # Collect results
            for future in as_completed(future_to_team):
                team_abbrev = future_to_team[future]
                try:
                    team_games = future.result()
                    if team_games:
                        all_games.extend(team_games)
                        self.logger.debug(f"Collected {len(team_games)} games for team {team_abbrev}")
                except Exception as e:
                    self.logger.error(f"Error collecting games for team {team_abbrev}: {e}")
        
        # Remove duplicates (games appear in both teams' schedules)
        unique_games = {}
        for game in all_games:
            game_id = game.get('id')
            if game_id and game_id not in unique_games:
                unique_games[game_id] = game
        
        games_list = list(unique_games.values())
        
        # Filter for regular season games only (gameType == 2)
        regular_season_games = [game for game in games_list if game.get('gameType') == 2]
        filtered_count = len(games_list) - len(regular_season_games)
        
        self.logger.info(f"Collected {len(games_list)} total games for season {season}")
        self.logger.info(f"Filtered to {len(regular_season_games)} regular season games (excluded {filtered_count} preseason/playoff games)")
        
        # Save to file
        file_path = self.config.get_season_file_path(season, "games")
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, 'w') as f:
            json.dump(regular_season_games, f, indent=2)
        
        return regular_season_games
    
    def _collect_team_schedule(self, team_abbrev: str, season: str) -> List[Dict[str, Any]]:
        """
        Collect schedule for a specific team.
        
        Args:
            team_abbrev: Team abbreviation
            season: Season identifier
            
        Returns:
            List of game dictionaries
        """
        url = self.config.get_endpoint("schedule", team=team_abbrev, season=season)
        data = self._make_request(url)
        
        if data:
            games = data.get('games', [])
            return games
        else:
            return []
    
    def collect_boxscores_for_games(self, season: str, games: List[Dict[str, Any]]) -> int:
        """
        Collect boxscores for a list of games.
        
        Args:
            season: Season identifier
            games: List of game dictionaries
            
        Returns:
            Number of boxscores collected
        """
        self.logger.info(f"Collecting boxscores for {len(games)} games in season {season}...")
        self.logger.info(f"Starting collection with {self.config.max_workers} workers, 1s delay between requests...")
        
        collected_count = 0
        total_games = len(games)
        
        # Use ThreadPoolExecutor for parallel collection
        with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            # Submit tasks for each game
            future_to_game = {}
            for game in games:
                game_id = game.get('id')
                if game_id:
                    future = executor.submit(self._collect_game_boxscore, season, game_id)
                    future_to_game[future] = game_id
            
            # Collect results with progress reporting
            for i, future in enumerate(as_completed(future_to_game), 1):
                game_id = future_to_game[future]
                try:
                    success = future.result()
                    if success:
                        collected_count += 1
                    
                    # Show progress every 25 games or at the end
                    if i % 25 == 0 or i == total_games:
                        stats = self.get_progress_stats()
                        collection_rate = round((collected_count / i) * 100, 1)
                        self.logger.info(f"Progress: {i}/{total_games} games processed, {collected_count} collected "
                                        f"({collection_rate}% collection rate, {stats['request_success_rate']}% API success)")
                        # Force flush to ensure immediate output
                        import sys
                        sys.stdout.flush()
                        
                except Exception as e:
                    self.logger.error(f"Error collecting boxscore for game {game_id}: {e}")
                    self.failed_games.append(game_id)
        
        stats = self.get_progress_stats()
        collection_rate = round((collected_count / total_games) * 100, 1)
        self.logger.info(f"Completed: {collected_count}/{total_games} boxscores collected "
                        f"({collection_rate}% collection rate, {stats['request_success_rate']}% API success)")
        
        # Report failed games if any
        if self.failed_games:
            self.logger.warning(f"Failed to collect {len(self.failed_games)} games: {self.failed_games[:10]}{'...' if len(self.failed_games) > 10 else ''}")
        return collected_count
    
    def _collect_game_boxscore(self, season: str, game_id: int) -> bool:
        """
        Collect boxscore for a specific game.
        
        Args:
            season: Season identifier
            game_id: Game ID
            
        Returns:
            True if successful, False otherwise
        """
        url = self.config.get_endpoint("boxscore", game_id=game_id)
        data = self._make_request(url)
        
        if data:
            # Save to file
            file_path = self.config.get_season_file_path(season, "boxscores", str(game_id))
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            with open(file_path, 'w') as f:
                json.dump(data, f, indent=2)
            return True
        else:
            # Track failed games
            self.failed_games.append(game_id)
            return False
    
    def collect_playbyplay_for_games(self, season: str, games: List[Dict[str, Any]]) -> int:
        """
        Collect play-by-play data for a list of games.
        
        Args:
            season: Season identifier
            games: List of game dictionaries
            
        Returns:
            Number of play-by-play datasets collected
        """
        self.logger.info(f"Collecting play-by-play data for {len(games)} games in season {season}...")
        
        collected_count = 0
        
        # Use ThreadPoolExecutor for parallel collection
        with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            # Submit tasks for each game
            future_to_game = {}
            for game in games:
                game_id = game.get('id')
                if game_id:
                    future = executor.submit(self._collect_game_playbyplay, season, game_id)
                    future_to_game[future] = game_id
            
            # Collect results
            for future in as_completed(future_to_game):
                game_id = future_to_game[future]
                try:
                    success = future.result()
                    if success:
                        collected_count += 1
                except Exception as e:
                    self.logger.error(f"Error collecting play-by-play for game {game_id}: {e}")
        
        self.logger.info(f"Collected {collected_count} play-by-play datasets for season {season}")
        return collected_count
    
    def _collect_game_playbyplay(self, season: str, game_id: int) -> bool:
        """
        Collect play-by-play data for a specific game.
        
        Args:
            season: Season identifier
            game_id: Game ID
            
        Returns:
            True if successful, False otherwise
        """
        url = self.config.get_endpoint("plays", game_id=game_id)
        data = self._make_request(url)
        
        if data:
            # Save to file
            file_path = self.config.get_season_file_path(season, "playbyplay", str(game_id))
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            with open(file_path, 'w') as f:
                json.dump(data, f, indent=2)
            return True
        else:
            return False
    
    def collect_players_from_games(self, season: str, games: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """
        Collect unique player information from game data.
        
        Args:
            season: Season identifier
            games: List of game dictionaries
            
        Returns:
            Dictionary of player_id -> player_info
        """
        self.logger.info(f"Collecting player information from {len(games)} games in season {season}...")
        
        players = {}
        
        # First, extract player IDs from boxscore data
        for game in games:
            game_id = game.get('id')
            if game_id:
                try:
                    # Load boxscore data
                    boxscore_path = self.config.get_season_file_path(season, "boxscores", str(game_id))
                    if os.path.exists(boxscore_path):
                        with open(boxscore_path, 'r') as f:
                            boxscore = json.load(f)
                        
                        # Extract player IDs from boxscore
                        player_ids = self._extract_player_ids_from_boxscore(boxscore)
                        
                        # Collect detailed player info for each player
                        for player_id in player_ids:
                            if player_id not in players:
                                player_info = self._collect_player_info(player_id)
                                if player_info:
                                    players[player_id] = player_info
                                    
                except Exception as e:
                    self.logger.error(f"Error processing players from game {game_id}: {e}")
        
        self.logger.info(f"Collected information for {len(players)} unique players in season {season}")
        
        # Save to file
        file_path = self.config.get_season_file_path(season, "players")
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, 'w') as f:
            json.dump(players, f, indent=2)
        
        return players
    
    def _extract_player_ids_from_boxscore(self, boxscore: Dict[str, Any]) -> List[str]:
        """Extract player IDs from boxscore data."""
        player_ids = []
        
        # Look in different sections of boxscore data
        for team_type in ['homeTeam', 'awayTeam']:
            team_data = boxscore.get(team_type, {})
            
            # Players in different stats sections
            for section in ['forwards', 'defensemen', 'goalies']:
                players_section = team_data.get(section, [])
                for player in players_section:
                    player_id = player.get('playerId')
                    if player_id:
                        player_ids.append(str(player_id))
        
        return list(set(player_ids))  # Remove duplicates
    
    def _collect_player_info(self, player_id: str) -> Optional[Dict[str, Any]]:
        """
        Collect detailed information for a specific player.
        
        Args:
            player_id: Player ID
            
        Returns:
            Player information dictionary or None if failed
        """
        url = self.config.get_endpoint("player", player_id=player_id)
        return self._make_request(url)
    
    def export_to_csv(self, season: str) -> Dict[str, str]:
        """
        Export collected data to CSV format.
        
        Args:
            season: Season identifier
            
        Returns:
            Dictionary of data_type -> csv_file_path
        """
        self.logger.info(f"Exporting data to CSV for season {season}...")
        
        csv_files = {}
        csv_dir = os.path.join(self.config.file_paths["csv_exports"], season)
        os.makedirs(csv_dir, exist_ok=True)
        
        # Export games
        games_csv = os.path.join(csv_dir, "games.csv")
        if self._export_games_to_csv(season, games_csv):
            csv_files['games'] = games_csv
        
        # Export players
        players_csv = os.path.join(csv_dir, "players.csv")
        if self._export_players_to_csv(season, players_csv):
            csv_files['players'] = players_csv
        
        self.logger.info(f"Exported {len(csv_files)} CSV files for season {season}")
        return csv_files
    
    def _export_games_to_csv(self, season: str, output_path: str) -> bool:
        """Export games data to CSV."""
        try:
            games_path = self.config.get_season_file_path(season, "games")
            if os.path.exists(games_path):
                with open(games_path, 'r') as f:
                    games = json.load(f)
                
                if games:
                    with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
                        fieldnames = ['id', 'date', 'homeTeam', 'awayTeam', 'gameState', 'gameType']
                        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                        writer.writeheader()
                        
                        for game in games:
                            row = {
                                'id': game.get('id'),
                                'date': game.get('gameDate'),
                                'homeTeam': game.get('homeTeam', {}).get('abbrev'),
                                'awayTeam': game.get('awayTeam', {}).get('abbrev'),
                                'gameState': game.get('gameState'),
                                'gameType': game.get('gameType')
                            }
                            writer.writerow(row)
                    return True
        except Exception as e:
            self.logger.error(f"Error exporting games to CSV: {e}")
        return False
    
    def _export_players_to_csv(self, season: str, output_path: str) -> bool:
        """Export players data to CSV."""
        try:
            players_path = self.config.get_season_file_path(season, "players")
            if os.path.exists(players_path):
                with open(players_path, 'r') as f:
                    players = json.load(f)
                
                if players:
                    with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
                        fieldnames = ['id', 'firstName', 'lastName', 'position', 'shoots', 'height', 'weight', 'birthDate', 'birthCity', 'birthCountry']
                        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                        writer.writeheader()
                        
                        for player_id, player_info in players.items():
                            row = {
                                'id': player_id,
                                'firstName': player_info.get('firstName'),
                                'lastName': player_info.get('lastName'),
                                'position': player_info.get('position'),
                                'shoots': player_info.get('shootsCatches'),
                                'height': player_info.get('heightInInches'),
                                'weight': player_info.get('weightInPounds'),
                                'birthDate': player_info.get('birthDate'),
                                'birthCity': player_info.get('birthCity'),
                                'birthCountry': player_info.get('birthCountry')
                            }
                            writer.writerow(row)
                    return True
        except Exception as e:
            self.logger.error(f"Error exporting players to CSV: {e}")
        return False
