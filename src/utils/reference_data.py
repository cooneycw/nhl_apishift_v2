#!/usr/bin/env python3
"""
Reference data loader for NHL HTML parsing.
Loads teams, players, games, and boxscore data to provide lookup capabilities.
"""

import json
import logging
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class TeamInfo:
    """Team information from reference data."""
    id: int
    name: str
    abbrev: str
    common_name: str
    place_name: str

@dataclass
class PlayerInfo:
    """Player information from reference data."""
    player_id: int
    sweater_number: int
    name: str
    position: str
    team_id: int

class ReferenceDataLoader:
    """
    Loads and provides access to NHL reference data from JSON files.
    """
    
    def __init__(self, storage_path: str = "storage/20242025/json"):
        """
        Initialize the reference data loader.
        
        Args:
            storage_path: Path to the JSON storage directory
        """
        self.storage_path = Path(storage_path)
        self.teams: Dict[int, TeamInfo] = {}
        self.players: Dict[int, PlayerInfo] = {}  # player_id -> PlayerInfo
        self.sweater_lookup: Dict[Tuple[int, int], PlayerInfo] = {}  # (team_id, sweater_number) -> PlayerInfo
        self.games: Dict[int, Dict] = {}  # game_id -> game_data
        self.boxscores: Dict[int, Dict] = {}  # game_id -> boxscore_data
        
        self._load_reference_data()
    
    def _load_reference_data(self):
        """Load all reference data from JSON files."""
        try:
            self._load_teams()
            self._load_games()
            self._load_boxscores()
            logger.info(f"Loaded reference data: {len(self.teams)} teams, {len(self.players)} players, {len(self.games)} games")
        except Exception as e:
            logger.error(f"Error loading reference data: {e}")
            raise
    
    def _load_teams(self):
        """Load team data from teams.json."""
        teams_file = self.storage_path / "teams.json"
        if not teams_file.exists():
            logger.warning(f"Teams file not found: {teams_file}")
            return
        
        try:
            with open(teams_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Extract team info from standings data
            if 'standings' in data:
                for team_data in data['standings']:
                    team_id = team_data.get('teamId')
                    if team_id:
                        team_info = TeamInfo(
                            id=team_id,
                            name=team_data.get('teamName', {}).get('default', ''),
                            abbrev=team_data.get('teamAbbrev', {}).get('default', ''),
                            common_name=team_data.get('teamCommonName', {}).get('default', ''),
                            place_name=team_data.get('teamPlaceName', {}).get('default', '')
                        )
                        self.teams[team_id] = team_info
                        
        except Exception as e:
            logger.error(f"Error loading teams: {e}")
    
    def _load_games(self):
        """Load game data from games.json."""
        games_file = self.storage_path / "games.json"
        if not games_file.exists():
            logger.warning(f"Games file not found: {games_file}")
            return
        
        try:
            with open(games_file, 'r', encoding='utf-8') as f:
                games_data = json.load(f)
            
            for game in games_data:
                game_id = game.get('id')
                if game_id:
                    self.games[game_id] = game
                    
        except Exception as e:
            logger.error(f"Error loading games: {e}")
    
    def _load_boxscores(self):
        """Load boxscore data from boxscores directory."""
        boxscores_dir = self.storage_path / "boxscores"
        if not boxscores_dir.exists():
            logger.warning(f"Boxscores directory not found: {boxscores_dir}")
            return
        
        try:
            for boxscore_file in boxscores_dir.glob("*.json"):
                with open(boxscore_file, 'r', encoding='utf-8') as f:
                    boxscore_data = json.load(f)
                
                game_id = boxscore_data.get('id')
                if game_id:
                    self.boxscores[game_id] = boxscore_data
                    self._extract_players_from_boxscore(boxscore_data)
                    
        except Exception as e:
            logger.error(f"Error loading boxscores: {e}")
    
    def _extract_players_from_boxscore(self, boxscore_data: Dict):
        """Extract player information from boxscore data."""
        game_id = boxscore_data.get('id')
        if not game_id:
            return
        
        # Extract players from both teams
        for team_type in ['awayTeam', 'homeTeam']:
            team_data = boxscore_data.get('playerByGameStats', {}).get(team_type, {})
            team_id = boxscore_data.get(team_type, {}).get('id')
            
            if not team_id:
                continue
            
            # Process forwards, defensemen, and goalies
            for position_group in ['forwards', 'defensemen', 'goalies']:
                players = team_data.get(position_group, [])
                for player_data in players:
                    player_id = player_data.get('playerId')
                    sweater_number = player_data.get('sweaterNumber')
                    name = player_data.get('name', {}).get('default', '')
                    position = player_data.get('position', '')
                    
                    if player_id and sweater_number is not None:
                        player_info = PlayerInfo(
                            player_id=player_id,
                            sweater_number=sweater_number,
                            name=name,
                            position=position,
                            team_id=team_id
                        )
                        
                        # Store by player_id
                        self.players[player_id] = player_info
                        
                        # Store by team_id + sweater_number for quick lookup
                        self.sweater_lookup[(team_id, sweater_number)] = player_info
    
    def get_team_by_id(self, team_id: int) -> Optional[TeamInfo]:
        """Get team information by team ID."""
        return self.teams.get(team_id)
    
    def get_team_by_abbrev(self, abbrev: str) -> Optional[TeamInfo]:
        """Get team information by team abbreviation."""
        for team in self.teams.values():
            if team.abbrev.upper() == abbrev.upper():
                return team
        return None
    
    def get_player_by_id(self, player_id: int) -> Optional[PlayerInfo]:
        """Get player information by player ID."""
        return self.players.get(player_id)
    
    def get_player_by_sweater(self, team_id: int, sweater_number: int) -> Optional[PlayerInfo]:
        """Get player information by team ID and sweater number."""
        return self.sweater_lookup.get((team_id, sweater_number))
    
    def get_game_by_id(self, game_id: int) -> Optional[Dict]:
        """Get game information by game ID."""
        return self.games.get(game_id)
    
    def get_boxscore_by_id(self, game_id: int) -> Optional[Dict]:
        """Get boxscore information by game ID."""
        return self.boxscores.get(game_id)
    
    def get_team_roster(self, team_id: int, game_id: int) -> List[PlayerInfo]:
        """Get team roster for a specific game."""
        boxscore = self.get_boxscore_by_id(game_id)
        if not boxscore:
            return []
        
        roster = []
        team_data = None
        
        # Find the team data in the boxscore
        if boxscore.get('awayTeam', {}).get('id') == team_id:
            team_data = boxscore.get('playerByGameStats', {}).get('awayTeam', {})
        elif boxscore.get('homeTeam', {}).get('id') == team_id:
            team_data = boxscore.get('playerByGameStats', {}).get('homeTeam', {})
        
        if not team_data:
            return []
        
        # Extract all players from the team
        for position_group in ['forwards', 'defensemen', 'goalies']:
            players = team_data.get(position_group, [])
            for player_data in players:
                player_id = player_data.get('playerId')
                if player_id and player_id in self.players:
                    roster.append(self.players[player_id])
        
        return roster
    
    def resolve_player_name(self, team_id: int, sweater_number: int, fallback_name: str = "") -> str:
        """
        Resolve player name using sweater number lookup.
        
        Args:
            team_id: Team ID
            sweater_number: Player sweater number
            fallback_name: Fallback name if lookup fails
            
        Returns:
            Resolved player name or fallback name
        """
        player_info = self.get_player_by_sweater(team_id, sweater_number)
        if player_info:
            return player_info.name
        return fallback_name
    
    def resolve_team_name(self, team_id: int, fallback_name: str = "") -> str:
        """
        Resolve team name using team ID lookup.
        
        Args:
            team_id: Team ID
            fallback_name: Fallback name if lookup fails
            
        Returns:
            Resolved team name or fallback name
        """
        team_info = self.get_team_by_id(team_id)
        if team_info:
            return team_info.name
        return fallback_name
