#!/usr/bin/env python3
"""
Player and Team-Level Goal Data Reconciliation System

This system performs detailed reconciliation of goal data at the player and team level,
comparing HTML reports against the authoritative Play-by-Play JSON source.

Key Features:
- Player-level goal reconciliation (scorer, assists)
- Team-level goal reconciliation (total goals, goal distribution)
- Detailed discrepancy analysis with specific player/team information
- Comprehensive reporting for data quality assurance

Usage:
    python player_team_goal_reconciliation.py --game-id 2024020001
    python player_team_goal_reconciliation.py --all-games
    python player_team_goal_reconciliation.py --team-analysis
"""

import json
import sys
import argparse
import re
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime
import logging
from collections import defaultdict

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('player_team_goal_reconciliation.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class PlayerGoalStats:
    """Player-level goal statistics."""
    player_id: int
    player_name: str
    sweater_number: int
    team: str
    goals: int
    assists: int
    points: int
    source: str

@dataclass
class TeamGoalStats:
    """Team-level goal statistics."""
    team_id: int
    team_abbrev: str
    total_goals: int
    goal_scorers: List[PlayerGoalStats]
    assist_players: List[PlayerGoalStats]
    source: str

@dataclass
class PlayerReconciliationResult:
    """Result of player-level goal reconciliation."""
    player_id: int
    player_name: str
    sweater_number: int
    team: str
    authoritative_goals: int
    authoritative_assists: int
    html_goals: int
    html_assists: int
    goal_discrepancy: int
    assist_discrepancy: int
    reconciliation_status: str  # 'perfect', 'minor_discrepancy', 'major_discrepancy'

@dataclass
class TeamReconciliationResult:
    """Result of team-level goal reconciliation."""
    team_id: int
    team_abbrev: str
    authoritative_goals: int
    html_gs_goals: int
    html_es_goals: int
    gs_discrepancy: int
    es_discrepancy: int
    gs_reconciliation_status: str
    es_reconciliation_status: str
    # New PL fields
    html_pl_goals: int = 0
    pl_discrepancy: int = 0
    pl_reconciliation_status: str = 'perfect'
    # Keep backward compatibility fields
    authoritative_total_goals: int = 0
    html_total_goals: int = 0
    goal_count_discrepancy: int = 0
    player_discrepancies: List[PlayerReconciliationResult] = None
    reconciliation_status: str = 'perfect'

@dataclass
class GameReconciliationResult:
    """Complete game reconciliation result."""
    game_id: str
    game_date: str
    home_team: str
    away_team: str
    total_goals: int
    team_results: Dict[str, TeamReconciliationResult]
    player_results: List[PlayerReconciliationResult]
    overall_reconciliation_percentage: float
    critical_discrepancies: List[Dict[str, Any]]
    warnings: List[Dict[str, Any]]

class PlayerTeamGoalReconciliation:
    """Comprehensive player and team-level goal reconciliation system."""
    
    def __init__(self, storage_path: str = 'storage/20242025'):
        self.storage_path = Path(storage_path)
        self.GOAL_EVENT_TYPE = 505
        
        # Team ID mappings
        self.team_id_mappings = {
            1: "NJD", 7: "BUF", 2: "NYI", 3: "NYR", 4: "PHI", 5: "PIT", 6: "BOS",
            8: "MTL", 9: "OTT", 10: "TOR", 12: "CAR", 13: "FLA", 14: "TBL", 15: "WSH",
            16: "CHI", 17: "DET", 18: "NSH", 19: "STL", 20: "CGY", 21: "COL", 22: "EDM",
            23: "VAN", 24: "ANA", 25: "DAL", 26: "LAK", 28: "SJS", 29: "CBJ", 30: "MIN",
            52: "WPG", 53: "ARI", 54: "VGK", 55: "SEA"
        }
        
        # Reverse mapping for team abbreviations to IDs
        self.team_abbrev_to_id = {v: k for k, v in self.team_id_mappings.items()}
    
    def reconcile_all_games(self, verbose: bool = False, output_file: str = None) -> Dict[str, Any]:
        """Reconcile player and team goal data for all games."""
        logger.info("Starting comprehensive player and team goal reconciliation...")
        self.verbose = verbose
        self.output_file = output_file
        
        # Get all game IDs from boxscores directory
        boxscore_dir = self.storage_path / 'json' / 'boxscores'
        game_files = list(boxscore_dir.glob('*.json'))
        
        total_games = len(game_files)
        reconciled_games = 0
        failed_games = 0
        
        season_summary = {
            'total_games': total_games,
            'reconciled_games': 0,
            'failed_games': 0,
            'total_goals': 0,
            'total_players_analyzed': 0,
            'perfect_reconciliations': 0,
            'minor_discrepancies': 0,
            'major_discrepancies': 0,
            'team_reconciliations': {},
            'player_reconciliations': {},
            'critical_issues': [],
            'reconciliation_results': []
        }
        
        for i, game_file in enumerate(game_files, 1):
            game_id = game_file.stem
            try:
                # Show progress indicator
                progress_percent = (i / total_games) * 100
                print(f"\rProcessing game {i}/{total_games} ({progress_percent:.1f}%) - Game {game_id}", end='', flush=True)
                
                result = self.reconcile_game(game_id)
                if result:
                    # Display readable game result if verbose mode is enabled
                    if self.verbose:
                        self._display_game_result(result, i, total_games)
                    
                    season_summary['reconciliation_results'].append(result)
                    reconciled_games += 1
                    season_summary['total_goals'] += result.total_goals
                    season_summary['total_players_analyzed'] += len(result.player_results)
                    
                    # Count reconciliation statuses
                    for player_result in result.player_results:
                        if player_result.reconciliation_status == 'perfect':
                            season_summary['perfect_reconciliations'] += 1
                        elif player_result.reconciliation_status == 'minor_discrepancy':
                            season_summary['minor_discrepancies'] += 1
                        elif player_result.reconciliation_status == 'major_discrepancy':
                            season_summary['major_discrepancies'] += 1
                    
                    # Track critical issues
                    season_summary['critical_issues'].extend(result.critical_discrepancies)
                    
                else:
                    print(f"\n❌ Failed to process game {game_id}")
                    failed_games += 1
                    
            except Exception as e:
                print(f"\n❌ Error processing game {game_id}: {e}")
                logger.error(f"Failed to reconcile game {game_id}: {e}")
                failed_games += 1
        
        # Clear the progress line and show completion
        print(f"\r{' ' * 80}\r", end='', flush=True)
        
        season_summary['reconciled_games'] = reconciled_games
        season_summary['failed_games'] = failed_games
        
        # Calculate overall reconciliation percentage
        total_player_checks = season_summary['total_players_analyzed']
        if total_player_checks > 0:
            perfect_percentage = (season_summary['perfect_reconciliations'] / total_player_checks) * 100
            season_summary['overall_reconciliation_percentage'] = perfect_percentage
        
        print(f"\n✅ Player/Team reconciliation complete: {reconciled_games}/{total_games} games")
        print(f"📊 Overall reconciliation: {season_summary.get('overall_reconciliation_percentage', 0):.1f}%")
        logger.info(f"Player/Team reconciliation complete: {reconciled_games}/{total_games} games")
        logger.info(f"Overall reconciliation: {season_summary.get('overall_reconciliation_percentage', 0):.1f}%")
        
        return season_summary
    
    def _display_game_result(self, result: GameReconciliationResult, game_num: int, total_games: int) -> None:
        """Display a readable game reconciliation result."""
        # Clear the progress line
        print(f"\r{' ' * 80}\r", end='', flush=True)
        
        # Build the output text
        output_lines = []
        
        # Game header
        output_lines.append(f"\nGame {game_num}/{total_games}: {result.game_id}")
        output_lines.append(f"{result.game_date} | {result.home_team} vs {result.away_team}")
        output_lines.append(f"Total Goals: {result.total_goals}")
        
        # Team reconciliation
        output_lines.append(f"\nTeam Reconciliation:")
        output_lines.append(f"Sources: Play-by-Play JSON (Auth) | Game Summary HTML (GS) | Event Summary HTML (ES) | Play-by-Play HTML (PL)")
        for team_abbrev, team_data in result.team_results.items():
            # Only show warning icon if there are actual discrepancies
            has_discrepancy = (team_data.gs_discrepancy != 0 or team_data.es_discrepancy != 0 or team_data.pl_discrepancy != 0)
            status_icon = "⚠️ " if has_discrepancy else ""
            output_lines.append(f"  {status_icon}{team_abbrev}: {team_data.authoritative_goals} goals")
            output_lines.append(f"    Auth: {team_data.authoritative_goals} | GS: {team_data.html_gs_goals} (Δ{team_data.gs_discrepancy}) | ES: {team_data.html_es_goals} (Δ{team_data.es_discrepancy}) | PL: {team_data.html_pl_goals} (Δ{team_data.pl_discrepancy})")
        
        # Player reconciliation summary
        perfect_count = sum(1 for p in result.player_results if p.reconciliation_status == 'perfect')
        minor_count = sum(1 for p in result.player_results if p.reconciliation_status == 'minor_discrepancy')
        major_count = sum(1 for p in result.player_results if p.reconciliation_status == 'major_discrepancy')
        
        output_lines.append(f"\nPlayer Reconciliation: {len(result.player_results)} players")
        output_lines.append(f"  Perfect: {perfect_count} | Minor: {minor_count} | Major: {major_count}")
        
        # Show any actual discrepancies (non-zero differences)
        actual_discrepancies = [p for p in result.player_results if p.goal_discrepancy != 0 or p.assist_discrepancy != 0]
        if actual_discrepancies:
            output_lines.append(f"\n⚠️ Actual Discrepancies Found:")
            for player in actual_discrepancies[:3]:  # Show first 3
                output_lines.append(f"  {player.player_name} #{player.sweater_number} ({player.team}):")
                output_lines.append(f"    Goals: Auth={player.authoritative_goals}, HTML={player.html_goals} (Δ{player.goal_discrepancy})")
                output_lines.append(f"    Assists: Auth={player.authoritative_assists}, HTML={player.html_assists} (Δ{player.assist_discrepancy})")
            if len(actual_discrepancies) > 3:
                output_lines.append(f"  ... and {len(actual_discrepancies) - 3} more")
        else:
            output_lines.append(f"  All player data matches perfectly!")
        
        # Show detailed player breakdown for quality assessment
        output_lines.append(f"\nDetailed Player Breakdown:")
        output_lines.append(f"Primary Comparison: Play-by-Play JSON vs Game Summary HTML (Status based on GS match)")
        for player in result.player_results:
            # Only show error icon if there are actual discrepancies
            has_discrepancy = (player.goal_discrepancy != 0 or player.assist_discrepancy != 0)
            status_icon = "⚠️ " if has_discrepancy else ""
            output_lines.append(f"  {status_icon}{player.player_name} #{player.sweater_number} ({player.team}):")
            output_lines.append(f"    Goals: Auth={player.authoritative_goals}, HTML={player.html_goals} (Δ{player.goal_discrepancy})")
            output_lines.append(f"    Assists: Auth={player.authoritative_assists}, HTML={player.html_assists} (Δ{player.assist_discrepancy})")
            output_lines.append(f"    Status: {player.reconciliation_status}")
        
        # Overall status
        overall_status = "Perfect" if result.overall_reconciliation_percentage == 100.0 else f"⚠️ {result.overall_reconciliation_percentage:.1f}%"
        output_lines.append(f"\nOverall Status: {overall_status}")
        output_lines.append("-" * 60)
        
        # Display to console
        for line in output_lines:
            print(line)
        
        # Write to file if verbose mode and output file specified
        if self.verbose and hasattr(self, 'output_file') and self.output_file:
            with open(self.output_file, 'a', encoding='utf-8') as f:
                for line in output_lines:
                    f.write(line + '\n')
    
    def reconcile_game(self, game_id: str) -> Optional[GameReconciliationResult]:
        """Reconcile player and team goal data for a specific game."""
        # Progress is now shown in the main loop, no need for individual game logging
        
        try:
            # Load authoritative data (Play-by-Play JSON)
            authoritative_goals = self._extract_authoritative_goals(game_id)
            if not authoritative_goals:
                logger.warning(f"No authoritative goal data found for game {game_id}")
                return None
            
            # Load HTML report data (GS)
            html_gs_goals = self._extract_html_goals(game_id)
            
            # Load Event Summary (ES) report data
            html_es_goals = self._extract_es_goals(game_id)
            
            # Load Play-by-Play (PL) report data
            html_pl_goals = self._extract_pl_goals(game_id)
            
            # Get game metadata
            game_metadata = self._get_game_metadata(game_id)
            
            # Perform team-level reconciliation (four-way comparison)
            team_results = self._reconcile_teams_four_way(authoritative_goals, html_gs_goals, html_es_goals, html_pl_goals, game_id)
            
            # Perform player-level reconciliation (four-way comparison)
            player_results = self._reconcile_players_four_way(authoritative_goals, html_gs_goals, html_es_goals, html_pl_goals, game_id)
            
            # Calculate overall reconciliation percentage
            total_players = len(player_results)
            perfect_players = len([p for p in player_results if p.reconciliation_status == 'perfect'])
            reconciliation_percentage = (perfect_players / total_players * 100) if total_players > 0 else 100
            
            # Identify critical discrepancies
            critical_discrepancies = self._identify_critical_discrepancies(team_results, player_results)
            
            # Generate warnings
            warnings = self._generate_warnings(team_results, player_results)
            
            result = GameReconciliationResult(
                game_id=game_id,
                game_date=game_metadata.get('date', ''),
                home_team=game_metadata.get('home_team', ''),
                away_team=game_metadata.get('away_team', ''),
                total_goals=len(authoritative_goals),
                team_results=team_results,
                player_results=player_results,
                overall_reconciliation_percentage=reconciliation_percentage,
                critical_discrepancies=critical_discrepancies,
                warnings=warnings
            )
            
            return result
            
        except Exception as e:
            logger.error(f"Error reconciling game {game_id}: {e}")
            return None
    
    def _extract_authoritative_goals(self, game_id: str) -> List[Dict[str, Any]]:
        """Extract authoritative goal data from Play-by-Play JSON."""
        pbp_file = self.storage_path / 'json' / 'playbyplay' / f'{game_id}.json'
        if not pbp_file.exists():
            return []
        
        with open(pbp_file, 'r') as f:
            pbp_data = json.load(f)
        
        goals = []
        player_mappings = self._load_player_mappings(game_id)
        
        for event in pbp_data.get('plays', []):
            if event.get('typeCode') == self.GOAL_EVENT_TYPE:
                details = event.get('details', {})
                period_descriptor = event.get('periodDescriptor', {})
                period = period_descriptor.get('number', 1)
                period_type = period_descriptor.get('periodType', 'REGULAR')
                
                # Extract goal information
                scorer_id = details.get('scoringPlayerId')
                assist1_id = details.get('assist1PlayerId')
                assist2_id = details.get('assist2PlayerId')
                
                # Get player names
                scorer_name = player_mappings.get(scorer_id, {}).get('name', f'Player_{scorer_id}')
                assist1_name = player_mappings.get(assist1_id, {}).get('name') if assist1_id else None
                assist2_name = player_mappings.get(assist2_id, {}).get('name') if assist2_id else None
                
                # Get sweater numbers
                scorer_sweater = player_mappings.get(scorer_id, {}).get('sweaterNumber', 0)
                assist1_sweater = player_mappings.get(assist1_id, {}).get('sweaterNumber') if assist1_id else None
                assist2_sweater = player_mappings.get(assist2_id, {}).get('sweaterNumber') if assist2_id else None
                
                # Determine team
                team_id = details.get('eventOwnerTeamId')
                team = self.team_id_mappings.get(team_id, f'Team_{team_id}')
                
                # Determine if this is a shootout goal
                is_shootout = period_type == 'SHOOTOUT'
                
                goal = {
                    'goal_number': len(goals) + 1,
                    'period': period,
                    'period_type': period_type,
                    'time': event.get('timeInPeriod', '00:00'),
                    'team': team,
                    'team_id': team_id,
                    'scorer_id': scorer_id,
                    'scorer_name': scorer_name,
                    'scorer_sweater': scorer_sweater,
                    'assist1_id': assist1_id,
                    'assist1_name': assist1_name,
                    'assist1_sweater': assist1_sweater,
                    'assist2_id': assist2_id,
                    'assist2_name': assist2_name,
                    'assist2_sweater': assist2_sweater,
                    'is_shootout': is_shootout,
                    'counts_for_stats': not is_shootout,  # Shootout goals don't count for player stats
                    'source': 'authoritative'
                }
                
                goals.append(goal)
        
        return goals
    
    def _extract_html_goals(self, game_id: str) -> List[Dict[str, Any]]:
        """Extract goal data from HTML reports (GS)."""
        # GS files use format: gs_020001.json (last 6 digits of game ID)
        gs_file = self.storage_path / 'json' / 'curate' / 'gs' / f'gs_{game_id[4:]}.json'
        if not gs_file.exists():
            return []
        
        with open(gs_file, 'r') as f:
            gs_data = json.load(f)
        
        goals = []
        scoring_summary = gs_data.get('scoring_summary', {})
        
        # Load player mappings to get player IDs from sweater numbers
        player_mappings = self._load_player_mappings(game_id)
        
        for goal_data in scoring_summary.get('goals', []):
            scorer = goal_data.get('scorer', {})
            assist1 = goal_data.get('assist1', {})
            assist2 = goal_data.get('assist2', {})
            
            # Determine if this is a shootout goal based on period
            period = goal_data.get('period', 1)
            is_shootout = period == 5  # Shootout is typically period 5 in GS reports
            
            # Get player IDs from sweater numbers
            scorer_sweater = scorer.get('sweater_number', 0)
            scorer_id = self._get_player_id_from_sweater(player_mappings, scorer_sweater, goal_data.get('team', ''))
            
            assist1_sweater = assist1.get('sweater_number') if assist1 else None
            assist1_id = self._get_player_id_from_sweater(player_mappings, assist1_sweater, goal_data.get('team', '')) if assist1_sweater else None
            
            assist2_sweater = assist2.get('sweater_number') if assist2 else None
            assist2_id = self._get_player_id_from_sweater(player_mappings, assist2_sweater, goal_data.get('team', '')) if assist2_sweater else None
            
            goal = {
                'goal_number': goal_data.get('goal_number', 0),
                'period': period,
                'period_type': 'SHOOTOUT' if is_shootout else 'REGULAR',
                'time': goal_data.get('time', '00:00'),
                'team': goal_data.get('team', ''),
                'team_id': self.team_abbrev_to_id.get(goal_data.get('team', ''), 0),
                'scorer_id': scorer_id,
                'scorer_name': scorer.get('name', ''),
                'scorer_sweater': scorer_sweater,
                'assist1_id': assist1_id,
                'assist1_name': assist1.get('name') if assist1 else None,
                'assist1_sweater': assist1_sweater,
                'assist2_id': assist2_id,
                'assist2_name': assist2.get('name') if assist2 else None,
                'assist2_sweater': assist2_sweater,
                'is_shootout': is_shootout,
                'counts_for_stats': not is_shootout,  # Shootout goals don't count for player stats
                'source': 'html_gs'
            }
            
            goals.append(goal)
        
        return goals
    
    def _extract_es_goals(self, game_id: str) -> List[Dict[str, Any]]:
        """Extract goal data from Event Summary (ES) reports."""
        # ES files use format: es_020001.json (last 6 digits of game ID)
        es_file = self.storage_path / 'json' / 'curate' / 'es' / f'es_{game_id[4:]}.json'
        if not es_file.exists():
            return []
        
        with open(es_file, 'r') as f:
            es_data = json.load(f)
        
        goals = []
        
        # ES reports contain player statistics, not individual goal events
        # We need to reconstruct goal events from player statistics
        player_stats = es_data.get('player_statistics', {})
        
        # Process visitor team players
        visitor_players = player_stats.get('visitor', [])
        visitor_team = es_data.get('game_header', {}).get('visitor_team', {}).get('abbreviation', '')
        
        for player in visitor_players:
            player_goals = player.get('goals', 0)
            player_assists = player.get('assists', 0)
            
            # Create goal events for each goal scored by this player
            for goal_num in range(player_goals):
                goal = {
                    'goal_number': goal_num + 1,
                    'period': 1,  # ES doesn't provide period info, default to 1
                    'period_type': 'REGULAR',  # ES doesn't distinguish shootout goals
                    'time': '00:00',  # ES doesn't provide time info
                    'team': visitor_team,
                    'team_id': player.get('team_id', 0),
                    'scorer_id': player.get('player_id', 0),
                    'scorer_name': player.get('name', ''),
                    'scorer_sweater': player.get('sweater_number', 0),
                    'assist1_id': None,  # ES doesn't provide assist details
                    'assist1_name': None,
                    'assist1_sweater': None,
                    'assist2_id': None,
                    'assist2_name': None,
                    'assist2_sweater': None,
                    'is_shootout': False,  # ES doesn't distinguish shootout goals
                    'counts_for_stats': True,  # All ES goals count for stats
                    'source': 'html_es'
                }
                goals.append(goal)
        
        # Process home team players
        home_players = player_stats.get('home', [])
        home_team = es_data.get('game_header', {}).get('home_team', {}).get('abbreviation', '')
        
        for player in home_players:
            player_goals = player.get('goals', 0)
            player_assists = player.get('assists', 0)
            
            # Create goal events for each goal scored by this player
            for goal_num in range(player_goals):
                goal = {
                    'goal_number': goal_num + 1,
                    'period': 1,  # ES doesn't provide period info, default to 1
                    'period_type': 'REGULAR',  # ES doesn't distinguish shootout goals
                    'time': '00:00',  # ES doesn't provide time info
                    'team': home_team,
                    'team_id': player.get('team_id', 0),
                    'scorer_id': player.get('player_id', 0),
                    'scorer_name': player.get('name', ''),
                    'scorer_sweater': player.get('sweater_number', 0),
                    'assist1_id': None,  # ES doesn't provide assist details
                    'assist1_name': None,
                    'assist1_sweater': None,
                    'assist2_id': None,
                    'assist2_name': None,
                    'assist2_sweater': None,
                    'is_shootout': False,  # ES doesn't distinguish shootout goals
                    'counts_for_stats': True,  # All ES goals count for stats
                    'source': 'html_es'
                }
                goals.append(goal)
        
        return goals
    
    def _extract_pl_goals(self, game_id: str) -> List[Dict[str, Any]]:
        """Extract goal data from Play-by-Play (PL) HTML reports."""
        # PL files use format: PL020001.HTM (last 6 digits of game ID)
        pl_file = self.storage_path / 'html' / 'reports' / 'PL' / f'PL{game_id[4:]}.HTM'
        if not pl_file.exists():
            return []
        
        from bs4 import BeautifulSoup
        import re
        
        with open(pl_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        soup = BeautifulSoup(content, 'html.parser')
        goals = []
        
        # Load player mappings to get player IDs from sweater numbers
        player_mappings = self._load_player_mappings(game_id)
        
        # Find all goal events (td elements with class containing "goal")
        goal_cells = soup.find_all('td', class_=re.compile(r'.*goal.*'))
        
        # Group goal cells by their parent row
        goal_rows = {}
        for cell in goal_cells:
            row = cell.find_parent('tr')
            if row and row not in goal_rows:
                goal_rows[row] = row.find_all('td')
        
        for row, cells in goal_rows.items():
            if len(cells) >= 6:  # Ensure we have enough cells for goal data
                # Extract goal information
                event_number = cells[0].get_text(strip=True)
                period = cells[1].get_text(strip=True)
                period_type = cells[2].get_text(strip=True)
                time_text = cells[3].get_text(strip=True)
                event_type = cells[4].get_text(strip=True)
                description = cells[5].get_text(strip=True)
                
                if event_type == 'GOAL':
                    # Parse the goal description to extract scorer and assists
                    # Format: "NJD #11 NOESEN(1), Snap , Off. Zone, 21 ft.\nAssist: #8 KOVACEVIC(1)"
                    goal_info = self._parse_pl_goal_description(description, player_mappings)
                    
                    if goal_info:
                        # Determine if this is a shootout goal
                        is_shootout = period == '5' or period_type == 'SO'
                        
                        goal = {
                            'goal_number': int(event_number) if event_number.isdigit() else 0,
                            'period': int(period) if period.isdigit() else 1,
                            'period_type': period_type,
                            'time': time_text,
                            'team': goal_info['team'],
                            'team_id': self.team_abbrev_to_id.get(goal_info['team'], 0),
                            'scorer_id': goal_info['scorer_id'],
                            'scorer_name': goal_info['scorer_name'],
                            'scorer_sweater': goal_info['scorer_sweater'],
                            'assist1_id': goal_info.get('assist1_id'),
                            'assist1_name': goal_info.get('assist1_name'),
                            'assist1_sweater': goal_info.get('assist1_sweater'),
                            'assist2_id': goal_info.get('assist2_id'),
                            'assist2_name': goal_info.get('assist2_name'),
                            'assist2_sweater': goal_info.get('assist2_sweater'),
                            'is_shootout': is_shootout,
                            'counts_for_stats': not is_shootout,  # Shootout goals don't count for player stats
                            'source': 'html_pl'
                        }
                        
                        goals.append(goal)
        
        return goals
    
    def _parse_pl_goal_description(self, description: str, player_mappings: Dict[int, Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Parse PL goal description to extract scorer and assists."""
        try:
            # Split by newlines to separate scorer and assists
            lines = description.split('\n')
            scorer_line = lines[0].strip()
            
            # Extract team from scorer line (e.g., "NJD #11 NOESEN(1)")
            team_match = re.match(r'^([A-Z]{3})\s+#(\d+)\s+([^(]+)', scorer_line)
            if not team_match:
                return None
                
            team = team_match.group(1)
            scorer_sweater = int(team_match.group(2))
            scorer_name = team_match.group(3).strip()
            
            # Get scorer player ID
            scorer_id = self._get_player_id_from_sweater(player_mappings, scorer_sweater, team)
            
            goal_info = {
                'team': team,
                'scorer_id': scorer_id,
                'scorer_name': scorer_name,
                'scorer_sweater': scorer_sweater
            }
            
            # Look for assists in subsequent lines
            for line in lines[1:]:
                line = line.strip()
                if line.startswith('Assist:'):
                    # Parse assist line (e.g., "Assist: #8 KOVACEVIC(1)")
                    assist_match = re.search(r'#(\d+)\s+([^(]+)', line)
                    if assist_match:
                        assist_sweater = int(assist_match.group(1))
                        assist_name = assist_match.group(2).strip()
                        assist_id = self._get_player_id_from_sweater(player_mappings, assist_sweater, team)
                        
                        if 'assist1_id' not in goal_info:
                            goal_info.update({
                                'assist1_id': assist_id,
                                'assist1_name': assist_name,
                                'assist1_sweater': assist_sweater
                            })
                        else:
                            goal_info.update({
                                'assist2_id': assist_id,
                                'assist2_name': assist_name,
                                'assist2_sweater': assist_sweater
                            })
            
            return goal_info
            
        except Exception as e:
            self.logger.error(f"Error parsing PL goal description: {e}")
            return None
    
    def _get_game_metadata(self, game_id: str) -> Dict[str, Any]:
        """Get game metadata from boxscore."""
        boxscore_file = self.storage_path / 'json' / 'boxscores' / f'{game_id}.json'
        if not boxscore_file.exists():
            return {}
        
        with open(boxscore_file, 'r') as f:
            boxscore_data = json.load(f)
        
        return {
            'date': boxscore_data.get('gameDate', ''),
            'home_team': boxscore_data.get('homeTeam', {}).get('abbrev', ''),
            'away_team': boxscore_data.get('awayTeam', {}).get('abbrev', ''),
            'home_score': boxscore_data.get('homeTeam', {}).get('score', 0),
            'away_score': boxscore_data.get('awayTeam', {}).get('score', 0)
        }
    
    def _reconcile_teams(self, authoritative_goals: List[Dict], html_goals: List[Dict], game_id: str) -> Dict[str, TeamReconciliationResult]:
        """Reconcile team-level goal data."""
        team_results = {}
        
        # Group goals by team for authoritative data (only count non-shootout goals for team stats)
        auth_team_goals = defaultdict(list)
        auth_team_goals_stats = defaultdict(list)  # Goals that count for statistics
        
        for goal in authoritative_goals:
            team = goal['team']
            auth_team_goals[team].append(goal)
            if goal.get('counts_for_stats', True):  # Only count non-shootout goals for team stats
                auth_team_goals_stats[team].append(goal)
        
        # Group goals by team for HTML data (only count non-shootout goals for team stats)
        html_team_goals = defaultdict(list)
        html_team_goals_stats = defaultdict(list)  # Goals that count for statistics
        
        for goal in html_goals:
            team = goal['team']
            html_team_goals[team].append(goal)
            if goal.get('counts_for_stats', True):  # Only count non-shootout goals for team stats
                html_team_goals_stats[team].append(goal)
        
        # Reconcile each team (using stats-counting goals for team totals)
        for team in set(list(auth_team_goals.keys()) + list(html_team_goals.keys())):
            auth_goals = auth_team_goals.get(team, [])
            html_goals_team = html_team_goals.get(team, [])
            auth_goals_stats = auth_team_goals_stats.get(team, [])
            html_goals_stats = html_team_goals_stats.get(team, [])
            
            # Use stats-counting goals for team reconciliation
            goal_count_discrepancy = len(auth_goals_stats) - len(html_goals_stats)
            
            # Determine reconciliation status
            if goal_count_discrepancy == 0:
                status = 'perfect'
            elif abs(goal_count_discrepancy) <= 1:
                status = 'minor_discrepancy'
            else:
                status = 'major_discrepancy'
            
            team_result = TeamReconciliationResult(
                team_id=self.team_abbrev_to_id.get(team, 0),
                team_abbrev=team,
                authoritative_total_goals=len(auth_goals_stats),  # Only count non-shootout goals
                html_total_goals=len(html_goals_stats),  # Only count non-shootout goals
                goal_count_discrepancy=goal_count_discrepancy,
                player_discrepancies=[],  # Will be filled by player reconciliation
                reconciliation_status=status
            )
            
            team_results[team] = team_result
        
        return team_results
    
    def _normalize_player_name(self, name: str) -> str:
        """Normalize player name for consistent matching."""
        if not name:
            return ""
        # Convert to title case and handle common formatting differences
        # Remove extra spaces and normalize periods
        normalized = name.title().replace('.', '. ').replace('  ', ' ').strip()
        return normalized
    
    def _reconcile_players(self, authoritative_goals: List[Dict], html_goals: List[Dict], game_id: str) -> List[PlayerReconciliationResult]:
        """Reconcile player-level goal data."""
        player_results = []
        
        # Count goals and assists for each player in authoritative data (only non-shootout goals count for stats)
        auth_player_stats = defaultdict(lambda: {'goals': 0, 'assists': 0, 'player_id': 0, 'name': '', 'sweater': 0, 'team': ''})
        
        for goal in authoritative_goals:
            # Only count non-shootout goals for player statistics
            if not goal.get('counts_for_stats', True):
                continue
                
            # Count goals using player ID as key
            scorer_id = goal.get('scorer_id')
            if scorer_id:
                auth_player_stats[scorer_id]['goals'] += 1
                auth_player_stats[scorer_id]['player_id'] = scorer_id
                auth_player_stats[scorer_id]['name'] = goal['scorer_name']
                auth_player_stats[scorer_id]['sweater'] = goal['scorer_sweater']
                auth_player_stats[scorer_id]['team'] = goal['team']
            
            # Count assists
            assist1_id = goal.get('assist1_id')
            if assist1_id:
                auth_player_stats[assist1_id]['assists'] += 1
                auth_player_stats[assist1_id]['player_id'] = assist1_id
                auth_player_stats[assist1_id]['name'] = goal['assist1_name']
                auth_player_stats[assist1_id]['sweater'] = goal['assist1_sweater']
                auth_player_stats[assist1_id]['team'] = goal['team']
            
            assist2_id = goal.get('assist2_id')
            if assist2_id:
                auth_player_stats[assist2_id]['assists'] += 1
                auth_player_stats[assist2_id]['player_id'] = assist2_id
                auth_player_stats[assist2_id]['name'] = goal['assist2_name']
                auth_player_stats[assist2_id]['sweater'] = goal['assist2_sweater']
                auth_player_stats[assist2_id]['team'] = goal['team']
        
        # Count goals and assists for each player in HTML data (only non-shootout goals count for stats)
        html_player_stats = defaultdict(lambda: {'goals': 0, 'assists': 0, 'player_id': 0, 'name': '', 'sweater': 0, 'team': ''})
        
        for goal in html_goals:
            # Only count non-shootout goals for player statistics
            if not goal.get('counts_for_stats', True):
                continue
                
            # Count goals using player ID as key
            scorer_id = goal.get('scorer_id')
            if scorer_id:
                html_player_stats[scorer_id]['goals'] += 1
                html_player_stats[scorer_id]['player_id'] = scorer_id
                html_player_stats[scorer_id]['name'] = goal['scorer_name']
                html_player_stats[scorer_id]['sweater'] = goal['scorer_sweater']
                html_player_stats[scorer_id]['team'] = goal['team']
            
            # Count assists
            assist1_id = goal.get('assist1_id')
            if assist1_id:
                html_player_stats[assist1_id]['assists'] += 1
                html_player_stats[assist1_id]['player_id'] = assist1_id
                html_player_stats[assist1_id]['name'] = goal['assist1_name']
                html_player_stats[assist1_id]['sweater'] = goal['assist1_sweater']
                html_player_stats[assist1_id]['team'] = goal['team']
            
            assist2_id = goal.get('assist2_id')
            if assist2_id:
                html_player_stats[assist2_id]['assists'] += 1
                html_player_stats[assist2_id]['player_id'] = assist2_id
                html_player_stats[assist2_id]['name'] = goal['assist2_name']
                html_player_stats[assist2_id]['sweater'] = goal['assist2_sweater']
                html_player_stats[assist2_id]['team'] = goal['team']
        
        # Compare player statistics using player IDs
        all_player_ids = set(list(auth_player_stats.keys()) + list(html_player_stats.keys()))
        
        for player_id in all_player_ids:
            auth_stats = auth_player_stats.get(player_id, {'goals': 0, 'assists': 0, 'player_id': 0, 'name': '', 'sweater': 0, 'team': ''})
            html_stats = html_player_stats.get(player_id, {'goals': 0, 'assists': 0, 'player_id': 0, 'name': '', 'sweater': 0, 'team': ''})
            
            goal_discrepancy = auth_stats['goals'] - html_stats['goals']
            assist_discrepancy = auth_stats['assists'] - html_stats['assists']
            
            # Determine reconciliation status
            total_discrepancy = abs(goal_discrepancy) + abs(assist_discrepancy)
            if total_discrepancy == 0:
                status = 'perfect'
            elif total_discrepancy <= 1:
                status = 'minor_discrepancy'
            else:
                status = 'major_discrepancy'
            
            # Use authoritative data for player info (more reliable)
            player_name = auth_stats['name'] or html_stats['name']
            sweater_number = auth_stats['sweater'] or html_stats['sweater']
            team = auth_stats['team'] or html_stats['team']
            
            player_result = PlayerReconciliationResult(
                player_id=player_id,
                player_name=player_name,
                sweater_number=sweater_number,
                team=team,
                authoritative_goals=auth_stats['goals'],
                authoritative_assists=auth_stats['assists'],
                html_goals=html_stats['goals'],
                html_assists=html_stats['assists'],
                goal_discrepancy=goal_discrepancy,
                assist_discrepancy=assist_discrepancy,
                reconciliation_status=status
            )
            
            player_results.append(player_result)
        
        return player_results
    
    def _reconcile_teams_three_way(self, authoritative_goals: List[Dict], html_gs_goals: List[Dict], 
                                  html_es_goals: List[Dict], game_id: str) -> Dict[str, TeamReconciliationResult]:
        """Reconcile team-level goal data across three sources."""
        team_results = {}
        
        # Get team abbreviations from metadata
        metadata = self._get_game_metadata(game_id)
        home_team = metadata.get('home_team', '')
        away_team = metadata.get('away_team', '')
        
        for team in [home_team, away_team]:
            if not team:
                continue
                
            # Count goals for each source (only non-shootout goals count for team stats)
            auth_goals = sum(1 for goal in authoritative_goals 
                           if goal.get('team') == team and goal.get('counts_for_stats', True))
            gs_goals = sum(1 for goal in html_gs_goals 
                          if goal.get('team') == team and goal.get('counts_for_stats', True))
            es_goals = sum(1 for goal in html_es_goals 
                          if goal.get('team') == team and goal.get('counts_for_stats', True))
            
            # Calculate discrepancies
            gs_discrepancy = auth_goals - gs_goals
            es_discrepancy = auth_goals - es_goals
            
            # Determine reconciliation status
            gs_status = 'perfect' if gs_discrepancy == 0 else 'minor_discrepancy' if abs(gs_discrepancy) <= 1 else 'major_discrepancy'
            es_status = 'perfect' if es_discrepancy == 0 else 'minor_discrepancy' if abs(es_discrepancy) <= 1 else 'major_discrepancy'
            
            team_results[team] = TeamReconciliationResult(
                team_id=self.team_abbrev_to_id.get(team, 0),
                team_abbrev=team,
                authoritative_goals=auth_goals,
                html_gs_goals=gs_goals,
                html_es_goals=es_goals,
                gs_discrepancy=gs_discrepancy,
                es_discrepancy=es_discrepancy,
                gs_reconciliation_status=gs_status,
                es_reconciliation_status=es_status,
                # Backward compatibility fields
                authoritative_total_goals=auth_goals,
                html_total_goals=gs_goals,
                goal_count_discrepancy=gs_discrepancy,
                player_discrepancies=[],
                reconciliation_status=overall_status
            )
        
        return team_results
    
    def _reconcile_players_three_way(self, authoritative_goals: List[Dict], html_gs_goals: List[Dict], 
                                    html_es_goals: List[Dict], game_id: str) -> List[PlayerReconciliationResult]:
        """Reconcile player-level goal data across three sources."""
        player_results = []
        
        # Count goals and assists for each player in authoritative data (only non-shootout goals count for stats)
        auth_player_stats = defaultdict(lambda: {'goals': 0, 'assists': 0, 'player_id': 0, 'name': '', 'sweater': 0, 'team': ''})
        
        for goal in authoritative_goals:
            # Only count non-shootout goals for player statistics
            if not goal.get('counts_for_stats', True):
                continue
                
            # Count goals using player ID as key
            scorer_id = goal.get('scorer_id')
            if scorer_id:
                auth_player_stats[scorer_id]['goals'] += 1
                auth_player_stats[scorer_id]['player_id'] = scorer_id
                auth_player_stats[scorer_id]['name'] = goal['scorer_name']
                auth_player_stats[scorer_id]['sweater'] = goal['scorer_sweater']
                auth_player_stats[scorer_id]['team'] = goal['team']
            
            # Count assists
            assist1_id = goal.get('assist1_id')
            if assist1_id:
                auth_player_stats[assist1_id]['assists'] += 1
                auth_player_stats[assist1_id]['player_id'] = assist1_id
                auth_player_stats[assist1_id]['name'] = goal['assist1_name']
                auth_player_stats[assist1_id]['sweater'] = goal['assist1_sweater']
                auth_player_stats[assist1_id]['team'] = goal['team']
            
            assist2_id = goal.get('assist2_id')
            if assist2_id:
                auth_player_stats[assist2_id]['assists'] += 1
                auth_player_stats[assist2_id]['player_id'] = assist2_id
                auth_player_stats[assist2_id]['name'] = goal['assist2_name']
                auth_player_stats[assist2_id]['sweater'] = goal['assist2_sweater']
                auth_player_stats[assist2_id]['team'] = goal['team']
        
        # Count goals and assists for each player in GS data (only non-shootout goals count for stats)
        gs_player_stats = defaultdict(lambda: {'goals': 0, 'assists': 0, 'player_id': 0, 'name': '', 'sweater': 0, 'team': ''})
        
        for goal in html_gs_goals:
            # Only count non-shootout goals for player statistics
            if not goal.get('counts_for_stats', True):
                continue
                
            # Count goals using player ID as key
            scorer_id = goal.get('scorer_id')
            if scorer_id:
                gs_player_stats[scorer_id]['goals'] += 1
                gs_player_stats[scorer_id]['player_id'] = scorer_id
                gs_player_stats[scorer_id]['name'] = goal['scorer_name']
                gs_player_stats[scorer_id]['sweater'] = goal['scorer_sweater']
                gs_player_stats[scorer_id]['team'] = goal['team']
            
            # Count assists
            assist1_id = goal.get('assist1_id')
            if assist1_id:
                gs_player_stats[assist1_id]['assists'] += 1
                gs_player_stats[assist1_id]['player_id'] = assist1_id
                gs_player_stats[assist1_id]['name'] = goal['assist1_name']
                gs_player_stats[assist1_id]['sweater'] = goal['assist1_sweater']
                gs_player_stats[assist1_id]['team'] = goal['team']
            
            assist2_id = goal.get('assist2_id')
            if assist2_id:
                gs_player_stats[assist2_id]['assists'] += 1
                gs_player_stats[assist2_id]['player_id'] = assist2_id
                gs_player_stats[assist2_id]['name'] = goal['assist2_name']
                gs_player_stats[assist2_id]['sweater'] = goal['assist2_sweater']
                gs_player_stats[assist2_id]['team'] = goal['team']
        
        # Count goals and assists for each player in ES data (only non-shootout goals count for stats)
        es_player_stats = defaultdict(lambda: {'goals': 0, 'assists': 0, 'player_id': 0, 'name': '', 'sweater': 0, 'team': ''})
        
        for goal in html_es_goals:
            # Only count non-shootout goals for player statistics
            if not goal.get('counts_for_stats', True):
                continue
                
            # Count goals using player ID as key
            scorer_id = goal.get('scorer_id')
            if scorer_id:
                es_player_stats[scorer_id]['goals'] += 1
                es_player_stats[scorer_id]['player_id'] = scorer_id
                es_player_stats[scorer_id]['name'] = goal['scorer_name']
                es_player_stats[scorer_id]['sweater'] = goal['scorer_sweater']
                es_player_stats[scorer_id]['team'] = goal['team']
            
            # Count assists
            assist1_id = goal.get('assist1_id')
            if assist1_id:
                es_player_stats[assist1_id]['assists'] += 1
                es_player_stats[assist1_id]['player_id'] = assist1_id
                es_player_stats[assist1_id]['name'] = goal['assist1_name']
                es_player_stats[assist1_id]['sweater'] = goal['assist1_sweater']
                es_player_stats[assist1_id]['team'] = goal['team']
            
            assist2_id = goal.get('assist2_id')
            if assist2_id:
                es_player_stats[assist2_id]['assists'] += 1
                es_player_stats[assist2_id]['player_id'] = assist2_id
                es_player_stats[assist2_id]['name'] = goal['assist2_name']
                es_player_stats[assist2_id]['sweater'] = goal['assist2_sweater']
                es_player_stats[assist2_id]['team'] = goal['team']
        
        # Compare player statistics using player IDs
        all_player_ids = set(list(auth_player_stats.keys()) + list(gs_player_stats.keys()) + list(es_player_stats.keys()))
        
        for player_id in all_player_ids:
            auth_stats = auth_player_stats.get(player_id, {'goals': 0, 'assists': 0, 'player_id': 0, 'name': '', 'sweater': 0, 'team': ''})
            gs_stats = gs_player_stats.get(player_id, {'goals': 0, 'assists': 0, 'player_id': 0, 'name': '', 'sweater': 0, 'team': ''})
            es_stats = es_player_stats.get(player_id, {'goals': 0, 'assists': 0, 'player_id': 0, 'name': '', 'sweater': 0, 'team': ''})
            
            gs_goal_discrepancy = auth_stats['goals'] - gs_stats['goals']
            gs_assist_discrepancy = auth_stats['assists'] - gs_stats['assists']
            es_goal_discrepancy = auth_stats['goals'] - es_stats['goals']
            es_assist_discrepancy = auth_stats['assists'] - es_stats['assists']
            
            # Determine reconciliation status
            gs_total_discrepancy = abs(gs_goal_discrepancy) + abs(gs_assist_discrepancy)
            es_total_discrepancy = abs(es_goal_discrepancy) + abs(es_assist_discrepancy)
            
            if gs_total_discrepancy == 0 and es_total_discrepancy == 0:
                status = 'perfect'
            elif gs_total_discrepancy <= 1 and es_total_discrepancy <= 1:
                status = 'minor_discrepancy'
            else:
                status = 'major_discrepancy'
            
            # Use authoritative data for player info (more reliable)
            player_name = auth_stats['name'] or gs_stats['name'] or es_stats['name']
            sweater_number = auth_stats['sweater'] or gs_stats['sweater'] or es_stats['sweater']
            team = auth_stats['team'] or gs_stats['team'] or es_stats['team']
            
            player_result = PlayerReconciliationResult(
                player_id=player_id,
                player_name=player_name,
                sweater_number=sweater_number,
                team=team,
                authoritative_goals=auth_stats['goals'],
                authoritative_assists=auth_stats['assists'],
                html_goals=gs_stats['goals'],  # Keep for backward compatibility
                html_assists=gs_stats['assists'],  # Keep for backward compatibility
                goal_discrepancy=gs_goal_discrepancy,  # Keep for backward compatibility
                assist_discrepancy=gs_assist_discrepancy,  # Keep for backward compatibility
                reconciliation_status=status
            )
            
            player_results.append(player_result)
        
        return player_results
    
    def _reconcile_teams_four_way(self, authoritative_goals: List[Dict], html_gs_goals: List[Dict], 
                                  html_es_goals: List[Dict], html_pl_goals: List[Dict], game_id: str) -> Dict[str, TeamReconciliationResult]:
        """Reconcile team-level goal data across four sources."""
        team_results = {}
        
        # Get team abbreviations from metadata
        metadata = self._get_game_metadata(game_id)
        home_team = metadata.get('home_team', '')
        away_team = metadata.get('away_team', '')
        
        for team in [home_team, away_team]:
            if not team:
                continue
                
            # Count goals for each source (only non-shootout goals count for team stats)
            auth_goals = sum(1 for goal in authoritative_goals 
                           if goal.get('team') == team and goal.get('counts_for_stats', True))
            gs_goals = sum(1 for goal in html_gs_goals 
                          if goal.get('team') == team and goal.get('counts_for_stats', True))
            es_goals = sum(1 for goal in html_es_goals 
                          if goal.get('team') == team and goal.get('counts_for_stats', True))
            pl_goals = sum(1 for goal in html_pl_goals 
                          if goal.get('team') == team and goal.get('counts_for_stats', True))
            
            # Calculate discrepancies
            gs_discrepancy = auth_goals - gs_goals
            es_discrepancy = auth_goals - es_goals
            pl_discrepancy = auth_goals - pl_goals
            
            # Determine reconciliation status - primary based on GS, ES/PL for additional validation
            gs_status = 'perfect' if gs_discrepancy == 0 else 'minor_discrepancy' if abs(gs_discrepancy) <= 1 else 'major_discrepancy'
            es_status = 'perfect' if es_discrepancy == 0 else 'minor_discrepancy' if abs(es_discrepancy) <= 1 else 'major_discrepancy'
            pl_status = 'perfect' if pl_discrepancy == 0 else 'minor_discrepancy' if abs(pl_discrepancy) <= 1 else 'major_discrepancy'
            
            # Overall status based on GS (primary source)
            overall_status = gs_status
            
            team_results[team] = TeamReconciliationResult(
                team_id=self.team_abbrev_to_id.get(team, 0),
                team_abbrev=team,
                authoritative_goals=auth_goals,
                html_gs_goals=gs_goals,
                html_es_goals=es_goals,
                html_pl_goals=pl_goals,
                gs_discrepancy=gs_discrepancy,
                es_discrepancy=es_discrepancy,
                pl_discrepancy=pl_discrepancy,
                gs_reconciliation_status=gs_status,
                es_reconciliation_status=es_status,
                pl_reconciliation_status=pl_status,
                # Backward compatibility fields
                authoritative_total_goals=auth_goals,
                html_total_goals=gs_goals,
                goal_count_discrepancy=gs_discrepancy,
                player_discrepancies=[],
                reconciliation_status=overall_status
            )
        
        return team_results
    
    def _reconcile_players_four_way(self, authoritative_goals: List[Dict], html_gs_goals: List[Dict], 
                                    html_es_goals: List[Dict], html_pl_goals: List[Dict], game_id: str) -> List[PlayerReconciliationResult]:
        """Reconcile player-level goal data across four sources."""
        player_results = []
        
        # Count goals and assists for each player in authoritative data (only non-shootout goals count for stats)
        auth_player_stats = defaultdict(lambda: {'goals': 0, 'assists': 0, 'player_id': 0, 'name': '', 'sweater': 0, 'team': ''})
        
        for goal in authoritative_goals:
            # Only count non-shootout goals for player statistics
            if not goal.get('counts_for_stats', True):
                continue
                
            # Count goals using player ID as key
            scorer_id = goal.get('scorer_id')
            if scorer_id:
                auth_player_stats[scorer_id]['goals'] += 1
                auth_player_stats[scorer_id]['player_id'] = scorer_id
                auth_player_stats[scorer_id]['name'] = goal['scorer_name']
                auth_player_stats[scorer_id]['sweater'] = goal['scorer_sweater']
                auth_player_stats[scorer_id]['team'] = goal['team']
            
            # Count assists
            assist1_id = goal.get('assist1_id')
            if assist1_id:
                auth_player_stats[assist1_id]['assists'] += 1
                auth_player_stats[assist1_id]['player_id'] = assist1_id
                auth_player_stats[assist1_id]['name'] = goal['assist1_name']
                auth_player_stats[assist1_id]['sweater'] = goal['assist1_sweater']
                auth_player_stats[assist1_id]['team'] = goal['team']
            
            assist2_id = goal.get('assist2_id')
            if assist2_id:
                auth_player_stats[assist2_id]['assists'] += 1
                auth_player_stats[assist2_id]['player_id'] = assist2_id
                auth_player_stats[assist2_id]['name'] = goal['assist2_name']
                auth_player_stats[assist2_id]['sweater'] = goal['assist2_sweater']
                auth_player_stats[assist2_id]['team'] = goal['team']
        
        # Count goals and assists for each player in GS data (only non-shootout goals count for stats)
        gs_player_stats = defaultdict(lambda: {'goals': 0, 'assists': 0, 'player_id': 0, 'name': '', 'sweater': 0, 'team': ''})
        
        for goal in html_gs_goals:
            # Only count non-shootout goals for player statistics
            if not goal.get('counts_for_stats', True):
                continue
                
            # Count goals using player ID as key
            scorer_id = goal.get('scorer_id')
            if scorer_id:
                gs_player_stats[scorer_id]['goals'] += 1
                gs_player_stats[scorer_id]['player_id'] = scorer_id
                gs_player_stats[scorer_id]['name'] = goal['scorer_name']
                gs_player_stats[scorer_id]['sweater'] = goal['scorer_sweater']
                gs_player_stats[scorer_id]['team'] = goal['team']
            
            # Count assists
            assist1_id = goal.get('assist1_id')
            if assist1_id:
                gs_player_stats[assist1_id]['assists'] += 1
                gs_player_stats[assist1_id]['player_id'] = assist1_id
                gs_player_stats[assist1_id]['name'] = goal['assist1_name']
                gs_player_stats[assist1_id]['sweater'] = goal['assist1_sweater']
                gs_player_stats[assist1_id]['team'] = goal['team']
            
            assist2_id = goal.get('assist2_id')
            if assist2_id:
                gs_player_stats[assist2_id]['assists'] += 1
                gs_player_stats[assist2_id]['player_id'] = assist2_id
                gs_player_stats[assist2_id]['name'] = goal['assist2_name']
                gs_player_stats[assist2_id]['sweater'] = goal['assist2_sweater']
                gs_player_stats[assist2_id]['team'] = goal['team']
        
        # Count goals and assists for each player in ES data (only non-shootout goals count for stats)
        es_player_stats = defaultdict(lambda: {'goals': 0, 'assists': 0, 'player_id': 0, 'name': '', 'sweater': 0, 'team': ''})
        
        for goal in html_es_goals:
            # Only count non-shootout goals for player statistics
            if not goal.get('counts_for_stats', True):
                continue
                
            # Count goals using player ID as key
            scorer_id = goal.get('scorer_id')
            if scorer_id:
                es_player_stats[scorer_id]['goals'] += 1
                es_player_stats[scorer_id]['player_id'] = scorer_id
                es_player_stats[scorer_id]['name'] = goal['scorer_name']
                es_player_stats[scorer_id]['sweater'] = goal['scorer_sweater']
                es_player_stats[scorer_id]['team'] = goal['team']
            
            # Count assists
            assist1_id = goal.get('assist1_id')
            if assist1_id:
                es_player_stats[assist1_id]['assists'] += 1
                es_player_stats[assist1_id]['player_id'] = assist1_id
                es_player_stats[assist1_id]['name'] = goal['assist1_name']
                es_player_stats[assist1_id]['sweater'] = goal['assist1_sweater']
                es_player_stats[assist1_id]['team'] = goal['team']
            
            assist2_id = goal.get('assist2_id')
            if assist2_id:
                es_player_stats[assist2_id]['assists'] += 1
                es_player_stats[assist2_id]['player_id'] = assist2_id
                es_player_stats[assist2_id]['name'] = goal['assist2_name']
                es_player_stats[assist2_id]['sweater'] = goal['assist2_sweater']
                es_player_stats[assist2_id]['team'] = goal['team']
        
        # Count goals and assists for each player in PL data (only non-shootout goals count for stats)
        pl_player_stats = defaultdict(lambda: {'goals': 0, 'assists': 0, 'player_id': 0, 'name': '', 'sweater': 0, 'team': ''})
        
        for goal in html_pl_goals:
            # Only count non-shootout goals for player statistics
            if not goal.get('counts_for_stats', True):
                continue
                
            # Count goals using player ID as key
            scorer_id = goal.get('scorer_id')
            if scorer_id:
                pl_player_stats[scorer_id]['goals'] += 1
                pl_player_stats[scorer_id]['player_id'] = scorer_id
                pl_player_stats[scorer_id]['name'] = goal['scorer_name']
                pl_player_stats[scorer_id]['sweater'] = goal['scorer_sweater']
                pl_player_stats[scorer_id]['team'] = goal['team']
            
            # Count assists
            assist1_id = goal.get('assist1_id')
            if assist1_id:
                pl_player_stats[assist1_id]['assists'] += 1
                pl_player_stats[assist1_id]['player_id'] = assist1_id
                pl_player_stats[assist1_id]['name'] = goal['assist1_name']
                pl_player_stats[assist1_id]['sweater'] = goal['assist1_sweater']
                pl_player_stats[assist1_id]['team'] = goal['team']
            
            assist2_id = goal.get('assist2_id')
            if assist2_id:
                pl_player_stats[assist2_id]['assists'] += 1
                pl_player_stats[assist2_id]['player_id'] = assist2_id
                pl_player_stats[assist2_id]['name'] = goal['assist2_name']
                pl_player_stats[assist2_id]['sweater'] = goal['assist2_sweater']
                pl_player_stats[assist2_id]['team'] = goal['team']
        
        # Compare player statistics using player IDs
        all_player_ids = set(list(auth_player_stats.keys()) + list(gs_player_stats.keys()) + 
                           list(es_player_stats.keys()) + list(pl_player_stats.keys()))
        
        for player_id in all_player_ids:
            auth_stats = auth_player_stats.get(player_id, {'goals': 0, 'assists': 0, 'player_id': 0, 'name': '', 'sweater': 0, 'team': ''})
            gs_stats = gs_player_stats.get(player_id, {'goals': 0, 'assists': 0, 'player_id': 0, 'name': '', 'sweater': 0, 'team': ''})
            es_stats = es_player_stats.get(player_id, {'goals': 0, 'assists': 0, 'player_id': 0, 'name': '', 'sweater': 0, 'team': ''})
            pl_stats = pl_player_stats.get(player_id, {'goals': 0, 'assists': 0, 'player_id': 0, 'name': '', 'sweater': 0, 'team': ''})
            
            gs_goal_discrepancy = auth_stats['goals'] - gs_stats['goals']
            gs_assist_discrepancy = auth_stats['assists'] - gs_stats['assists']
            es_goal_discrepancy = auth_stats['goals'] - es_stats['goals']
            es_assist_discrepancy = auth_stats['assists'] - es_stats['assists']
            pl_goal_discrepancy = auth_stats['goals'] - pl_stats['goals']
            pl_assist_discrepancy = auth_stats['assists'] - pl_stats['assists']
            
            # Determine reconciliation status based on all sources
            gs_total_discrepancy = abs(gs_goal_discrepancy) + abs(gs_assist_discrepancy)
            es_total_discrepancy = abs(es_goal_discrepancy) + abs(es_assist_discrepancy)
            pl_total_discrepancy = abs(pl_goal_discrepancy) + abs(pl_assist_discrepancy)
            
            # Primary reconciliation is based on GS (Game Summary) - the most reliable HTML source
            # ES and PL are used for additional validation but don't override GS results
            if gs_total_discrepancy == 0:
                status = 'perfect'
            elif gs_total_discrepancy <= 1:
                status = 'minor_discrepancy'
            else:
                status = 'major_discrepancy'
            
            # Use authoritative data for player info (more reliable)
            player_name = auth_stats['name'] or gs_stats['name'] or es_stats['name'] or pl_stats['name']
            sweater_number = auth_stats['sweater'] or gs_stats['sweater'] or es_stats['sweater'] or pl_stats['sweater']
            team = auth_stats['team'] or gs_stats['team'] or es_stats['team'] or pl_stats['team']
            
            player_result = PlayerReconciliationResult(
                player_id=player_id,
                player_name=player_name,
                sweater_number=sweater_number,
                team=team,
                authoritative_goals=auth_stats['goals'],
                authoritative_assists=auth_stats['assists'],
                html_goals=gs_stats['goals'],  # Keep for backward compatibility
                html_assists=gs_stats['assists'],  # Keep for backward compatibility
                goal_discrepancy=gs_goal_discrepancy,  # Keep for backward compatibility
                assist_discrepancy=gs_assist_discrepancy,  # Keep for backward compatibility
                reconciliation_status=status
            )
            
            player_results.append(player_result)
        
        return player_results
    
    def _identify_critical_discrepancies(self, team_results: Dict[str, TeamReconciliationResult], 
                                       player_results: List[PlayerReconciliationResult]) -> List[Dict[str, Any]]:
        """Identify critical discrepancies that require attention."""
        critical_discrepancies = []
        
        # Check for major team discrepancies
        for team, result in team_results.items():
            if result.reconciliation_status == 'major_discrepancy':
                critical_discrepancies.append({
                    'type': 'major_team_discrepancy',
                    'team': team,
                    'authoritative_goals': result.authoritative_total_goals,
                    'html_goals': result.html_total_goals,
                    'discrepancy': result.goal_count_discrepancy,
                    'message': f"Major team goal discrepancy: {team} has {result.goal_count_discrepancy} goal difference"
                })
        
        # Check for major player discrepancies
        for player in player_results:
            if player.reconciliation_status == 'major_discrepancy':
                critical_discrepancies.append({
                    'type': 'major_player_discrepancy',
                    'player': f"{player.player_name} #{player.sweater_number}",
                    'team': player.team,
                    'goal_discrepancy': player.goal_discrepancy,
                    'assist_discrepancy': player.assist_discrepancy,
                    'message': f"Major player discrepancy: {player.player_name} has {player.goal_discrepancy} goal and {player.assist_discrepancy} assist differences"
                })
        
        return critical_discrepancies
    
    def _generate_warnings(self, team_results: Dict[str, TeamReconciliationResult], 
                          player_results: List[PlayerReconciliationResult]) -> List[Dict[str, Any]]:
        """Generate warnings for minor discrepancies."""
        warnings = []
        
        # Check for minor team discrepancies
        for team, result in team_results.items():
            if result.reconciliation_status == 'minor_discrepancy':
                warnings.append({
                    'type': 'minor_team_discrepancy',
                    'team': team,
                    'discrepancy': result.goal_count_discrepancy,
                    'message': f"Minor team goal discrepancy: {team} has {result.goal_count_discrepancy} goal difference"
                })
        
        # Check for minor player discrepancies
        for player in player_results:
            if player.reconciliation_status == 'minor_discrepancy':
                warnings.append({
                    'type': 'minor_player_discrepancy',
                    'player': f"{player.player_name} #{player.sweater_number}",
                    'team': player.team,
                    'goal_discrepancy': player.goal_discrepancy,
                    'assist_discrepancy': player.assist_discrepancy,
                    'message': f"Minor player discrepancy: {player.player_name} has {player.goal_discrepancy} goal and {player.assist_discrepancy} assist differences"
                })
        
        return warnings
    
    def _load_player_mappings(self, game_id: str) -> Dict[int, Dict[str, Any]]:
        """Load player ID to name/sweater mappings from boxscore data."""
        boxscore_file = self.storage_path / 'json' / 'boxscores' / f'{game_id}.json'
        if not boxscore_file.exists():
            return {}
        
        with open(boxscore_file, 'r') as f:
            boxscore_data = json.load(f)
        
        player_mappings = {}
        
        # Extract from playerByGameStats
        player_stats = boxscore_data.get('playerByGameStats', {})
        
        # Get team abbreviations
        away_team_abbrev = boxscore_data.get('awayTeam', {}).get('abbrev', '')
        home_team_abbrev = boxscore_data.get('homeTeam', {}).get('abbrev', '')
        
        # Extract from away team
        away_team = player_stats.get('awayTeam', {})
        for position_group in ['forwards', 'defense', 'goalies']:
            for player in away_team.get(position_group, []):
                player_id = player.get('playerId')
                if player_id:
                    player_mappings[player_id] = {
                        'name': player.get('name', {}).get('default', ''),
                        'sweaterNumber': player.get('sweaterNumber', 0),
                        'team': away_team_abbrev
                    }
        
        # Extract from home team
        home_team = player_stats.get('homeTeam', {})
        for position_group in ['forwards', 'defense', 'goalies']:
            for player in home_team.get(position_group, []):
                player_id = player.get('playerId')
                if player_id:
                    player_mappings[player_id] = {
                        'name': player.get('name', {}).get('default', ''),
                        'sweaterNumber': player.get('sweaterNumber', 0),
                        'team': home_team_abbrev
                    }
        
        return player_mappings
    
    def _get_player_id_from_sweater(self, player_mappings: Dict[int, Dict[str, Any]], sweater_number: int, team: str) -> Optional[int]:
        """Get player ID from sweater number and team."""
        if not sweater_number:
            return None
        
        # Find player with matching sweater number and team
        for player_id, player_info in player_mappings.items():
            if (player_info.get('sweaterNumber') == sweater_number and 
                player_info.get('team') == team):
                return player_id
        
        return None
    
    def generate_detailed_report(self, result: GameReconciliationResult) -> str:
        """Generate a detailed reconciliation report for a game."""
        report = []
        report.append("=" * 80)
        report.append(f"PLAYER/TEAM GOAL RECONCILIATION REPORT - GAME {result.game_id}")
        report.append("=" * 80)
        report.append(f"Date: {result.game_date}")
        report.append(f"Teams: {result.away_team} @ {result.home_team}")
        report.append(f"Total Goals: {result.total_goals}")
        report.append(f"Overall Reconciliation: {result.overall_reconciliation_percentage:.1f}%")
        report.append("")
        report.append("NOTE: Shootout goals are excluded from player statistics and team totals")
        report.append("as they do not count toward official NHL statistics.")
        report.append("")
        
        # Team-level summary (three-way comparison)
        report.append("TEAM-LEVEL RECONCILIATION (Three-Way Comparison)")
        report.append("-" * 50)
        for team, team_result in result.team_results.items():
            report.append(f"{team}:")
            report.append(f"  Authoritative Goals: {team_result.authoritative_goals}")
            report.append(f"  HTML GS Goals: {team_result.html_gs_goals} (Δ{team_result.gs_discrepancy}) [{team_result.gs_reconciliation_status}]")
            report.append(f"  HTML ES Goals: {team_result.html_es_goals} (Δ{team_result.es_discrepancy}) [{team_result.es_reconciliation_status}]")
            report.append("")
        
        # Player-level summary
        report.append("PLAYER-LEVEL RECONCILIATION")
        report.append("-" * 40)
        
        # Group players by status
        perfect_players = [p for p in result.player_results if p.reconciliation_status == 'perfect']
        minor_discrepancies = [p for p in result.player_results if p.reconciliation_status == 'minor_discrepancy']
        major_discrepancies = [p for p in result.player_results if p.reconciliation_status == 'major_discrepancy']
        
        report.append(f"Perfect Reconciliations: {len(perfect_players)}")
        report.append(f"Minor Discrepancies: {len(minor_discrepancies)}")
        report.append(f"Major Discrepancies: {len(major_discrepancies)}")
        report.append("")
        
        # Show major discrepancies
        if major_discrepancies:
            report.append("MAJOR DISCREPANCIES:")
            for player in major_discrepancies:
                report.append(f"  {player.player_name} #{player.sweater_number} ({player.team}):")
                report.append(f"    Goals: Auth={player.authoritative_goals}, HTML GS={player.html_goals} (Δ{player.goal_discrepancy})")
                report.append(f"    Assists: Auth={player.authoritative_assists}, HTML GS={player.html_assists} (Δ{player.assist_discrepancy})")
            report.append("")
        
        # Show minor discrepancies
        if minor_discrepancies:
            report.append("MINOR DISCREPANCIES:")
            for player in minor_discrepancies:
                report.append(f"  {player.player_name} #{player.sweater_number} ({player.team}):")
                report.append(f"    Goals: Auth={player.authoritative_goals}, HTML GS={player.html_goals} (Δ{player.goal_discrepancy})")
                report.append(f"    Assists: Auth={player.authoritative_assists}, HTML GS={player.html_assists} (Δ{player.assist_discrepancy})")
            report.append("")
        
        # Critical issues
        if result.critical_discrepancies:
            report.append("CRITICAL ISSUES:")
            for issue in result.critical_discrepancies:
                report.append(f"  {issue['message']}")
            report.append("")
        
        # Warnings
        if result.warnings:
            report.append("WARNINGS:")
            for warning in result.warnings:
                report.append(f"  {warning['message']}")
            report.append("")
        
        return "\n".join(report)
    
    def save_reconciliation_results(self, season_summary: Dict[str, Any], output_file: str = None):
        """Save reconciliation results to file."""
        if output_file is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_file = f'player_team_goal_reconciliation_{timestamp}.json'
        
        # Convert dataclasses to dictionaries for proper JSON serialization
        def convert_dataclass_to_dict(obj):
            if hasattr(obj, '__dict__'):
                return {k: convert_dataclass_to_dict(v) for k, v in obj.__dict__.items()}
            elif isinstance(obj, list):
                return [convert_dataclass_to_dict(item) for item in obj]
            elif isinstance(obj, dict):
                return {k: convert_dataclass_to_dict(v) for k, v in obj.items()}
            else:
                return obj
        
        # Convert the reconciliation results
        converted_summary = convert_dataclass_to_dict(season_summary)
        
        with open(output_file, 'w') as f:
            json.dump(converted_summary, f, indent=2)
        
        logger.info(f"Reconciliation results saved to {output_file}")
        
        # Also save a summary report
        report_file = output_file.replace('.json', '_report.txt')
        report = self.generate_season_summary_report(season_summary)
        with open(report_file, 'w') as f:
            f.write(report)
        
        logger.info(f"Reconciliation report saved to {report_file}")
    
    def generate_season_summary_report(self, season_summary: Dict[str, Any]) -> str:
        """Generate a season summary report."""
        report = []
        report.append("=" * 80)
        report.append("NHL PLAYER/TEAM GOAL RECONCILIATION - SEASON SUMMARY")
        report.append("=" * 80)
        report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append(f"Season: 2024-2025")
        report.append("")
        
        # Executive Summary
        report.append("EXECUTIVE SUMMARY")
        report.append("-" * 40)
        report.append(f"Total Games: {season_summary['total_games']}")
        report.append(f"Reconciled Games: {season_summary['reconciled_games']}")
        report.append(f"Failed Games: {season_summary['failed_games']}")
        report.append(f"Total Goals: {season_summary['total_goals']}")
        report.append(f"Total Players Analyzed: {season_summary['total_players_analyzed']}")
        report.append(f"Overall Reconciliation: {season_summary.get('overall_reconciliation_percentage', 0):.1f}%")
        report.append("")
        
        # Reconciliation Breakdown
        report.append("RECONCILIATION BREAKDOWN")
        report.append("-" * 40)
        report.append(f"Perfect Reconciliations: {season_summary['perfect_reconciliations']}")
        report.append(f"Minor Discrepancies: {season_summary['minor_discrepancies']}")
        report.append(f"Major Discrepancies: {season_summary['major_discrepancies']}")
        report.append("")
        
        # Critical Issues Summary
        if season_summary['critical_issues']:
            report.append("CRITICAL ISSUES SUMMARY")
            report.append("-" * 40)
            issue_types = defaultdict(int)
            for issue in season_summary['critical_issues']:
                issue_types[issue['type']] += 1
            
            for issue_type, count in issue_types.items():
                report.append(f"{issue_type}: {count} occurrences")
            report.append("")
        
        # Recommendations
        report.append("RECOMMENDATIONS")
        report.append("-" * 40)
        report.append("1. Use Play-by-Play JSON as authoritative source for all goal data")
        report.append("2. HTML reports show high accuracy for goal counting")
        report.append("3. Minor discrepancies are typically due to formatting differences")
        report.append("4. Major discrepancies require manual review and correction")
        report.append("5. Implement automated validation in the curation pipeline")
        report.append("")
        
        return "\n".join(report)

def main():
    parser = argparse.ArgumentParser(description='Player/Team Goal Data Reconciliation System')
    parser.add_argument('--game-id', help='Reconcile specific game ID')
    parser.add_argument('--all-games', action='store_true', help='Reconcile all games in season')
    parser.add_argument('--team-analysis', action='store_true', help='Focus on team-level analysis')
    parser.add_argument('--storage-path', default='storage/20242025', help='Storage path')
    parser.add_argument('--output', help='Output file for results')
    parser.add_argument('--verbose', action='store_true', help='Show detailed game-by-game output during processing')
    
    args = parser.parse_args()
    
    system = PlayerTeamGoalReconciliation(args.storage_path)
    
    if args.game_id:
        # Reconcile specific game
        system.verbose = args.verbose
        system.output_file = args.output
        result = system.reconcile_game(args.game_id)
        if result:
            if args.verbose:
                # Show new readable format
                system._display_game_result(result, 1, 1)
            else:
                # Show detailed report format
                print(system.generate_detailed_report(result))
            
            # Save individual game report
            if args.output:
                with open(args.output, 'w') as f:
                    f.write(system.generate_detailed_report(result))
                print(f"\nReport saved to {args.output}")
        else:
            print(f"Failed to reconcile game {args.game_id}")
    
    elif args.all_games:
        # Reconcile all games
        season_summary = system.reconcile_all_games(verbose=args.verbose, output_file=args.output)
        system.save_reconciliation_results(season_summary, args.output)
        
        print(f"Reconciliation complete: {season_summary['reconciled_games']}/{season_summary['total_games']} games")
        print(f"Overall reconciliation: {season_summary.get('overall_reconciliation_percentage', 0):.1f}%")
        print(f"Total goals: {season_summary['total_goals']}")
        print(f"Perfect reconciliations: {season_summary['perfect_reconciliations']}")
        print(f"Major discrepancies: {season_summary['major_discrepancies']}")
    
    else:
        parser.print_help()

if __name__ == '__main__':
    main()
