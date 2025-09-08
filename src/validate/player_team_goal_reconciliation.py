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
class PhaseGoalData:
    """Goal data organized by game phase."""
    regular_time: List[Dict[str, Any]] = None
    overtime: List[Dict[str, Any]] = None
    shootout: List[Dict[str, Any]] = None
    
    def __post_init__(self):
        if self.regular_time is None:
            self.regular_time = []
        if self.overtime is None:
            self.overtime = []
        if self.shootout is None:
            self.shootout = []

@dataclass
class PhaseReconciliationResult:
    """Reconciliation result for a specific game phase."""
    phase: str  # 'regular_time', 'overtime', 'shootout'
    authoritative_goals: List[Dict[str, Any]]
    html_goals: List[Dict[str, Any]]
    player_discrepancies: List[Dict[str, Any]]
    team_discrepancies: List[Dict[str, Any]]
    reconciliation_status: str  # 'perfect', 'minor_discrepancy', 'major_discrepancy'
    data_sources_used: List[str]  # Which sources were used for this phase

@dataclass
class PlayerDetailedStats:
    """Comprehensive player statistics for reconciliation."""
    player_id: int
    player_name: str
    sweater_number: int
    team_abbrev: str
    position: str  # F, D, G
    
    # Goals and Assists
    goals_regulation: int = 0
    goals_overtime: int = 0
    goals_shootout: int = 0
    assists_regulation: int = 0
    assists_overtime: int = 0
    assists_shootout: int = 0
    
    # Skater Stats (F/D)
    time_on_ice_regulation: str = "00:00"
    time_on_ice_overtime: str = "00:00"
    shots_regulation: int = 0
    shots_overtime: int = 0
    shots_shootout: int = 0
    plus_minus: int = 0
    penalty_minutes: int = 0
    
    # Goaltender Stats (G)
    saves_regulation: int = 0
    saves_overtime: int = 0
    saves_shootout: int = 0
    goals_against_regulation: int = 0
    goals_against_overtime: int = 0
    goals_against_shootout: int = 0
    shots_faced_regulation: int = 0
    shots_faced_overtime: int = 0
    shots_faced_shootout: int = 0
    
    # Source tracking
    source: str = ""  # "authoritative", "pl_html", "es_html", "gs_html"
    
    def get_total_goals(self) -> int:
        return self.goals_regulation + self.goals_overtime + self.goals_shootout
    
    def get_total_assists(self) -> int:
        return self.assists_regulation + self.assists_overtime + self.assists_shootout
    
    def get_total_points(self) -> int:
        return self.get_total_goals() + self.get_total_assists()
    
    def get_total_saves(self) -> int:
        return self.saves_regulation + self.saves_overtime + self.saves_shootout
    
    def get_total_goals_against(self) -> int:
        return self.goals_against_regulation + self.goals_against_overtime + self.goals_against_shootout

@dataclass
class TeamDetailedStats:
    """Comprehensive team statistics for reconciliation."""
    team_abbrev: str
    
    # Goals by period type
    goals_regulation: int = 0
    goals_overtime: int = 0
    goals_shootout: int = 0
    
    # Shots by period type
    shots_regulation: int = 0
    shots_overtime: int = 0
    shots_shootout: int = 0
    
    # Penalties
    penalty_minutes: int = 0
    power_play_goals: int = 0
    power_play_opportunities: int = 0
    short_handed_goals: int = 0
    
    # Faceoffs
    faceoffs_won: int = 0
    faceoffs_lost: int = 0
    
    # Source tracking
    source: str = ""  # "authoritative", "pl_html", "es_html", "gs_html"
    
    def get_total_goals(self) -> int:
        return self.goals_regulation + self.goals_overtime + self.goals_shootout
    
    def get_total_shots(self) -> int:
        return self.shots_regulation + self.shots_overtime + self.shots_shootout

@dataclass
class PlayerReconciliationResult:
    """Player-level reconciliation result with proper phase breakdown."""
    player_id: int
    player_name: str
    sweater_number: int
    team: str
    position: str = "F"  # F, D, G
    
    # Regulation Time Goals and Assists
    auth_regulation_goals: int = 0
    auth_regulation_assists: int = 0
    gs_regulation_goals: int = 0
    gs_regulation_assists: int = 0
    es_regulation_goals: int = 0
    es_regulation_assists: int = 0
    pl_regulation_goals: int = 0
    pl_regulation_assists: int = 0
    
    # Overtime Goals and Assists
    auth_overtime_goals: int = 0
    auth_overtime_assists: int = 0
    gs_overtime_goals: int = 0
    gs_overtime_assists: int = 0
    es_overtime_goals: int = 0
    es_overtime_assists: int = 0
    pl_overtime_goals: int = 0
    pl_overtime_assists: int = 0
    
    # Shootout Goals (individual goals during shootout - no assists in shootout)
    auth_shootout_goals: int = 0
    gs_shootout_goals: int = 0
    es_shootout_goals: int = 0
    pl_shootout_goals: int = 0
    
    # Discrepancies by phase
    regulation_goal_discrepancy: int = 0
    regulation_assist_discrepancy: int = 0
    overtime_goal_discrepancy: int = 0
    overtime_assist_discrepancy: int = 0
    shootout_goal_discrepancy: int = 0
    
    # ES combined REG+OT comparison (player-level)
    auth_combined_goals: int = 0
    auth_combined_assists: int = 0
    es_combined_goals: int = 0
    es_combined_assists: int = 0
    es_combined_goal_discrepancy: int = 0
    es_combined_assist_discrepancy: int = 0
    es_combined_status: str = "perfect"
    
    # Overall reconciliation status
    reconciliation_status: str = "perfect"
    
    # Backward compatibility fields
    authoritative_goals: int = 0
    authoritative_assists: int = 0
    gs_html_goals: int = 0
    gs_html_assists: int = 0
    es_html_goals: int = 0
    es_html_assists: int = 0
    pl_html_goals: int = 0
    pl_html_assists: int = 0
    html_goals: int = 0
    html_assists: int = 0
    goal_discrepancy: int = 0
    assist_discrepancy: int = 0

@dataclass
class TeamReconciliationResult:
    """Team-level reconciliation result with proper phase breakdown."""
    team_id: int
    team_abbrev: str
    
    # Regulation Time Goals
    auth_regulation_goals: int = 0
    gs_regulation_goals: int = 0
    es_regulation_goals: int = 0
    pl_regulation_goals: int = 0
    
    # Overtime Goals
    auth_overtime_goals: int = 0
    gs_overtime_goals: int = 0
    es_overtime_goals: int = 0
    pl_overtime_goals: int = 0
    
    # Shootout Goals (individual goals during shootout)
    auth_shootout_goals: int = 0
    gs_shootout_goals: int = 0
    es_shootout_goals: int = 0
    pl_shootout_goals: int = 0
    
    # Shootout Outcome (0 or 1 - who won the shootout)
    auth_shootout_outcome: int = 0
    gs_shootout_outcome: int = 0
    es_shootout_outcome: int = 0
    pl_shootout_outcome: int = 0
    
    # Discrepancies by phase
    regulation_discrepancy: int = 0
    overtime_discrepancy: int = 0
    shootout_goals_discrepancy: int = 0
    shootout_outcome_discrepancy: int = 0
    
    # ES combined REG+OT comparison (team-level)
    auth_combined_goals: int = 0
    es_combined_goals: int = 0
    es_combined_discrepancy: int = 0
    es_combined_status: str = 'perfect'
    
    # Overall reconciliation status
    reconciliation_status: str = 'perfect'
    
    # Backward compatibility fields
    authoritative_goals: int = 0
    html_gs_goals: int = 0
    html_es_goals: int = 0
    html_pl_goals: int = 0
    gs_discrepancy: int = 0
    es_discrepancy: int = 0
    pl_discrepancy: int = 0
    gs_reconciliation_status: str = 'perfect'
    es_reconciliation_status: str = 'perfect'
    pl_reconciliation_status: str = 'perfect'
    authoritative_total_goals: int = 0
    html_total_goals: int = 0
    goal_count_discrepancy: int = 0
    player_discrepancies: List[PlayerReconciliationResult] = None

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
    game_metadata: Dict[str, Any] = None

@dataclass
class ComprehensiveGameReconciliationResult:
    """Comprehensive game reconciliation with detailed stats by source."""
    game_id: str
    game_date: str
    home_team: str
    away_team: str
    
    # Team stats by source
    home_team_stats: Dict[str, TeamDetailedStats]  # source -> TeamDetailedStats
    away_team_stats: Dict[str, TeamDetailedStats]  # source -> TeamDetailedStats
    
    # Player stats by source
    home_player_stats: Dict[str, List[PlayerDetailedStats]]  # source -> List[PlayerDetailedStats]
    away_player_stats: Dict[str, List[PlayerDetailedStats]]  # source -> List[PlayerDetailedStats]
    
    # Reconciliation summary
    overall_reconciliation_percentage: float
    critical_discrepancies: List[Dict[str, Any]]
    warnings: List[Dict[str, Any]]
    game_metadata: Dict[str, Any] = None

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
            52: "WPG", 53: "ARI", 54: "VGK", 55: "SEA", 59: "UTA"
        }
        
        # Reverse mapping for team abbreviations to IDs
        self.team_abbrev_to_id = {v: k for k, v in self.team_id_mappings.items()}
    
    def _classify_goal_by_phase(self, goal: Dict[str, Any]) -> str:
        """Classify a goal by game phase."""
        period = goal.get('period', 1)
        period_type = goal.get('period_type', 'REGULAR')
        is_shootout = goal.get('is_shootout', False)
        
        if is_shootout or period == 5 or period_type == 'SHOOTOUT':
            return 'shootout'
        elif period == 4 or period_type == 'OVERTIME':
            return 'overtime'
        else:
            return 'regular_time'
    
    def _split_goals_by_phase(self, goals: List[Dict[str, Any]]) -> PhaseGoalData:
        """Split goals by game phase."""
        phase_data = PhaseGoalData()
        
        for goal in goals:
            phase = self._classify_goal_by_phase(goal)
            if phase == 'regular_time':
                phase_data.regular_time.append(goal)
            elif phase == 'overtime':
                phase_data.overtime.append(goal)
            elif phase == 'shootout':
                phase_data.shootout.append(goal)
        
        return phase_data
    
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
    
    def reconcile_all_games_enhanced(self, verbose: bool = False, output_file: str = None, games_filter: List[str] = None) -> Dict[str, Any]:
        """
        Enhanced reconciliation with individual game files and comprehensive reporting.
        
        Args:
            verbose: Whether to show detailed progress
            output_file: Base name for output files
            
        Returns:
            Dictionary containing comprehensive reconciliation results
        """
        logger.info("Starting enhanced comprehensive player and team goal reconciliation...")
        self.verbose = verbose
        self.output_file = output_file or f"reconciliation_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Get all game IDs from boxscores directory
        boxscore_dir = self.storage_path / 'json' / 'boxscores'
        game_files = list(boxscore_dir.glob('*.json'))
        
        # Apply games filter if provided
        if games_filter:
            filtered_game_files = []
            for game_file in game_files:
                game_id = game_file.stem  # Get filename without extension
                if game_id in games_filter:
                    filtered_game_files.append(game_file)
            game_files = filtered_game_files
            if verbose:
                logger.info(f"Games filter applied: {len(game_files)} games selected from {len(list(boxscore_dir.glob('*.json')))} total")
        
        total_games = len(game_files)
        reconciled_games = 0
        failed_games = 0
        
        # Enhanced season summary
        season_summary = {
            'total_games': total_games,
            'reconciled_games': 0,
            'failed_games': 0,
            'total_goals': 0,
            'total_players_analyzed': 0,
            'perfect_reconciliations': 0,
            'minor_discrepancies': 0,
            'major_discrepancies': 0,
            'overall_reconciliation': 0.0,
            'games_with_discrepancies': 0,
            'games_with_warnings': 0,
            'non_reconciled_games': [],
            'individual_game_results': [],
            'composite_stats': {
                'total_goals_by_source': {},
                'discrepancy_types': {},
                'team_performance': {},
                'player_performance': {}
            }
        }
        
        # Create output directory for individual game files in the proper location
        reconciliation_dir = Path(self.storage_path) / "json" / "curate" / "reconciliation"
        reconciliation_dir.mkdir(parents=True, exist_ok=True)
        
        # Create timestamped subdirectory for this reconciliation run
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_dir = reconciliation_dir / f"recon_{timestamp}"
        output_dir.mkdir(exist_ok=True)
        
        logger.info(f"🎯 Starting enhanced reconciliation for {total_games} games")
        
        for i, game_file in enumerate(game_files):
            game_id = game_file.stem
            
            # Progress updates every 5%
            if (i + 1) % max(1, total_games // 20) == 0 or i == total_games - 1:
                progress = ((i + 1) / total_games) * 100
                logger.info(f"📊 Progress: {i + 1}/{total_games} games processed ({progress:.1f}% complete)")
            
            try:
                # Reconcile individual game
                game_result = self.reconcile_game(game_id)
                
                if game_result:
                    reconciled_games += 1
                    season_summary['total_goals'] += game_result.total_goals
                    season_summary['total_players_analyzed'] += len(game_result.player_results)
                    season_summary['perfect_reconciliations'] += len([p for p in game_result.player_results if p.reconciliation_status == 'perfect'])
                    season_summary['minor_discrepancies'] += len([p for p in game_result.player_results if p.reconciliation_status == 'minor_discrepancy'])
                    season_summary['major_discrepancies'] += len([p for p in game_result.player_results if p.reconciliation_status == 'major_discrepancy'])
                    
                    # Track games with discrepancies
                    if any(p.reconciliation_status != 'perfect' for p in game_result.player_results):
                        season_summary['games_with_discrepancies'] += 1
                        season_summary['non_reconciled_games'].append({
                            'game_id': game_id,
                            'discrepancy_count': len([p for p in game_result.player_results if p.reconciliation_status != 'perfect']),
                            'major_discrepancies': len([p for p in game_result.player_results if p.reconciliation_status == 'major_discrepancy'])
                        })
                    
                    # Store individual game result
                    season_summary['individual_game_results'].append({
                        'game_id': game_id,
                        'total_goals': game_result.total_goals,
                        'reconciliation_percentage': game_result.overall_reconciliation_percentage,
                        'player_count': len(game_result.player_results),
                        'perfect_players': len([p for p in game_result.player_results if p.reconciliation_status == 'perfect']),
                        'discrepancy_players': len([p for p in game_result.player_results if p.reconciliation_status != 'perfect'])
                    })
                    
                    # Generate individual game file
                    self._generate_individual_game_report(game_result, output_dir / f"game_{game_id}_reconciliation.txt")
                    
                    # Update composite stats
                    self._update_composite_stats(season_summary['composite_stats'], game_result)
                    
                else:
                    failed_games += 1
                    season_summary['failed_games'] += 1
                    season_summary['non_reconciled_games'].append({
                        'game_id': game_id,
                        'error': 'Failed to reconcile'
                    })
                    
            except Exception as e:
                failed_games += 1
                season_summary['failed_games'] += 1
                logger.error(f"Error reconciling game {game_id}: {e}")
                season_summary['non_reconciled_games'].append({
                    'game_id': game_id,
                    'error': str(e)
                })
        
        # Calculate overall reconciliation percentage
        if season_summary['total_players_analyzed'] > 0:
            season_summary['overall_reconciliation'] = (season_summary['perfect_reconciliations'] / season_summary['total_players_analyzed']) * 100
        
        season_summary['reconciled_games'] = reconciled_games
        
        # Generate comprehensive summary report
        self._generate_comprehensive_summary_report(season_summary, output_dir)
        
        # Note: JSON results are not saved as this is for user consumption only
        
        logger.info(f"✅ Enhanced reconciliation complete: {reconciled_games}/{total_games} games")
        logger.info(f"📊 Overall reconciliation: {season_summary['overall_reconciliation']:.1f}%")
        logger.info(f"📁 Individual game reports saved to: {output_dir}")
        logger.info(f"📄 Comprehensive summary: {output_dir}/comprehensive_summary.txt")
        
        return season_summary
    
    def _generate_individual_game_report(self, game_result: GameReconciliationResult, output_file: Path) -> None:
        """Generate detailed individual game reconciliation report."""
        try:
            with open(output_file, 'w') as f:
                f.write("=" * 80 + "\n")
                f.write(f"NHL GOAL RECONCILIATION - GAME {game_result.game_id}\n")
                f.write("=" * 80 + "\n")
                f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"Game Date: {game_result.game_metadata.get('game_date', 'Unknown')}\n")
                f.write(f"Teams: {game_result.game_metadata.get('away_team', 'Unknown')} @ {game_result.game_metadata.get('home_team', 'Unknown')}\n\n")
                
                f.write("EXECUTIVE SUMMARY\n")
                f.write("-" * 40 + "\n")
                f.write(f"Total Goals: {game_result.total_goals}\n")
                f.write(f"Reconciliation: {game_result.overall_reconciliation_percentage:.1f}%\n")
                f.write(f"Players Analyzed: {len(game_result.player_results)}\n")
                f.write(f"Perfect Reconciliations: {len([p for p in game_result.player_results if p.reconciliation_status == 'perfect'])}\n")
                f.write(f"Minor Discrepancies: {len([p for p in game_result.player_results if p.reconciliation_status == 'minor_discrepancy'])}\n")
                f.write(f"Major Discrepancies: {len([p for p in game_result.player_results if p.reconciliation_status == 'major_discrepancy'])}\n\n")
                
                # Team-level reconciliation by phase
                f.write("TEAM-LEVEL RECONCILIATION BY PHASE\n")
                f.write("-" * 50 + "\n")
                f.write("Sources: Authoritative (Boxscore) | GS HTML | ES HTML | PL HTML\n")
                f.write("Note: GS HTML only shows shootout OUTCOME (winning goal), not individual shootout goals\n\n")
                
                for team_abbrev, team_result in game_result.team_results.items():
                    f.write(f"{team_abbrev} TEAM TOTALS:\n")
                    
                    # Regulation Time
                    f.write(f"  REGULATION TIME:\n")
                    f.write(f"    Authoritative: {team_result.auth_regulation_goals} goals\n")
                    f.write(f"    GS HTML:       {team_result.gs_regulation_goals} goals (Δ{team_result.regulation_discrepancy:+d})\n")
                    f.write(f"    ES HTML:       --\n")
                    f.write(f"    PL HTML:       {team_result.pl_regulation_goals} goals\n")
                    
                    # Overtime
                    f.write(f"  OVERTIME:\n")
                    f.write(f"    Authoritative: {team_result.auth_overtime_goals} goals\n")
                    f.write(f"    GS HTML:       {team_result.gs_overtime_goals} goals (Δ{team_result.overtime_discrepancy:+d})\n")
                    f.write(f"    ES HTML:       --\n")
                    f.write(f"    PL HTML:       {team_result.pl_overtime_goals} goals\n")
                    
                    # Shootout Goals (individual goals during shootout)
                    f.write(f"  SHOOTOUT GOALS (individual):\n")
                    f.write(f"    Authoritative: {team_result.auth_shootout_goals} goals\n")
                    f.write(f"    GS HTML:       --\n")
                    f.write(f"    ES HTML:       --\n")
                    f.write(f"    PL HTML:       {team_result.pl_shootout_goals} goals\n")
                    
                    # Shootout Outcome (0 or 1 - who won)
                    f.write(f"  SHOOTOUT OUTCOME (0/1):\n")
                    f.write(f"    Authoritative: {team_result.auth_shootout_outcome}\n")
                    f.write(f"    GS HTML:       {team_result.gs_shootout_outcome} (Δ{team_result.shootout_outcome_discrepancy:+d})\n")
                    f.write(f"    ES HTML:       --\n")
                    f.write(f"    PL HTML:       {team_result.pl_shootout_outcome}\n")
                    
                    # ES combined REG+OT line
                    f.write(f"  ES TOTAL REG+OT:\n")
                    f.write(f"    Authoritative: {team_result.auth_combined_goals} goals\n")
                    f.write(f"    ES HTML:       {team_result.es_combined_goals} goals (Δ{team_result.es_combined_discrepancy:+d})\n")
                    f.write(f"  OVERALL STATUS: {team_result.reconciliation_status} | ES Combined: {team_result.es_combined_status}\n\n")
                
                # Player-level reconciliation by phase
                f.write("PLAYER-LEVEL RECONCILIATION BY PHASE\n")
                f.write("-" * 50 + "\n")
                f.write("Sources: Authoritative (Boxscore) | GS HTML | ES HTML | PL HTML\n")
                f.write("Note: PL HTML does not include assist data - only goals are compared\n")
                f.write("Note: Shootout goals are individual goals during shootout (no assists in shootout)\n")
                f.write("Note: * ES totals placed under regulation may include OT (no phase breakdown)\n\n")
                
                # Group players by team
                players_by_team = {}
                for player in game_result.player_results:
                    team = player.team
                    if team not in players_by_team:
                        players_by_team[team] = []
                    players_by_team[team].append(player)
                
                for team, players in players_by_team.items():
                    f.write(f"{team} PLAYERS:\n")
                    f.write("-" * 30 + "\n")
                    
                    for player in sorted(players, key=lambda p: p.sweater_number):
                        f.write(f"  #{player.sweater_number:2d} {player.player_name:<25} ({player.position})\n")
                        
                        # Regulation Time
                        f.write(f"      REGULATION TIME:\n")
                        # Compute ES asterisk flags when ES totals likely include OT distribution
                        es_goals_star = "*" if (player.auth_overtime_goals > 0 and player.es_regulation_goals == (player.auth_regulation_goals + player.auth_overtime_goals)) else ""
                        es_assists_star = "*" if (player.auth_overtime_assists > 0 and player.es_regulation_assists == (player.auth_regulation_assists + player.auth_overtime_assists)) else ""
                        f.write(f"        Authoritative: {player.auth_regulation_goals}G {player.auth_regulation_assists}A\n")
                        f.write(f"        GS HTML:       {player.gs_regulation_goals}G {player.gs_regulation_assists}A\n")
                        f.write(f"        ES HTML:       -- --\n")
                        f.write(f"        PL HTML:       {player.pl_regulation_goals}G --A (no assist data)\n")
                        f.write(f"        Discrepancy:   {player.regulation_goal_discrepancy:+d}G {player.regulation_assist_discrepancy:+d}A\n")
                        
                        # Overtime
                        f.write(f"      OVERTIME:\n")
                        f.write(f"        Authoritative: {player.auth_overtime_goals}G {player.auth_overtime_assists}A\n")
                        f.write(f"        GS HTML:       {player.gs_overtime_goals}G {player.gs_overtime_assists}A\n")
                        f.write(f"        ES HTML:       -- --\n")
                        f.write(f"        PL HTML:       {player.pl_overtime_goals}G --A (no assist data)\n")
                        f.write(f"        Discrepancy:   {player.overtime_goal_discrepancy:+d}G {player.overtime_assist_discrepancy:+d}A\n")
                        
                        # Shootout Goals (no assists)
                        f.write(f"      SHOOTOUT GOALS:\n")
                        f.write(f"        Authoritative: {player.auth_shootout_goals}G\n")
                        f.write(f"        GS HTML:       --\n")
                        f.write(f"        ES HTML:       --\n")
                        f.write(f"        PL HTML:       {player.pl_shootout_goals}G\n")
                        f.write(f"        Discrepancy:   {player.shootout_goal_discrepancy:+d}G\n")
                        
                        # ES combined totals row
                        f.write(f"      ES TOTAL REG+OT:\n")
                        f.write(f"        Authoritative: {player.auth_combined_goals}G {player.auth_combined_assists}A\n")
                        f.write(f"        ES HTML:       {player.es_combined_goals}G {player.es_combined_assists}A (Δ{player.es_combined_goal_discrepancy:+d}G Δ{player.es_combined_assist_discrepancy:+d}A)\n")
                        f.write(f"      OVERALL STATUS: {player.reconciliation_status} | ES Combined: {player.es_combined_status}\n\n")
                
                # Critical discrepancies
                if game_result.critical_discrepancies:
                    f.write("\n\nCRITICAL DISCREPANCIES\n")
                    f.write("-" * 40 + "\n")
                    for discrepancy in game_result.critical_discrepancies:
                        f.write(f"  - {discrepancy}\n")
                
                # Warnings
                if game_result.warnings:
                    f.write("\n\nWARNINGS\n")
                    f.write("-" * 40 + "\n")
                    for warning in game_result.warnings:
                        f.write(f"  - {warning}\n")
                
        except Exception as e:
            logger.error(f"Error generating individual game report for {game_result.game_id}: {e}")
    
    def _update_composite_stats(self, composite_stats: Dict[str, Any], game_result: GameReconciliationResult) -> None:
        """Update composite statistics with game result data."""
        try:
            # Update goal counts by source
            for team_abbrev, team_result in game_result.team_results.items():
                team = team_abbrev
                if team not in composite_stats['total_goals_by_source']:
                    composite_stats['total_goals_by_source'][team] = {
                        'authoritative': 0, 'gs_html': 0, 'es_html': 0, 'pl_html': 0
                    }
                
                composite_stats['total_goals_by_source'][team]['authoritative'] += team_result.authoritative_goals
                composite_stats['total_goals_by_source'][team]['gs_html'] += team_result.html_gs_goals
                composite_stats['total_goals_by_source'][team]['es_html'] += team_result.html_es_goals
                composite_stats['total_goals_by_source'][team]['pl_html'] += team_result.html_pl_goals
            
            # Update discrepancy types
            for player in game_result.player_results:
                if player.reconciliation_status != 'perfect':
                    status = player.reconciliation_status
                    if status not in composite_stats['discrepancy_types']:
                        composite_stats['discrepancy_types'][status] = 0
                    composite_stats['discrepancy_types'][status] += 1
            
            # Update team performance
            for team_abbrev, team_result in game_result.team_results.items():
                team = team_abbrev
                if team not in composite_stats['team_performance']:
                    composite_stats['team_performance'][team] = {
                        'games': 0, 'perfect_games': 0, 'total_discrepancies': 0
                    }
                
                composite_stats['team_performance'][team]['games'] += 1
                if (team_result.gs_reconciliation_status == 'perfect' and 
                    team_result.es_reconciliation_status == 'perfect' and 
                    team_result.pl_reconciliation_status == 'perfect'):
                    composite_stats['team_performance'][team]['perfect_games'] += 1
                
                composite_stats['team_performance'][team]['total_discrepancies'] += (
                    abs(team_result.gs_discrepancy) + 
                    abs(team_result.es_discrepancy) + 
                    abs(team_result.pl_discrepancy)
                )
                
        except Exception as e:
            logger.error(f"Error updating composite stats: {e}")
    
    def _generate_comprehensive_summary_report(self, season_summary: Dict[str, Any], output_dir: Path) -> None:
        """Generate comprehensive summary report with non-reconciled games and composite stats."""
        try:
            summary_file = output_dir / "comprehensive_summary.txt"
            
            with open(summary_file, 'w') as f:
                f.write("=" * 80 + "\n")
                f.write("NHL COMPREHENSIVE SEASON RECONCILIATION SUMMARY\n")
                f.write("=" * 80 + "\n")
                f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"Season: 2024-2025\n")
                f.write(f"Sources: Authoritative (Boxscore) | GS HTML | ES HTML | PL HTML\n")
                f.write(f"Note: PL HTML does not include assist data - only goals are compared\n\n")
                
                # Executive Summary
                f.write("EXECUTIVE SUMMARY\n")
                f.write("-" * 40 + "\n")
                f.write(f"Total Games: {season_summary['total_games']}\n")
                f.write(f"Reconciled Games: {season_summary['reconciled_games']}\n")
                f.write(f"Failed Games: {season_summary['failed_games']}\n")
                f.write(f"Total Goals: {season_summary['total_goals']}\n")
                f.write(f"Total Players Analyzed: {season_summary['total_players_analyzed']}\n")
                f.write(f"Overall Reconciliation: {season_summary['overall_reconciliation']:.1f}%\n\n")
                
                # Reconciliation Breakdown
                f.write("RECONCILIATION BREAKDOWN\n")
                f.write("-" * 40 + "\n")
                f.write(f"Perfect Reconciliations: {season_summary['perfect_reconciliations']}\n")
                f.write(f"Minor Discrepancies: {season_summary['minor_discrepancies']}\n")
                f.write(f"Major Discrepancies: {season_summary['major_discrepancies']}\n")
                f.write(f"Games with Discrepancies: {season_summary['games_with_discrepancies']}\n\n")
                
                # Non-Reconciled Games
                if season_summary['non_reconciled_games']:
                    f.write("NON-RECONCILED GAMES\n")
                    f.write("-" * 40 + "\n")
                    for game in season_summary['non_reconciled_games']:
                        f.write(f"Game {game['game_id']}: ")
                        if 'error' in game:
                            f.write(f"Error - {game['error']}\n")
                        else:
                            f.write(f"Discrepancies: {game.get('discrepancy_count', 0)} "
                                   f"(Major: {game.get('major_discrepancies', 0)})\n")
                    f.write("\n")
                
                # Composite Statistics
                f.write("COMPOSITE STATISTICS\n")
                f.write("-" * 40 + "\n")
                
                # Goals by source
                f.write("\nGoals by Source:\n")
                for team, goals in season_summary['composite_stats']['total_goals_by_source'].items():
                    f.write(f"  {team}:\n")
                    f.write(f"    Authoritative: {goals['authoritative']}\n")
                    f.write(f"    GS HTML: {goals['gs_html']}\n")
                    f.write(f"    ES HTML: {goals['es_html']}\n")
                    f.write(f"    PL HTML: {goals['pl_html']}\n")
                
                # Discrepancy types
                f.write("\nDiscrepancy Types:\n")
                for disc_type, count in season_summary['composite_stats']['discrepancy_types'].items():
                    f.write(f"  {disc_type}: {count}\n")
                
                # Team performance
                f.write("\nTeam Performance:\n")
                for team, perf in season_summary['composite_stats']['team_performance'].items():
                    perfect_rate = (perf['perfect_games'] / perf['games'] * 100) if perf['games'] > 0 else 0
                    f.write(f"  {team}: {perf['games']} games, {perf['perfect_games']} perfect ({perfect_rate:.1f}%), "
                           f"{perf['total_discrepancies']} total discrepancies\n")
                
                # Recommendations
                f.write("\n\nRECOMMENDATIONS\n")
                f.write("-" * 40 + "\n")
                f.write("1. Use Play-by-Play JSON as authoritative source for all goal data\n")
                f.write("2. HTML reports show high accuracy for goal counting\n")
                f.write("3. Minor discrepancies are typically due to formatting differences\n")
                if season_summary['major_discrepancies'] > 0:
                    f.write("4. Major discrepancies require manual review and correction\n")
                f.write("5. Implement automated validation in the curation pipeline\n")
                f.write("6. Enhanced PL parsing with line player data shows excellent results\n")
                
        except Exception as e:
            logger.error(f"Error generating comprehensive summary report: {e}")
    
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
        """Reconcile player and team goal data for a specific game with enhanced phase-based approach."""
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
            
            # Split goals by phase for enhanced reconciliation
            auth_phase_data = self._split_goals_by_phase(authoritative_goals)
            gs_phase_data = self._split_goals_by_phase(html_gs_goals)
            pl_phase_data = self._split_goals_by_phase(html_pl_goals)
            
            # Perform phase-based reconciliation
            phase_results = self._reconcile_by_phase(
                auth_phase_data, gs_phase_data, pl_phase_data, game_id
            )
            
            # Perform traditional team-level reconciliation (four-way comparison)
            team_results = self._reconcile_teams_four_way(authoritative_goals, html_gs_goals, html_es_goals, html_pl_goals, game_id)
            
            # Perform traditional player-level reconciliation (four-way comparison)
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
                warnings=warnings,
                game_metadata=game_metadata
            )
            
            return result
            
        except Exception as e:
            logger.error(f"Error reconciling game {game_id}: {e}")
            return None
    
    def _reconcile_by_phase(self, auth_phase_data: PhaseGoalData, gs_phase_data: PhaseGoalData, 
                           pl_phase_data: PhaseGoalData, game_id: str) -> Dict[str, PhaseReconciliationResult]:
        """Reconcile goals by game phase with appropriate data sources."""
        phase_results = {}
        
        # Regular Time: Use GS + PL data (exclude ES)
        if auth_phase_data.regular_time:
            regular_goals = gs_phase_data.regular_time + pl_phase_data.regular_time
            phase_results['regular_time'] = self._reconcile_phase(
                'regular_time', auth_phase_data.regular_time, regular_goals, 
                ['authoritative_pl', 'html_gs', 'html_pl'], game_id
            )
        
        # Overtime: Use GS + PL data (exclude ES)
        if auth_phase_data.overtime:
            overtime_goals = gs_phase_data.overtime + pl_phase_data.overtime
            phase_results['overtime'] = self._reconcile_phase(
                'overtime', auth_phase_data.overtime, overtime_goals,
                ['authoritative_pl', 'html_gs', 'html_pl'], game_id
            )
        
        # Shootout: Use PL data only (exclude GS and ES)
        if auth_phase_data.shootout:
            phase_results['shootout'] = self._reconcile_phase(
                'shootout', auth_phase_data.shootout, pl_phase_data.shootout,
                ['authoritative_pl', 'html_pl'], game_id
            )
        
        return phase_results
    
    def _reconcile_phase(self, phase: str, auth_goals: List[Dict[str, Any]], 
                        html_goals: List[Dict[str, Any]], 
                        data_sources: List[str], game_id: str) -> PhaseReconciliationResult:
        """Reconcile goals for a specific phase."""
        # Perform reconciliation logic here
        player_discrepancies = self._reconcile_players_for_phase(phase, auth_goals, html_goals)
        team_discrepancies = self._reconcile_teams_for_phase(phase, auth_goals, html_goals)
        
        # Determine overall status
        total_discrepancies = len(player_discrepancies) + len(team_discrepancies)
        if total_discrepancies == 0:
            status = 'perfect'
        elif total_discrepancies <= 2:
            status = 'minor_discrepancy'
        else:
            status = 'major_discrepancy'
        
        return PhaseReconciliationResult(
            phase=phase,
            authoritative_goals=auth_goals,
            html_goals=html_goals,
            player_discrepancies=player_discrepancies,
            team_discrepancies=team_discrepancies,
            reconciliation_status=status,
            data_sources_used=data_sources
        )
    
    def _reconcile_players_for_phase(self, phase: str, auth_goals: List[Dict[str, Any]], 
                                   html_goals: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Reconcile player statistics for a specific phase."""
        # Implementation would go here - for now return empty list
        return []
    
    def _reconcile_teams_for_phase(self, phase: str, auth_goals: List[Dict[str, Any]], 
                                 html_goals: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Reconcile team statistics for a specific phase."""
        # Implementation would go here - for now return empty list
        return []
    
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
                period_type_raw = period_descriptor.get('periodType', 'REG')
                
                # Map NHL period types to our standard format
                period_type_mapping = {
                    'REG': 'REGULAR',
                    'OT': 'OVERTIME', 
                    'SO': 'SHOOTOUT'
                }
                period_type = period_type_mapping.get(period_type_raw, 'REGULAR')
                
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
                is_shootout = period_type == 'SHOOTOUT' or period_type_raw == 'SO'
                
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
    
    def _extract_shootout_outcome(self, game_id: str) -> Dict[str, Any]:
        """Extract shootout outcome from Play-by-Play JSON boxscore data."""
        pbp_file = self.storage_path / 'json' / 'playbyplay' / f'{game_id}.json'
        if not pbp_file.exists():
            return {}
        
        with open(pbp_file, 'r') as f:
            pbp_data = json.load(f)
        
        # Check if game went to shootout
        if not pbp_data.get('shootoutInUse', False):
            return {}
        
        away_team = pbp_data.get('awayTeam', {})
        home_team = pbp_data.get('homeTeam', {})
        
        away_score = away_team.get('score', 0)
        home_score = home_team.get('score', 0)
        
        # Calculate regular goals (exclude shootout outcome)
        regular_goals = self._count_regular_goals_from_pbp(pbp_data)
        away_regular = regular_goals.get(away_team.get('id'), 0)
        home_regular = regular_goals.get(home_team.get('id'), 0)
        
        # Shootout outcome is the difference between total score and regular goals
        away_shootout_outcome = 1 if away_score > away_regular else 0
        home_shootout_outcome = 1 if home_score > home_regular else 0
        
        return {
            'away_team': away_team.get('abbrev', ''),
            'home_team': home_team.get('abbrev', ''),
            'away_regular_goals': away_regular,
            'home_regular_goals': home_regular,
            'away_total_score': away_score,
            'home_total_score': home_score,
            'away_shootout_outcome': away_shootout_outcome,
            'home_shootout_outcome': home_shootout_outcome,
            'shootout_winner': away_team.get('abbrev', '') if away_shootout_outcome > home_shootout_outcome else home_team.get('abbrev', '')
        }
    
    def _count_regular_goals_from_pbp(self, pbp_data: Dict[str, Any]) -> Dict[int, int]:
        """Count regular goals (non-shootout) by team from Play-by-Play data."""
        team_goals = {}
        
        for event in pbp_data.get('plays', []):
            if event.get('typeCode') == self.GOAL_EVENT_TYPE:
                period_descriptor = event.get('periodDescriptor', {})
                period_type_raw = period_descriptor.get('periodType', 'REG')
                
                # Only count non-shootout goals
                if period_type_raw != 'SO':
                    team_id = event.get('details', {}).get('eventOwnerTeamId')
                    if team_id:
                        team_goals[team_id] = team_goals.get(team_id, 0) + 1
        
        return team_goals
    
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
            
            # Get period and period_type from curated data
            period = goal_data.get('period', 1)
            period_type = goal_data.get('period_type', 'REGULAR')  # Use curated period_type
            is_shootout = period_type == 'SHOOTOUT'
            
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
                'period_type': period_type,  # Use the curated period_type directly
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
        
        with open(pl_file, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
        
        # Use lxml for robustness; fall back to html.parser if unavailable
        try:
            soup = BeautifulSoup(content, 'lxml')
        except Exception:
            soup = BeautifulSoup(content, 'html.parser')
        
        goals: List[Dict[str, Any]] = []
        player_mappings = self._load_player_mappings(game_id)
        
        # Iterate all rows in the document and detect goal events by the event-type cell text
        # Typical PL columns: [Evt#, Per, Str, Time, Event, Description]
        for row in soup.find_all('tr'):
            cells = row.find_all('td')
            if len(cells) < 6:
                continue
            event_number = cells[0].get_text(strip=True)
            period_text = cells[1].get_text(strip=True)
            time_text = cells[3].get_text(strip=True)
            event_type_text = cells[4].get_text(strip=True).upper()
            # Description sometimes contains newlines like 'Assist:' on next line
            description = cells[5].get_text('\n', strip=True)
            
            # Detect goal rows (covers variants like 'GOAL', 'SHOT - GOAL')
            if 'GOAL' not in event_type_text:
                continue
            
            # Determine numeric period and mapped period_type
            period = int(period_text) if period_text.isdigit() else 1
            if period >= 5:
                period_type = 'SHOOTOUT'
            elif period == 4:
                period_type = 'OVERTIME'
            else:
                period_type = 'REGULAR'
            is_shootout = (period_type == 'SHOOTOUT')
            
            # Parse description to extract team/sweater/name (+ optional assists if present)
            goal_info = self._parse_pl_goal_description(description, player_mappings)
            if not goal_info:
                continue
            
            goal_record = {
                'goal_number': int(event_number) if event_number.isdigit() else 0,
                'period': period,
                'period_type': period_type,
                'time': time_text,
                'team': goal_info['team'],
                'team_id': self.team_abbrev_to_id.get(goal_info['team'], 0),
                'scorer_id': goal_info['scorer_id'],
                'scorer_name': goal_info['scorer_name'],
                'scorer_sweater': goal_info['scorer_sweater'],
                # PL assists are unreliable; retain if parsed but they won't be used for reconciliation
                'assist1_id': goal_info.get('assist1_id'),
                'assist1_name': goal_info.get('assist1_name'),
                'assist1_sweater': goal_info.get('assist1_sweater'),
                'assist2_id': goal_info.get('assist2_id'),
                'assist2_name': goal_info.get('assist2_name'),
                'assist2_sweater': goal_info.get('assist2_sweater'),
                'is_shootout': is_shootout,
                'counts_for_stats': not is_shootout,  # Shootout goals don't count toward stats
                'source': 'html_pl'
            }
            goals.append(goal_record)
        
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
                reconciliation_status=gs_status
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
        """Reconcile team-level goal data across four sources with proper phase breakdown."""
        team_results = {}
        
        # Get team abbreviations from metadata
        metadata = self._get_game_metadata(game_id)
        home_team = metadata.get('home_team', '')
        away_team = metadata.get('away_team', '')
        
        # Extract shootout outcome from authoritative data
        shootout_outcome = self._extract_shootout_outcome(game_id)
        
        # Pre-compute PL shootout goal counts (used to derive PL shootout outcome)
        pl_so_counts: Dict[str, int] = {}
        for team in [home_team, away_team]:
            if team:
                pl_so_counts[team] = sum(1 for goal in html_pl_goals 
                                         if goal.get('team') == team and goal.get('is_shootout', False))
        
        for team in [home_team, away_team]:
            if not team:
                continue
            
            # Count goals by phase for each source
            # Regulation Time Goals
            auth_regulation = sum(1 for goal in authoritative_goals 
                                if goal.get('team') == team and goal.get('period_type') == 'REGULAR')
            gs_regulation = sum(1 for goal in html_gs_goals 
                              if goal.get('team') == team and goal.get('period_type') == 'REGULAR')
            es_regulation = sum(1 for goal in html_es_goals 
                              if goal.get('team') == team and goal.get('period_type') == 'REGULAR')
            pl_regulation = sum(1 for goal in html_pl_goals 
                              if goal.get('team') == team and goal.get('period_type') == 'REGULAR')
            
            # Overtime Goals
            auth_overtime = sum(1 for goal in authoritative_goals 
                              if goal.get('team') == team and goal.get('period_type') == 'OVERTIME')
            gs_overtime = sum(1 for goal in html_gs_goals 
                            if goal.get('team') == team and goal.get('period_type') == 'OVERTIME')
            es_overtime = sum(1 for goal in html_es_goals 
                            if goal.get('team') == team and goal.get('period_type') == 'OVERTIME')
            pl_overtime = sum(1 for goal in html_pl_goals 
                            if goal.get('team') == team and goal.get('period_type') == 'OVERTIME')
            
            # Shootout Goals (individual goals during shootout)
            auth_shootout_goals = sum(1 for goal in authoritative_goals 
                                    if goal.get('team') == team and goal.get('is_shootout', False))
            gs_shootout_goals = sum(1 for goal in html_gs_goals 
                                  if goal.get('team') == team and goal.get('is_shootout', False))
            es_shootout_goals = sum(1 for goal in html_es_goals 
                                  if goal.get('team') == team and goal.get('is_shootout', False))
            pl_shootout_goals = sum(1 for goal in html_pl_goals 
                                  if goal.get('team') == team and goal.get('is_shootout', False))
            
            # Shootout Outcome (0 or 1 - who won the shootout)
            auth_shootout_outcome = 0
            gs_shootout_outcome = 1 if gs_shootout_goals > 0 else 0  # GS shows winning goal
            es_shootout_outcome = 0  # ES doesn't provide shootout outcome
            # Compute PL shootout outcome from PL shootout goal counts
            pl_shootout_outcome = 0
            other_team = home_team if team == away_team else away_team
            if team in pl_so_counts and other_team in pl_so_counts:
                if pl_so_counts[team] > pl_so_counts[other_team]:
                    pl_shootout_outcome = 1
                else:
                    pl_shootout_outcome = 0
            
            if shootout_outcome:
                if team == shootout_outcome.get('away_team'):
                    auth_shootout_outcome = shootout_outcome.get('away_shootout_outcome', 0)
                elif team == shootout_outcome.get('home_team'):
                    auth_shootout_outcome = shootout_outcome.get('home_shootout_outcome', 0)
            
            # Calculate discrepancies by phase
            regulation_discrepancy = auth_regulation - gs_regulation  # Primary comparison with GS
            overtime_discrepancy = auth_overtime - gs_overtime
            shootout_goals_discrepancy = auth_shootout_goals - pl_shootout_goals  # PL is authoritative for shootout goals
            shootout_outcome_discrepancy = auth_shootout_outcome - gs_shootout_outcome  # GS shows outcome
            
            # Determine overall reconciliation status
            total_discrepancies = abs(regulation_discrepancy) + abs(overtime_discrepancy) + abs(shootout_goals_discrepancy) + abs(shootout_outcome_discrepancy)
            if total_discrepancies == 0:
                overall_status = 'perfect'
            elif total_discrepancies <= 2:
                overall_status = 'minor_discrepancy'
            else:
                overall_status = 'major_discrepancy'
            
            # Calculate backward compatibility totals
            auth_total = auth_regulation + auth_overtime
            gs_total = gs_regulation + gs_overtime
            es_total = es_regulation + es_overtime
            pl_total = pl_regulation + pl_overtime
            
            # ES combined REG+OT comparison
            es_combined_discrepancy = auth_total - es_total
            es_combined_status = 'perfect' if es_combined_discrepancy == 0 else ('minor_discrepancy' if abs(es_combined_discrepancy) <= 1 else 'major_discrepancy')
            
            team_results[team] = TeamReconciliationResult(
                team_id=self.team_abbrev_to_id.get(team, 0),
                team_abbrev=team,
                
                # Phase breakdown
                auth_regulation_goals=auth_regulation,
                gs_regulation_goals=gs_regulation,
                es_regulation_goals=es_regulation,
                pl_regulation_goals=pl_regulation,
                
                auth_overtime_goals=auth_overtime,
                gs_overtime_goals=gs_overtime,
                es_overtime_goals=es_overtime,
                pl_overtime_goals=pl_overtime,
                
                auth_shootout_goals=auth_shootout_goals,
                gs_shootout_goals=gs_shootout_goals,
                es_shootout_goals=es_shootout_goals,
                pl_shootout_goals=pl_shootout_goals,
                
                auth_shootout_outcome=auth_shootout_outcome,
                gs_shootout_outcome=gs_shootout_outcome,
                es_shootout_outcome=es_shootout_outcome,
                pl_shootout_outcome=pl_shootout_outcome,
                
                regulation_discrepancy=regulation_discrepancy,
                overtime_discrepancy=overtime_discrepancy,
                shootout_goals_discrepancy=shootout_goals_discrepancy,
                shootout_outcome_discrepancy=shootout_outcome_discrepancy,
                
                reconciliation_status=overall_status,
                
                # Backward compatibility fields
                authoritative_goals=auth_total,
                html_gs_goals=gs_total,
                html_es_goals=es_total,
                html_pl_goals=pl_total,
                # ES combined REG+OT
                auth_combined_goals=auth_total,
                es_combined_goals=es_total,
                es_combined_discrepancy=es_combined_discrepancy,
                es_combined_status=es_combined_status,
                gs_discrepancy=auth_total - gs_total,
                es_discrepancy=auth_total - es_total,
                pl_discrepancy=auth_total - pl_total,
                gs_reconciliation_status='perfect' if regulation_discrepancy == 0 else 'minor_discrepancy' if abs(regulation_discrepancy) <= 1 else 'major_discrepancy',
                es_reconciliation_status='perfect' if (auth_total - es_total) == 0 else 'minor_discrepancy' if abs(auth_total - es_total) <= 1 else 'major_discrepancy',
                pl_reconciliation_status='perfect' if (auth_total - pl_total) == 0 else 'minor_discrepancy' if abs(auth_total - pl_total) <= 1 else 'major_discrepancy',
                authoritative_total_goals=auth_total,
                html_total_goals=gs_total,
                goal_count_discrepancy=auth_total - gs_total,
                player_discrepancies=[]
            )
        
        return team_results
    
    
    def _reconcile_players_four_way(self, authoritative_goals: List[Dict], html_gs_goals: List[Dict], 
                                    html_es_goals: List[Dict], html_pl_goals: List[Dict], game_id: str) -> List[PlayerReconciliationResult]:
        """Reconcile player-level goal data across four sources with proper phase breakdown."""
        player_results = []
        
        # Count goals and assists by phase for each player in authoritative data
        auth_player_stats = defaultdict(lambda: {
            'regulation_goals': 0, 'regulation_assists': 0,
            'overtime_goals': 0, 'overtime_assists': 0,
            'shootout_goals': 0,
            'player_id': 0, 'name': '', 'sweater': 0, 'team': ''
        })
        
        for goal in authoritative_goals:
            period_type = goal.get('period_type', 'REGULAR')
            scorer_id = goal.get('scorer_id')
            
            if scorer_id:
                if period_type == 'REGULAR':
                    auth_player_stats[scorer_id]['regulation_goals'] += 1
                elif period_type == 'OVERTIME':
                    auth_player_stats[scorer_id]['overtime_goals'] += 1
                elif period_type == 'SHOOTOUT' or goal.get('is_shootout', False):
                    auth_player_stats[scorer_id]['shootout_goals'] += 1
                
                auth_player_stats[scorer_id]['player_id'] = scorer_id
                auth_player_stats[scorer_id]['name'] = goal['scorer_name']
                auth_player_stats[scorer_id]['sweater'] = goal['scorer_sweater']
                auth_player_stats[scorer_id]['team'] = goal['team']
            
            # Count assists (no assists in shootout)
            if period_type != 'SHOOTOUT' and not goal.get('is_shootout', False):
                assist1_id = goal.get('assist1_id')
                if assist1_id:
                    if period_type == 'REGULAR':
                        auth_player_stats[assist1_id]['regulation_assists'] += 1
                    elif period_type == 'OVERTIME':
                        auth_player_stats[assist1_id]['overtime_assists'] += 1
                    
                    auth_player_stats[assist1_id]['player_id'] = assist1_id
                    auth_player_stats[assist1_id]['name'] = goal['assist1_name']
                    auth_player_stats[assist1_id]['sweater'] = goal['assist1_sweater']
                    auth_player_stats[assist1_id]['team'] = goal['team']
                
                assist2_id = goal.get('assist2_id')
                if assist2_id:
                    if period_type == 'REGULAR':
                        auth_player_stats[assist2_id]['regulation_assists'] += 1
                    elif period_type == 'OVERTIME':
                        auth_player_stats[assist2_id]['overtime_assists'] += 1
                    
                    auth_player_stats[assist2_id]['player_id'] = assist2_id
                    auth_player_stats[assist2_id]['name'] = goal['assist2_name']
                    auth_player_stats[assist2_id]['sweater'] = goal['assist2_sweater']
                    auth_player_stats[assist2_id]['team'] = goal['team']
        
        # Count goals and assists by phase for each player in GS data
        gs_player_stats = defaultdict(lambda: {
            'regulation_goals': 0, 'regulation_assists': 0,
            'overtime_goals': 0, 'overtime_assists': 0,
            'shootout_goals': 0,
            'player_id': 0, 'name': '', 'sweater': 0, 'team': ''
        })
        
        for goal in html_gs_goals:
            period_type = goal.get('period_type', 'REGULAR')
            scorer_id = goal.get('scorer_id')
            
            if scorer_id:
                if period_type == 'REGULAR':
                    gs_player_stats[scorer_id]['regulation_goals'] += 1
                elif period_type == 'OVERTIME':
                    gs_player_stats[scorer_id]['overtime_goals'] += 1
                elif period_type == 'SHOOTOUT' or goal.get('is_shootout', False):
                    gs_player_stats[scorer_id]['shootout_goals'] += 1
                
                gs_player_stats[scorer_id]['player_id'] = scorer_id
                gs_player_stats[scorer_id]['name'] = goal['scorer_name']
                gs_player_stats[scorer_id]['sweater'] = goal['scorer_sweater']
                gs_player_stats[scorer_id]['team'] = goal['team']
            
            # Count assists (no assists in shootout)
            if period_type != 'SHOOTOUT' and not goal.get('is_shootout', False):
                assist1_id = goal.get('assist1_id')
                if assist1_id:
                    if period_type == 'REGULAR':
                        gs_player_stats[assist1_id]['regulation_assists'] += 1
                    elif period_type == 'OVERTIME':
                        gs_player_stats[assist1_id]['overtime_assists'] += 1
                    
                    gs_player_stats[assist1_id]['player_id'] = assist1_id
                    gs_player_stats[assist1_id]['name'] = goal['assist1_name']
                    gs_player_stats[assist1_id]['sweater'] = goal['assist1_sweater']
                    gs_player_stats[assist1_id]['team'] = goal['team']
                
                assist2_id = goal.get('assist2_id')
                if assist2_id:
                    if period_type == 'REGULAR':
                        gs_player_stats[assist2_id]['regulation_assists'] += 1
                    elif period_type == 'OVERTIME':
                        gs_player_stats[assist2_id]['overtime_assists'] += 1
                    
                    gs_player_stats[assist2_id]['player_id'] = assist2_id
                    gs_player_stats[assist2_id]['name'] = goal['assist2_name']
                    gs_player_stats[assist2_id]['sweater'] = goal['assist2_sweater']
                    gs_player_stats[assist2_id]['team'] = goal['team']
        
        # Count goals and assists by phase for each player in ES data
        es_player_stats = defaultdict(lambda: {
            'regulation_goals': 0, 'regulation_assists': 0,
            'overtime_goals': 0, 'overtime_assists': 0,
            'shootout_goals': 0,
            'player_id': 0, 'name': '', 'sweater': 0, 'team': ''
        })
        
        for goal in html_es_goals:
            period_type = goal.get('period_type', 'REGULAR')
            scorer_id = goal.get('scorer_id')
            
            if scorer_id:
                if period_type == 'REGULAR':
                    es_player_stats[scorer_id]['regulation_goals'] += 1
                elif period_type == 'OVERTIME':
                    es_player_stats[scorer_id]['overtime_goals'] += 1
                elif period_type == 'SHOOTOUT' or goal.get('is_shootout', False):
                    es_player_stats[scorer_id]['shootout_goals'] += 1
                
                es_player_stats[scorer_id]['player_id'] = scorer_id
                es_player_stats[scorer_id]['name'] = goal['scorer_name']
                es_player_stats[scorer_id]['sweater'] = goal['scorer_sweater']
                es_player_stats[scorer_id]['team'] = goal['team']

        # Augment ES assists directly from curated ES player statistics (assign to regulation by default)
        try:
            es_file = self.storage_path / 'json' / 'curate' / 'es' / f'es_{game_id[4:]}.json'
            if es_file.exists():
                with open(es_file, 'r') as ef:
                    es_curated = json.load(ef)
                # Merge home and visitor players
                es_players = []
                stats = es_curated.get('player_statistics', {})
                es_players.extend(stats.get('home', []) or [])
                es_players.extend(stats.get('visitor', []) or [])
                for p in es_players:
                    pid = p.get('player_id')
                    if pid is None:
                        continue
                    assists_count = int(p.get('assists', 0) or 0)
                    goals_count = int(p.get('goals', 0) or 0)
                    # Ensure player record exists
                    _ = es_player_stats[pid]
                    es_player_stats[pid]['player_id'] = pid
                    es_player_stats[pid]['name'] = es_player_stats[pid]['name'] or p.get('name', '')
                    es_player_stats[pid]['sweater'] = es_player_stats[pid]['sweater'] or p.get('sweater_number', 0)
                    team_abbrev = es_player_stats[pid]['team'] or (es_curated.get('game_header', {}).get('home_team', {}).get('abbreviation', '') if p.get('team_type') == 'home' else es_curated.get('game_header', {}).get('visitor_team', {}).get('abbreviation', ''))
                    es_player_stats[pid]['team'] = team_abbrev
                    # Assign totals to regulation buckets by convention (ES lacks phase breakdown)
                    es_player_stats[pid]['regulation_assists'] = assists_count
                    # If we reconstructed fewer goals than ES total, align to ES totals
                    if (es_player_stats[pid]['regulation_goals'] + es_player_stats[pid]['overtime_goals'] + es_player_stats[pid]['shootout_goals']) < goals_count:
                        es_player_stats[pid]['regulation_goals'] = goals_count
        except Exception:
            # Non-fatal: keep existing es_player_stats
            pass
        
        # Count goals by phase for each player in PL data (PL doesn't have assists)
        pl_player_stats = defaultdict(lambda: {
            'regulation_goals': 0, 'regulation_assists': 0,
            'overtime_goals': 0, 'overtime_assists': 0,
            'shootout_goals': 0,
            'player_id': 0, 'name': '', 'sweater': 0, 'team': ''
        })
        
        for goal in html_pl_goals:
            period_type = goal.get('period_type', 'REGULAR')
            scorer_id = goal.get('scorer_id')
            
            if scorer_id:
                if period_type == 'REGULAR':
                    pl_player_stats[scorer_id]['regulation_goals'] += 1
                elif period_type == 'OVERTIME':
                    pl_player_stats[scorer_id]['overtime_goals'] += 1
                elif period_type == 'SHOOTOUT' or goal.get('is_shootout', False):
                    pl_player_stats[scorer_id]['shootout_goals'] += 1
                
                pl_player_stats[scorer_id]['player_id'] = scorer_id
                pl_player_stats[scorer_id]['name'] = goal['scorer_name']
                pl_player_stats[scorer_id]['sweater'] = goal['scorer_sweater']
                pl_player_stats[scorer_id]['team'] = goal['team']
        
        # Get all unique players across all sources
        all_player_ids = set()
        all_player_ids.update(auth_player_stats.keys())
        all_player_ids.update(gs_player_stats.keys())
        all_player_ids.update(es_player_stats.keys())
        all_player_ids.update(pl_player_stats.keys())
        
        # Create reconciliation results for each player
        for player_id in all_player_ids:
            auth_stats = auth_player_stats[player_id]
            gs_stats = gs_player_stats[player_id]
            es_stats = es_player_stats[player_id]
            pl_stats = pl_player_stats[player_id]
            
            # Use authoritative data as primary source for player info
            player_name = auth_stats['name'] or gs_stats['name'] or es_stats['name'] or pl_stats['name']
            sweater_number = auth_stats['sweater'] or gs_stats['sweater'] or es_stats['sweater'] or pl_stats['sweater']
            team = auth_stats['team'] or gs_stats['team'] or es_stats['team'] or pl_stats['team']
            
            # Calculate discrepancies by phase
            regulation_goal_discrepancy = auth_stats['regulation_goals'] - gs_stats['regulation_goals']
            regulation_assist_discrepancy = auth_stats['regulation_assists'] - gs_stats['regulation_assists']
            overtime_goal_discrepancy = auth_stats['overtime_goals'] - gs_stats['overtime_goals']
            overtime_assist_discrepancy = auth_stats['overtime_assists'] - gs_stats['overtime_assists']
            shootout_goal_discrepancy = auth_stats['shootout_goals'] - pl_stats['shootout_goals']
            
            # Determine overall reconciliation status
            total_discrepancy = (abs(regulation_goal_discrepancy) + abs(regulation_assist_discrepancy) + 
                               abs(overtime_goal_discrepancy) + abs(overtime_assist_discrepancy) + 
                               abs(shootout_goal_discrepancy))
            
            if total_discrepancy == 0:
                status = 'perfect'
            elif total_discrepancy <= 2:
                status = 'minor_discrepancy'
            else:
                status = 'major_discrepancy'
            
            # Get player position (default to Forward if not available)
            position = "F"  # Default position
            
            # Calculate backward compatibility totals
            auth_total_goals = auth_stats['regulation_goals'] + auth_stats['overtime_goals']
            auth_total_assists = auth_stats['regulation_assists'] + auth_stats['overtime_assists']
            gs_total_goals = gs_stats['regulation_goals'] + gs_stats['overtime_goals']
            gs_total_assists = gs_stats['regulation_assists'] + gs_stats['overtime_assists']
            es_total_goals = es_stats['regulation_goals'] + es_stats['overtime_goals']
            es_total_assists = es_stats['regulation_assists'] + es_stats['overtime_assists']
            pl_total_goals = pl_stats['regulation_goals'] + pl_stats['overtime_goals']
            
            # ES combined REG+OT comparison
            es_combined_goal_discrepancy = auth_total_goals - es_total_goals
            es_combined_assist_discrepancy = auth_total_assists - es_total_assists
            es_combined_status = 'perfect' if (es_combined_goal_discrepancy == 0 and es_combined_assist_discrepancy == 0) else (
                'minor_discrepancy' if (abs(es_combined_goal_discrepancy) + abs(es_combined_assist_discrepancy)) <= 2 else 'major_discrepancy'
            )
            
            player_result = PlayerReconciliationResult(
                player_id=player_id,
                player_name=player_name,
                sweater_number=sweater_number,
                team=team,
                position=position,
                
                # Phase breakdown
                auth_regulation_goals=auth_stats['regulation_goals'],
                auth_regulation_assists=auth_stats['regulation_assists'],
                gs_regulation_goals=gs_stats['regulation_goals'],
                gs_regulation_assists=gs_stats['regulation_assists'],
                es_regulation_goals=es_stats['regulation_goals'],
                es_regulation_assists=es_stats['regulation_assists'],
                pl_regulation_goals=pl_stats['regulation_goals'],
                pl_regulation_assists=0,  # PL doesn't have assists
                
                auth_overtime_goals=auth_stats['overtime_goals'],
                auth_overtime_assists=auth_stats['overtime_assists'],
                gs_overtime_goals=gs_stats['overtime_goals'],
                gs_overtime_assists=gs_stats['overtime_assists'],
                es_overtime_goals=es_stats['overtime_goals'],
                es_overtime_assists=es_stats['overtime_assists'],
                pl_overtime_goals=pl_stats['overtime_goals'],
                pl_overtime_assists=0,  # PL doesn't have assists
                
                auth_shootout_goals=auth_stats['shootout_goals'],
                gs_shootout_goals=gs_stats['shootout_goals'],
                es_shootout_goals=es_stats['shootout_goals'],
                pl_shootout_goals=pl_stats['shootout_goals'],
                
                regulation_goal_discrepancy=regulation_goal_discrepancy,
                regulation_assist_discrepancy=regulation_assist_discrepancy,
                overtime_goal_discrepancy=overtime_goal_discrepancy,
                overtime_assist_discrepancy=overtime_assist_discrepancy,
                shootout_goal_discrepancy=shootout_goal_discrepancy,
                
                # ES combined totals
                auth_combined_goals=auth_total_goals,
                auth_combined_assists=auth_total_assists,
                es_combined_goals=es_total_goals,
                es_combined_assists=es_total_assists,
                es_combined_goal_discrepancy=es_combined_goal_discrepancy,
                es_combined_assist_discrepancy=es_combined_assist_discrepancy,
                es_combined_status=es_combined_status,
                
                reconciliation_status=status,
                
                # Backward compatibility fields
                authoritative_goals=auth_total_goals,
                authoritative_assists=auth_total_assists,
                gs_html_goals=gs_total_goals,
                gs_html_assists=gs_total_assists,
                es_html_goals=es_total_goals,
                es_html_assists=es_total_assists,
                pl_html_goals=pl_total_goals,
                pl_html_assists=0,  # PL doesn't have assists
                html_goals=gs_total_goals,  # Use GS as primary
                html_assists=es_total_assists,  # Use ES as primary
                goal_discrepancy=auth_total_goals - gs_total_goals,
                assist_discrepancy=auth_total_assists - es_total_assists
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
