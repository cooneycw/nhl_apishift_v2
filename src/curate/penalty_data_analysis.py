#!/usr/bin/env python3
"""
NHL Penalty Data Analysis Script
Analyzes penalty data relationships across different data sources for the 2024-2025 season.
Enhanced to handle complex penalty scenarios and examine event/situation codes.
"""

import json
import os
import re
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from collections import defaultdict
import pandas as pd

class PenaltyDataAnalyzer:
    def __init__(self, storage_path: str = "storage/20242025"):
        self.storage_path = Path(storage_path)
        self.gamecenter_path = self.storage_path / "json" / "gamecenter_landing"
        self.boxscore_path = self.storage_path / "json" / "boxscores"
        self.playbyplay_path = self.storage_path / "json" / "playbyplay"
        self.shiftcharts_path = self.storage_path / "json" / "shiftcharts"
        self.html_path = self.storage_path / "html" / "reports"
        
        # Analysis results
        self.penalty_summary = {}
        self.data_relationships = {}
        self.discrepancies = []
        
        # Penalty rule analysis
        self.penalty_rules = {}
        self.event_codes = {}
        self.situation_codes = {}
        
    def analyze_game_penalties(self, game_id: str) -> Dict:
        """Analyze penalty data for a specific game across all sources."""
        game_data = {
            'game_id': game_id,
            'gamecenter_penalties': [],
            'boxscore_penalties': [],
            'html_penalties': [],
            'playbyplay_penalties': [],
            'relationships': {},
            'discrepancies': [],
            'complex_scenarios': []
        }
        
        # 1. Extract penalties from Gamecenter Landing
        gamecenter_file = self.gamecenter_path / f"gamecenter_landing_{game_id}.json"
        if gamecenter_file.exists():
            game_data['gamecenter_penalties'] = self.extract_gamecenter_penalties(gamecenter_file)
        
        # 2. Extract penalties from Boxscore
        boxscore_file = self.boxscore_path / f"{game_id}.json"
        if boxscore_file.exists():
            game_data['boxscore_penalties'] = self.extract_boxscore_penalties(boxscore_file)
        
        # 3. Extract penalties from HTML Play-by-Play
        html_file = self.html_path / "PL" / f"PL{game_id}.HTM"
        if html_file.exists():
            game_data['html_penalties'] = self.extract_html_penalties(html_file)
        
        # 4. Extract penalties from Play-by-Play JSON (if available)
        playbyplay_file = self.playbyplay_path / f"{game_id}.json"
        if playbyplay_file.exists():
            game_data['playbyplay_penalties'] = self.extract_playbyplay_penalties(playbyplay_file)
        
        # 5. Analyze relationships and discrepancies
        game_data['relationships'] = self.analyze_penalty_relationships(game_data)
        game_data['discrepancies'] = self.identify_discrepancies(game_data)
        game_data['complex_scenarios'] = self.identify_complex_penalty_scenarios(game_data)
        
        return game_data
    
    def extract_gamecenter_penalties(self, file_path: Path) -> List[Dict]:
        """Extract penalty data from Gamecenter Landing JSON file."""
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
            
            penalties = []
            if 'summary' in data and 'penalties' in data['summary']:
                for period_penalties in data['summary']['penalties']:
                    period_num = period_penalties['periodDescriptor']['number']
                    for penalty in period_penalties.get('penalties', []):
                        penalty_data = {
                            'period': period_num,
                            'time': penalty.get('timeInPeriod', ''),
                            'type': penalty.get('type', ''),
                            'duration': penalty.get('duration', 0),
                            'committed_by': penalty.get('committedByPlayer', {}).get('default', ''),
                            'team': penalty.get('teamAbbrev', {}).get('default', ''),
                            'drawn_by': penalty.get('drawnBy', {}).get('default', ''),
                            'description': penalty.get('descKey', ''),
                            'source': 'gamecenter_landing',
                            'event_id': penalty.get('eventId', ''),
                            'situation_code': penalty.get('situationCode', ''),
                            'strength': penalty.get('strength', ''),
                            'is_power_play': self.is_power_play_penalty(penalty.get('descKey', ''))
                        }
                        penalties.append(penalty_data)
            
            return penalties
        except Exception as e:
            print(f"Error extracting penalties from {file_path}: {e}")
            return []
    
    def extract_playbyplay_penalties(self, file_path: Path) -> List[Dict]:
        """Extract penalty data from Play-by-Play JSON file."""
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
            
            penalties = []
            if 'plays' in data:
                for play in data['plays']:
                    if 'eventTypeId' in play and play['eventTypeId'] in ['PENALTY', 'PENALTY_SHOT']:
                        penalty_data = {
                            'event_id': play.get('eventId', ''),
                            'period': play.get('periodDescriptor', {}).get('number', ''),
                            'time': play.get('timeInPeriod', ''),
                            'time_remaining': play.get('timeRemaining', ''),
                            'situation_code': play.get('situationCode', ''),
                            'event_type': play.get('eventTypeId', ''),
                            'team': play.get('team', {}).get('abbrev', ''),
                            'player': play.get('player', {}).get('name', ''),
                            'penalty_type': play.get('penaltyType', ''),
                            'penalty_minutes': play.get('penaltyMinutes', 0),
                            'source': 'playbyplay_json'
                        }
                        penalties.append(penalty_data)
            
            return penalties
        except Exception as e:
            print(f"Error extracting penalties from {file_path}: {e}")
            return []
    
    def extract_boxscore_penalties(self, file_path: Path) -> List[Dict]:
        """Extract penalty data from Boxscore JSON file."""
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
            
            penalties = []
            
            # Extract player-level PIM data
            for team_type in ['awayTeam', 'homeTeam']:
                if team_type in data and 'playerByGameStats' in data:
                    team_data = data['playerByGameStats'].get(team_type, {})
                    team_abbrev = data[team_type]['abbrev']
                    
                    for position_type in ['forwards', 'defensemen', 'goalies']:
                        if position_type in team_data:
                            for player in team_data[position_type]:
                                if 'pim' in player and player['pim'] > 0:
                                    penalty_data = {
                                        'player_id': player.get('playerId', ''),
                                        'player_name': player.get('name', {}).get('default', ''),
                                        'team': team_abbrev,
                                        'position': player.get('position', ''),
                                        'pim': player['pim'],
                                        'source': 'boxscore'
                                    }
                                    penalties.append(penalty_data)
            
            return penalties
        except Exception as e:
            print(f"Error extracting penalties from {file_path}: {e}")
            return []
    
    def extract_html_penalties(self, file_path: Path) -> List[Dict]:
        """Extract penalty data from HTML Play-by-Play file."""
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
            
            penalties = []
            
            # Look for penalty patterns in HTML
            # This is a simplified approach - in practice, you'd want more robust HTML parsing
            penalty_patterns = [
                r'(\d{1,2}:\d{2})\s*([A-Z]+)\s*([A-Z\s]+)\s*penalty\s*([^<]+)',
                r'(\d{1,2}:\d{2})\s*penalty\s*([^<]+)',
            ]
            
            for pattern in penalty_patterns:
                matches = re.findall(pattern, content, re.IGNORECASE)
                for match in matches:
                    if len(match) >= 2:
                        penalty_data = {
                            'time': match[0] if match[0] else '',
                            'description': match[-1].strip() if match[-1] else '',
                            'source': 'html_playbyplay'
                        }
                        penalties.append(penalty_data)
            
            return penalties
        except Exception as e:
            print(f"Error extracting penalties from {file_path}: {e}")
            return []
    
    def is_power_play_penalty(self, penalty_description: str) -> bool:
        """Determine if a penalty leads to a power play."""
        non_power_play_penalties = [
            'fighting', 'misconduct', 'game-misconduct', 'match-penalty',
            'too-many-men-on-the-ice', 'delay-of-game', 'unsportsmanlike-conduct'
        ]
        
        penalty_lower = penalty_description.lower()
        return not any(non_pp in penalty_lower for non_pp in non_power_play_penalties)
    
    def identify_complex_penalty_scenarios(self, game_data: Dict) -> List[Dict]:
        """Identify complex penalty scenarios that require special handling."""
        scenarios = []
        
        # 1. Simultaneous penalties (same time, multiple players)
        time_groups = defaultdict(list)
        for penalty in game_data['gamecenter_penalties']:
            time_groups[penalty['time']].append(penalty)
        
        for time, penalties in time_groups.items():
            if len(penalties) > 1:
                # Check if penalties are to different teams
                teams = set(p['team'] for p in penalties)
                if len(teams) > 1:
                    scenarios.append({
                        'type': 'simultaneous_penalties',
                        'time': time,
                        'penalties': penalties,
                        'description': f'Multiple penalties at {time} to different teams'
                    })
                else:
                    scenarios.append({
                        'type': 'multiple_team_penalties',
                        'time': time,
                        'penalties': penalties,
                        'description': f'Multiple penalties at {time} to same team'
                    })
        
        # 2. Team penalties (no specific player)
        team_penalties = [p for p in game_data['gamecenter_penalties'] 
                         if not p['committed_by'] or p['committed_by'] == '']
        if team_penalties:
            scenarios.append({
                'type': 'team_penalties',
                'penalties': team_penalties,
                'description': 'Penalties without specific player assignment'
            })
        
        # 3. Non-power play penalties
        non_pp_penalties = [p for p in game_data['gamecenter_penalties'] 
                           if not p.get('is_power_play', True)]
        if non_pp_penalties:
            scenarios.append({
                'type': 'non_power_play_penalties',
                'penalties': non_pp_penalties,
                'description': 'Penalties that do not lead to power plays'
            })
        
        # 4. Penalty shot scenarios
        penalty_shots = [p for p in game_data['gamecenter_penalties'] 
                        if 'penalty-shot' in p.get('description', '').lower()]
        if penalty_shots:
            scenarios.append({
                'type': 'penalty_shots',
                'penalties': penalty_shots,
                'description': 'Penalty shot scenarios'
            })
        
        return scenarios
    
    def analyze_situation_codes(self, game_data: Dict) -> Dict:
        """Analyze situation codes to understand penalty rules and power play scenarios."""
        situation_analysis = {
            'situation_codes': defaultdict(list),
            'power_play_scenarios': [],
            'even_strength_scenarios': [],
            'penalty_kill_scenarios': []
        }
        
        for penalty in game_data['gamecenter_penalties']:
            situation_code = penalty.get('situation_code', '')
            if situation_code:
                situation_analysis['situation_codes'][situation_code].append(penalty)
        
        # Analyze common situation codes
        for code, penalties in situation_analysis['situation_codes'].items():
            if len(penalties) > 1:
                # Look for patterns in this situation code
                teams = set(p['team'] for p in penalties)
                power_play_status = [p.get('is_power_play', True) for p in penalties]
                
                situation_analysis['situation_codes'][code] = {
                    'penalties': penalties,
                    'teams_involved': list(teams),
                    'power_play_impact': power_play_status,
                    'total_penalties': len(penalties)
                }
        
        return situation_analysis
    
    def analyze_penalty_relationships(self, game_data: Dict) -> Dict:
        """Analyze relationships between penalty data from different sources."""
        relationships = {
            'total_penalties_by_source': {},
            'player_penalty_mapping': {},
            'time_consistency': {},
            'team_penalty_totals': {},
            'power_play_analysis': {},
            'complex_scenario_summary': {}
        }
        
        # Count total penalties by source
        relationships['total_penalties_by_source'] = {
            'gamecenter_landing': len(game_data['gamecenter_penalties']),
            'boxscore': len(game_data['boxscore_penalties']),
            'html_playbyplay': len(game_data['html_penalties']),
            'playbyplay_json': len(game_data['playbyplay_penalties'])
        }
        
        # Map player penalties between sources
        player_penalties = defaultdict(list)
        
        # From gamecenter landing
        for penalty in game_data['gamecenter_penalties']:
            if penalty['committed_by']:
                player_penalties[penalty['committed_by']].append({
                    'source': 'gamecenter_landing',
                    'data': penalty
                })
        
        # From boxscore
        for penalty in game_data['boxscore_penalties']:
            if penalty['player_name']:
                player_penalties[penalty['player_name']].append({
                    'source': 'boxscore',
                    'data': penalty
                })
        
        relationships['player_penalty_mapping'] = dict(player_penalties)
        
        # Analyze team penalty totals
        team_totals = defaultdict(int)
        team_power_play_penalties = defaultdict(int)
        team_non_power_play_penalties = defaultdict(int)
        
        for penalty in game_data['gamecenter_penalties']:
            if penalty['team'] and penalty['duration']:
                team_totals[penalty['team']] += penalty['duration']
                if penalty.get('is_power_play', True):
                    team_power_play_penalties[penalty['team']] += penalty['duration']
                else:
                    team_non_power_play_penalties[penalty['team']] += penalty['duration']
        
        relationships['team_penalty_totals'] = dict(team_totals)
        relationships['power_play_analysis'] = {
            'power_play_penalties': dict(team_power_play_penalties),
            'non_power_play_penalties': dict(team_non_power_play_penalties)
        }
        
        # Analyze complex scenarios
        if game_data['complex_scenarios']:
            relationships['complex_scenario_summary'] = {
                'total_scenarios': len(game_data['complex_scenarios']),
                'scenario_types': list(set(s['type'] for s in game_data['complex_scenarios'])),
                'scenarios': game_data['complex_scenarios']
            }
        
        return relationships
    
    def identify_discrepancies(self, game_data: Dict) -> List[Dict]:
        """Identify discrepancies between penalty data sources."""
        discrepancies = []
        
        # Check for missing penalty data in different sources
        gamecenter_count = len(game_data['gamecenter_penalties'])
        boxscore_count = len(game_data['boxscore_penalties'])
        html_count = len(game_data['html_penalties'])
        playbyplay_count = len(game_data['playbyplay_penalties'])
        
        if gamecenter_count == 0 and (boxscore_count > 0 or html_count > 0):
            discrepancies.append({
                'type': 'missing_source_data',
                'description': 'Gamecenter landing missing penalty data',
                'severity': 'high'
            })
        
        # Check for penalty count mismatches
        if abs(gamecenter_count - html_count) > 2:
            discrepancies.append({
                'type': 'count_mismatch',
                'description': f'Penalty count mismatch: Gamecenter={gamecenter_count}, HTML={html_count}',
                'severity': 'medium'
            })
        
        # Check for complex scenario handling
        if game_data['complex_scenarios']:
            for scenario in game_data['complex_scenarios']:
                if scenario['type'] == 'simultaneous_penalties':
                    # Verify that all simultaneous penalties are captured
                    penalties = scenario['penalties']
                    teams = set(p['team'] for p in penalties)
                    if len(teams) > 1:
                        # This should result in 4-on-4 or other even strength scenarios
                        discrepancies.append({
                            'type': 'simultaneous_penalty_validation',
                            'description': f'Simultaneous penalties at {scenario["time"]} need power play validation',
                            'severity': 'medium',
                            'scenario': scenario
                        })
        
        # Check for player name inconsistencies
        gamecenter_players = set(p['committed_by'] for p in game_data['gamecenter_penalties'] if p['committed_by'])
        boxscore_players = set(p['player_name'] for p in game_data['boxscore_penalties'] if p['player_name'])
        
        if gamecenter_players and boxscore_players:
            common_players = gamecenter_players.intersection(boxscore_players)
            if len(common_players) < min(len(gamecenter_players), len(boxscore_players)) * 0.8:
                discrepancies.append({
                    'type': 'player_name_mismatch',
                    'description': 'Significant player name mismatch between sources',
                    'severity': 'medium'
                })
        
        return discrepancies
    
    def analyze_season_penalties(self, sample_size: int = 10) -> Dict:
        """Analyze penalty data for a sample of games across the season."""
        print(f"Analyzing penalty data for {sample_size} sample games...")
        
        # Get list of available games
        gamecenter_files = list(self.gamecenter_path.glob("gamecenter_landing_*.json"))
        sample_games = gamecenter_files[:sample_size]
        
        season_analysis = {
            'total_games_analyzed': len(sample_games),
            'games_with_penalties': 0,
            'total_penalties': 0,
            'penalty_distribution': defaultdict(int),
            'source_reliability': {},
            'common_discrepancies': defaultdict(int),
            'complex_scenarios': defaultdict(int),
            'situation_code_analysis': defaultdict(int),
            'power_play_analysis': {
                'power_play_penalties': 0,
                'non_power_play_penalties': 0,
                'simultaneous_penalties': 0
            }
        }
        
        for game_file in sample_games:
            game_id = game_file.stem.replace('gamecenter_landing_', '')
            print(f"Analyzing game {game_id}...")
            
            game_analysis = self.analyze_game_penalties(game_id)
            
            if game_analysis['gamecenter_penalties']:
                season_analysis['games_with_penalties'] += 1
                season_analysis['total_penalties'] += len(game_analysis['gamecenter_penalties'])
                
                # Analyze penalty types
                for penalty in game_analysis['gamecenter_penalties']:
                    penalty_type = penalty.get('description', 'unknown')
                    season_analysis['penalty_distribution'][penalty_type] += 1
                    
                    # Track power play vs non-power play penalties
                    if penalty.get('is_power_play', True):
                        season_analysis['power_play_analysis']['power_play_penalties'] += 1
                    else:
                        season_analysis['power_play_analysis']['non_power_play_penalties'] += 1
                
                # Track complex scenarios
                for scenario in game_analysis['complex_scenarios']:
                    scenario_type = scenario['type']
                    season_analysis['complex_scenarios'][scenario_type] += 1
                    
                    if scenario_type == 'simultaneous_penalties':
                        season_analysis['power_play_analysis']['simultaneous_penalties'] += 1
                
                # Track situation codes
                for penalty in game_analysis['gamecenter_penalties']:
                    situation_code = penalty.get('situation_code', '')
                    if situation_code:
                        season_analysis['situation_code_analysis'][situation_code] += 1
                
                # Track discrepancies
                for discrepancy in game_analysis['discrepancies']:
                    season_analysis['common_discrepancies'][discrepancy['type']] += 1
        
        # Calculate source reliability
        if season_analysis['total_games_analyzed'] > 0:
            season_analysis['source_reliability'] = {
                'gamecenter_landing': season_analysis['games_with_penalties'] / season_analysis['total_games_analyzed'],
                'data_completeness': season_analysis['total_penalties'] / max(season_analysis['games_with_penalties'], 1)
            }
        
        return season_analysis
    
    def generate_reconciliation_report(self) -> str:
        """Generate a comprehensive reconciliation report."""
        report = []
        report.append("# NHL Penalty Data Reconciliation Report")
        report.append("## Analysis Summary")
        report.append("")
        
        # Season analysis
        season_analysis = self.analyze_season_penalties(sample_size=15)
        
        report.append(f"### Games Analyzed: {season_analysis['total_games_analyzed']}")
        report.append(f"### Games with Penalties: {season_analysis['games_with_penalties']}")
        report.append(f"### Total Penalties: {season_analysis['total_penalties']}")
        report.append("")
        
        # Penalty distribution
        report.append("### Penalty Type Distribution")
        for penalty_type, count in sorted(season_analysis['penalty_distribution'].items(), 
                                        key=lambda x: x[1], reverse=True):
            report.append(f"- **{penalty_type}**: {count}")
        report.append("")
        
        # Power play analysis
        report.append("### Power Play Analysis")
        pp_analysis = season_analysis['power_play_analysis']
        report.append(f"- **Power Play Penalties**: {pp_analysis['power_play_penalties']}")
        report.append(f"- **Non-Power Play Penalties**: {pp_analysis['non_power_play_penalties']}")
        report.append(f"- **Simultaneous Penalties**: {pp_analysis['simultaneous_penalties']}")
        report.append("")
        
        # Complex scenarios
        report.append("### Complex Penalty Scenarios")
        for scenario_type, count in sorted(season_analysis['complex_scenarios'].items(), 
                                         key=lambda x: x[1], reverse=True):
            report.append(f"- **{scenario_type}**: {count} occurrences")
        report.append("")
        
        # Situation codes
        report.append("### Situation Code Analysis")
        for code, count in sorted(season_analysis['situation_code_analysis'].items(), 
                                 key=lambda x: x[1], reverse=True)[:10]:
            report.append(f"- **{code}**: {count} occurrences")
        report.append("")
        
        # Source reliability
        report.append("### Source Reliability")
        for source, reliability in season_analysis['source_reliability'].items():
            report.append(f"- **{source}**: {reliability:.2%}")
        report.append("")
        
        # Common discrepancies
        report.append("### Common Discrepancies")
        for discrepancy_type, count in sorted(season_analysis['common_discrepancies'].items(), 
                                            key=lambda x: x[1], reverse=True):
            report.append(f"- **{discrepancy_type}**: {count} occurrences")
        report.append("")
        
        # Complex penalty rules
        report.append("## Complex Penalty Rules and Scenarios")
        report.append("")
        report.append("### 1. Simultaneous Penalties")
        report.append("- **Definition**: Multiple penalties assessed at the same time")
        report.append("- **Impact**: Can result in 4-on-4, 3-on-3, or other even strength scenarios")
        report.append("- **Reconciliation Challenge**: Must ensure all penalties are captured and power play calculations are correct")
        report.append("")
        
        report.append("### 2. Team Penalties")
        report.append("- **Definition**: Penalties without specific player assignment (e.g., too many men on ice)")
        report.append("- **Impact**: Penalty served by another player, affects team statistics")
        report.append("- **Reconciliation Challenge**: Link penalty to serving player and validate team totals")
        report.append("")
        
        report.append("### 3. Non-Power Play Penalties")
        report.append("- **Definition**: Penalties that don't result in power plays (fighting, misconducts)")
        report.append("- **Impact**: No numerical advantage, different statistical treatment")
        report.append("- **Reconciliation Challenge**: Ensure proper categorization and statistical handling")
        report.append("")
        
        report.append("### 4. Event and Situation Codes")
        report.append("- **Event Codes**: Identify penalty event types (PENALTY, PENALTY_SHOT)")
        report.append("- **Situation Codes**: Define game situations (power play, even strength, penalty kill)")
        report.append("- **Reconciliation Challenge**: Map codes to penalty rules and validate consistency")
        report.append("")
        
        # Recommendations
        report.append("## Enhanced Reconciliation Strategy for 100% Accuracy")
        report.append("")
        report.append("### 1. Primary Data Source")
        report.append("- **Gamecenter Landing** remains the authoritative source for penalty data")
        report.append("- **Enhanced Validation**: Cross-reference with event codes and situation codes")
        report.append("- **Complex Scenario Handling**: Implement logic for simultaneous penalties and team penalties")
        report.append("")
        
        report.append("### 2. Advanced Cross-Validation")
        report.append("- **Power Play Validation**: Verify power play calculations based on penalty types")
        report.append("- **Simultaneous Penalty Logic**: Ensure 4-on-4 and other scenarios are correctly calculated")
        report.append("- **Team Penalty Assignment**: Link team penalties to serving players")
        report.append("")
        
        report.append("### 3. Event Code Analysis")
        report.append("- **Penalty Event Types**: Map PENALTY vs PENALTY_SHOT events")
        report.append("- **Situation Code Mapping**: Understand power play vs even strength scenarios")
        report.append("- **Rule Validation**: Implement NHL penalty rules for validation")
        report.append("")
        
        report.append("### 4. Implementation Priority")
        report.append("1. **High Priority**: Implement complex penalty scenario handling")
        report.append("2. **Medium Priority**: Develop event code and situation code analysis")
        report.append("3. **Low Priority**: Enhance HTML parsing for additional context")
        
        return "\n".join(report)

def main():
    """Main analysis function."""
    analyzer = PenaltyDataAnalyzer()
    
    # Generate comprehensive report
    report = analyzer.generate_reconciliation_report()
    
    # Save report
    with open("penalty_reconciliation_report.md", "w") as f:
        f.write(report)
    
    print("Enhanced penalty data analysis complete!")
    print("Report saved to: penalty_reconciliation_report.md")
    
    # Also print summary to console
    print("\n" + "="*50)
    print("ENHANCED ANALYSIS SUMMARY")
    print("="*50)
    
    # Analyze a few sample games with enhanced analysis
    sample_games = ["2024021171", "2024021130", "2024021152"]
    
    for game_id in sample_games:
        print(f"\nGame {game_id}:")
        game_data = analyzer.analyze_game_penalties(game_id)
        
        print(f"  Gamecenter Penalties: {len(game_data['gamecenter_penalties'])}")
        print(f"  Boxscore Penalties: {len(game_data['boxscore_penalties'])}")
        print(f"  HTML Penalties: {len(game_data['html_penalties'])}")
        print(f"  Play-by-Play Penalties: {len(game_data['playbyplay_penalties'])}")
        print(f"  Complex Scenarios: {len(game_data['complex_scenarios'])}")
        print(f"  Discrepancies: {len(game_data['discrepancies'])}")
        
        # Show complex scenarios
        if game_data['complex_scenarios']:
            print("  Complex Scenarios Found:")
            for scenario in game_data['complex_scenarios']:
                print(f"    - {scenario['type']}: {scenario['description']}")

if __name__ == "__main__":
    main()
