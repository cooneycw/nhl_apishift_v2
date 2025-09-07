#!/usr/bin/env python3
"""
Goal Data Extractor

This utility extracts goal data from all available sources for analysis and reconciliation.
It provides a simple interface to extract goal information from different data sources.

Usage:
    python goal_data_extractor.py --game-id 2024020001 --source all
    python goal_data_extractor.py --game-id 2024020001 --source playbyplay
    python goal_data_extractor.py --game-id 2024020001 --source boxscore
"""

import json
import argparse
from pathlib import Path
from typing import Dict, List, Any, Optional

class GoalDataExtractor:
    """Extract goal data from various NHL data sources."""
    
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
    
    def extract_all_sources(self, game_id: str) -> Dict[str, Any]:
        """Extract goal data from all available sources."""
        results = {
            'game_id': game_id,
            'playbyplay': self.extract_playbyplay_goals(game_id),
            'boxscore': self.extract_boxscore_goals(game_id),
            'gs_html': self.extract_gs_goals(game_id),
            'th_events': self.extract_th_events(game_id)
        }
        
        # Add summary
        results['summary'] = {
            'playbyplay_goals': len(results['playbyplay']),
            'boxscore_total': results['boxscore'].get('total_goals', 0),
            'gs_goals': len(results['gs_html']),
            'th_g_events': len(results['th_events'].get('home', [])) + len(results['th_events'].get('visitor', []))
        }
        
        return results
    
    def extract_playbyplay_goals(self, game_id: str) -> List[Dict[str, Any]]:
        """Extract goal events from play-by-play JSON."""
        pbp_file = self.storage_path / 'json' / 'playbyplay' / f'{game_id}.json'
        if not pbp_file.exists():
            print(f"Play-by-play file not found: {pbp_file}")
            return []
        
        with open(pbp_file, 'r') as f:
            pbp_data = json.load(f)
        
        goals = []
        goal_number = 1
        
        # Load player mappings
        player_mappings = self._load_player_mappings(game_id)
        
        for event in pbp_data.get('plays', []):
            if event.get('typeCode') == self.GOAL_EVENT_TYPE:
                details = event.get('details', {})
                
                # Get period from periodDescriptor
                period_descriptor = event.get('periodDescriptor', {})
                period = period_descriptor.get('number', 1)
                
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
                
                # Determine strength
                situation_code = event.get('situationCode', '1551')
                strength = self._determine_strength(situation_code)
                
                goal = {
                    'goal_number': goal_number,
                    'period': period,
                    'time': event.get('timeInPeriod', '00:00'),
                    'team': team,
                    'scorer_id': scorer_id,
                    'scorer_name': scorer_name,
                    'scorer_sweater': scorer_sweater,
                    'assist1_id': assist1_id,
                    'assist1_name': assist1_name,
                    'assist1_sweater': assist1_sweater,
                    'assist2_id': assist2_id,
                    'assist2_name': assist2_name,
                    'assist2_sweater': assist2_sweater,
                    'strength': strength,
                    'shot_type': details.get('shotType'),
                    'zone_code': details.get('zoneCode'),
                    'x_coord': details.get('xCoord'),
                    'y_coord': details.get('yCoord'),
                    'situation_code': situation_code,
                    'source': 'playbyplay_json'
                }
                
                goals.append(goal)
                goal_number += 1
        
        return goals
    
    def extract_boxscore_goals(self, game_id: str) -> Dict[str, Any]:
        """Extract goal information from boxscore JSON."""
        boxscore_file = self.storage_path / 'json' / 'boxscores' / f'{game_id}.json'
        if not boxscore_file.exists():
            print(f"Boxscore file not found: {boxscore_file}")
            return {}
        
        with open(boxscore_file, 'r') as f:
            boxscore_data = json.load(f)
        
        away_team = boxscore_data.get('awayTeam', {})
        home_team = boxscore_data.get('homeTeam', {})
        
        return {
            'away_team': {
                'id': away_team.get('id'),
                'abbrev': away_team.get('abbrev'),
                'score': away_team.get('score', 0)
            },
            'home_team': {
                'id': home_team.get('id'),
                'abbrev': home_team.get('abbrev'),
                'score': home_team.get('score', 0)
            },
            'total_goals': (away_team.get('score', 0) + home_team.get('score', 0))
        }
    
    def extract_gs_goals(self, game_id: str) -> List[Dict[str, Any]]:
        """Extract goal events from GS HTML report."""
        gs_file = self.storage_path / 'json' / 'curate' / 'gs' / f'gs_{game_id[4:]}.json'
        if not gs_file.exists():
            print(f"GS file not found: {gs_file}")
            return []
        
        with open(gs_file, 'r') as f:
            gs_data = json.load(f)
        
        goals = []
        scoring_summary = gs_data.get('scoring_summary', {})
        
        for goal_data in scoring_summary.get('goals', []):
            scorer = goal_data.get('scorer', {})
            assist1 = goal_data.get('assist1', {})
            assist2 = goal_data.get('assist2', {})
            
            goal = {
                'goal_number': goal_data.get('goal_number', 0),
                'period': goal_data.get('period', 1),
                'time': goal_data.get('time', '00:00'),
                'team': goal_data.get('team', ''),
                'scorer_name': scorer.get('name', ''),
                'scorer_sweater': scorer.get('sweater_number', 0),
                'assist1_name': assist1.get('name') if assist1 else None,
                'assist1_sweater': assist1.get('sweater_number') if assist1 else None,
                'assist2_name': assist2.get('name') if assist2 else None,
                'assist2_sweater': assist2.get('sweater_number') if assist2 else None,
                'strength': goal_data.get('strength', 'EV'),
                'source': 'gs_html'
            }
            
            goals.append(goal)
        
        return goals
    
    def extract_th_events(self, game_id: str) -> Dict[str, List[Dict]]:
        """Extract 'G' events from TH/TV HTML reports."""
        th_file = self.storage_path / 'json' / 'curate' / 'th' / f'th_{game_id[4:]}.json'
        tv_file = self.storage_path / 'json' / 'curate' / 'tv' / f'tv_{game_id[4:]}.json'
        
        events = {'home': [], 'visitor': []}
        
        # Extract TH events
        if th_file.exists():
            with open(th_file, 'r') as f:
                th_data = json.load(f)
            events['home'] = self._extract_g_events_from_th_data(th_data, 'home')
        
        # Extract TV events
        if tv_file.exists():
            with open(tv_file, 'r') as f:
                tv_data = json.load(f)
            events['visitor'] = self._extract_g_events_from_th_data(tv_data, 'visitor')
        
        return events
    
    def _extract_g_events_from_th_data(self, th_data: Dict, team_type: str) -> List[Dict]:
        """Extract 'G' events from TH/TV data."""
        events = []
        
        player_data = th_data.get('player_time_on_ice', {}).get(team_type, [])
        
        for player in player_data:
            for shift in player.get('shifts', []):
                if 'G' in shift.get('event', ''):
                    events.append({
                        'player_name': player.get('name'),
                        'sweater_number': player.get('sweater_number'),
                        'team_type': team_type,
                        'period': shift.get('period'),
                        'shift_number': shift.get('shift_number'),
                        'start_time': shift.get('start', {}).get('elapsed'),
                        'end_time': shift.get('end', {}).get('elapsed'),
                        'event': shift.get('event')
                    })
        
        return events
    
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
        
        # Extract from away team
        away_team = player_stats.get('awayTeam', {})
        for position_group in ['forwards', 'defense', 'goalies']:
            for player in away_team.get(position_group, []):
                player_id = player.get('playerId')
                if player_id:
                    player_mappings[player_id] = {
                        'name': player.get('name', {}).get('default', ''),
                        'sweaterNumber': player.get('sweaterNumber', 0)
                    }
        
        # Extract from home team
        home_team = player_stats.get('homeTeam', {})
        for position_group in ['forwards', 'defense', 'goalies']:
            for player in home_team.get(position_group, []):
                player_id = player.get('playerId')
                if player_id:
                    player_mappings[player_id] = {
                        'name': player.get('name', {}).get('default', ''),
                        'sweaterNumber': player.get('sweaterNumber', 0)
                    }
        
        return player_mappings
    
    def _determine_strength(self, situation_code: str) -> str:
        """Determine strength situation from situation code."""
        if situation_code == '1551':
            return 'EV'
        elif situation_code == '1451':
            return 'PP'
        elif situation_code == '1351':
            return 'SH'
        elif situation_code == '1651':
            return '4v4'
        elif situation_code == '1751':
            return '3v3'
        else:
            return 'EV'
    
    def print_goal_summary(self, game_id: str):
        """Print a summary of goal data from all sources."""
        data = self.extract_all_sources(game_id)
        summary = data['summary']
        
        print(f"\n=== GOAL DATA SUMMARY FOR GAME {game_id} ===")
        print(f"Play-by-Play Goals: {summary['playbyplay_goals']}")
        print(f"Boxscore Total: {summary['boxscore_total']}")
        print(f"GS HTML Goals: {summary['gs_goals']}")
        print(f"TH/TV 'G' Events: {summary['th_g_events']}")
        print()
        
        # Show play-by-play goals (most accurate)
        if data['playbyplay']:
            print("PLAY-BY-PLAY GOALS (Authoritative):")
            for goal in data['playbyplay']:
                assists = []
                if goal['assist1_name']:
                    assists.append(f"{goal['assist1_name']} #{goal['assist1_sweater']}")
                if goal['assist2_name']:
                    assists.append(f"{goal['assist2_name']} #{goal['assist2_sweater']}")
                
                assist_str = f" ({', '.join(assists)})" if assists else ""
                print(f"  Goal {goal['goal_number']}: {goal['scorer_name']} #{goal['scorer_sweater']} ({goal['team']}) - Period {goal['period']}, {goal['time']} - {goal['strength']}{assist_str}")
        
        print()
        
        # Show boxscore summary
        if data['boxscore']:
            print("BOXSCORE SUMMARY:")
            print(f"  {data['boxscore']['away_team']['abbrev']}: {data['boxscore']['away_team']['score']}")
            print(f"  {data['boxscore']['home_team']['abbrev']}: {data['boxscore']['home_team']['score']}")
            print(f"  Total: {data['boxscore']['total_goals']}")
        
        print()
        
        # Show GS goals
        if data['gs_html']:
            print("GS HTML GOALS:")
            for goal in data['gs_html']:
                assists = []
                if goal['assist1_name']:
                    assists.append(f"{goal['assist1_name']} #{goal['assist1_sweater']}")
                if goal['assist2_name']:
                    assists.append(f"{goal['assist2_name']} #{goal['assist2_sweater']}")
                
                assist_str = f" ({', '.join(assists)})" if assists else ""
                print(f"  Goal {goal['goal_number']}: {goal['scorer_name']} #{goal['scorer_sweater']} ({goal['team']}) - Period {goal['period']}, {goal['time']} - {goal['strength']}{assist_str}")
        
        print()
        
        # Show TH/TV events (with warning)
        if data['th_events']['home'] or data['th_events']['visitor']:
            print("TH/TV 'G' EVENTS (Players on ice during goals - NOT goal scorers):")
            print("  WARNING: 'G' events mark ALL players on ice during goals, not goal scorers!")
            
            if data['th_events']['home']:
                print("  Home Team 'G' Events:")
                for event in data['th_events']['home'][:5]:  # Show first 5
                    print(f"    {event['player_name']} #{event['sweater_number']} - Period {event['period']}, Shift {event['shift_number']}")
                if len(data['th_events']['home']) > 5:
                    print(f"    ... and {len(data['th_events']['home']) - 5} more")
            
            if data['th_events']['visitor']:
                print("  Visitor Team 'G' Events:")
                for event in data['th_events']['visitor'][:5]:  # Show first 5
                    print(f"    {event['player_name']} #{event['sweater_number']} - Period {event['period']}, Shift {event['shift_number']}")
                if len(data['th_events']['visitor']) > 5:
                    print(f"    ... and {len(data['th_events']['visitor']) - 5} more")

def main():
    parser = argparse.ArgumentParser(description='Extract goal data from NHL sources')
    parser.add_argument('--game-id', required=True, help='Game ID to extract')
    parser.add_argument('--source', choices=['all', 'playbyplay', 'boxscore', 'gs_html', 'th_events'], 
                       default='all', help='Data source to extract from')
    parser.add_argument('--storage-path', default='storage/20242025', help='Storage path')
    parser.add_argument('--output', help='Output file for JSON results')
    
    args = parser.parse_args()
    
    extractor = GoalDataExtractor(args.storage_path)
    
    if args.source == 'all':
        # Show summary
        extractor.print_goal_summary(args.game_id)
        
        # Extract all data
        data = extractor.extract_all_sources(args.game_id)
        
        if args.output:
            with open(args.output, 'w') as f:
                json.dump(data, f, indent=2)
            print(f"\nData saved to {args.output}")
    
    elif args.source == 'playbyplay':
        goals = extractor.extract_playbyplay_goals(args.game_id)
        print(f"Play-by-Play Goals for Game {args.game_id}: {len(goals)}")
        for goal in goals:
            print(f"  {goal['scorer_name']} #{goal['scorer_sweater']} ({goal['team']}) - Period {goal['period']}, {goal['time']}")
    
    elif args.source == 'boxscore':
        boxscore = extractor.extract_boxscore_goals(args.game_id)
        if boxscore:
            print(f"Boxscore for Game {args.game_id}:")
            print(f"  {boxscore['away_team']['abbrev']}: {boxscore['away_team']['score']}")
            print(f"  {boxscore['home_team']['abbrev']}: {boxscore['home_team']['score']}")
            print(f"  Total: {boxscore['total_goals']}")
    
    elif args.source == 'gs_html':
        goals = extractor.extract_gs_goals(args.game_id)
        print(f"GS HTML Goals for Game {args.game_id}: {len(goals)}")
        for goal in goals:
            print(f"  {goal['scorer_name']} #{goal['scorer_sweater']} ({goal['team']}) - Period {goal['period']}, {goal['time']}")
    
    elif args.source == 'th_events':
        events = extractor.extract_th_events(args.game_id)
        total_events = len(events['home']) + len(events['visitor'])
        print(f"TH/TV 'G' Events for Game {args.game_id}: {total_events}")
        print("WARNING: These mark players on ice during goals, NOT goal scorers!")
        print(f"  Home: {len(events['home'])} events")
        print(f"  Visitor: {len(events['visitor'])} events")

if __name__ == '__main__':
    main()
