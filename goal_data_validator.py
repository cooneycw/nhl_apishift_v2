#!/usr/bin/env python3
"""
Goal Data Validation Script

This script validates goal data consistency between different NHL report sources
and identifies discrepancies that require attention.

Usage:
    python goal_data_validator.py <game_id>
    
Example:
    python goal_data_validator.py 2024020001
"""

import json
import sys
from pathlib import Path
from typing import Dict, List, Any, Optional

class GoalDataValidator:
    def __init__(self, storage_path: str = 'storage/20242025'):
        self.storage_path = Path(storage_path)
        self.discrepancies = []
        
    def validate_game(self, game_id: str) -> Dict[str, Any]:
        """Validate goal data for a specific game."""
        print(f"=== Goal Data Validation for Game {game_id} ===")
        
        # Load data from different sources
        gs_data = self._load_gs_data(game_id)
        th_data = self._load_th_data(game_id)
        tv_data = self._load_tv_data(game_id)
        
        # Extract goal information
        gs_goals = self._extract_gs_goals(gs_data)
        th_events = self._extract_th_events(th_data, 'home')
        tv_events = self._extract_th_events(tv_data, 'visitor')
        
        # Perform validation
        validation_results = {
            'game_id': game_id,
            'gs_goals': gs_goals,
            'th_events': th_events,
            'tv_events': tv_events,
            'discrepancies': [],
            'warnings': [],
            'recommendations': []
        }
        
        # Validate goal counts
        self._validate_goal_counts(validation_results)
        
        # Validate goal details
        self._validate_goal_details(validation_results)
        
        # Check for data quality issues
        self._check_data_quality(validation_results)
        
        return validation_results
    
    def _load_gs_data(self, game_id: str) -> Optional[Dict]:
        """Load Game Summary data."""
        gs_path = self.storage_path / 'json' / 'curate' / 'gs' / f'gs_{game_id}.json'
        if gs_path.exists():
            with open(gs_path, 'r') as f:
                return json.load(f)
        return None
    
    def _load_th_data(self, game_id: str) -> Optional[Dict]:
        """Load Time on Ice Home data."""
        th_path = self.storage_path / 'json' / 'curate' / 'th' / f'th_{game_id}.json'
        if th_path.exists():
            with open(th_path, 'r') as f:
                return json.load(f)
        return None
    
    def _load_tv_data(self, game_id: str) -> Optional[Dict]:
        """Load Time on Ice Visitor data."""
        tv_path = self.storage_path / 'json' / 'curate' / 'tv' / f'tv_{game_id}.json'
        if tv_path.exists():
            with open(tv_path, 'r') as f:
                return json.load(f)
        return None
    
    def _extract_gs_goals(self, gs_data: Optional[Dict]) -> List[Dict]:
        """Extract goal data from GS report."""
        if not gs_data:
            return []
        
        goals = gs_data.get('scoring_summary', {}).get('goals', [])
        return [
            {
                'goal_number': goal.get('goal_number'),
                'period': goal.get('period'),
                'time': goal.get('time'),
                'team': goal.get('team'),
                'scorer': goal.get('scorer', {}).get('name'),
                'scorer_sweater': goal.get('scorer', {}).get('sweater_number'),
                'strength': goal.get('strength')
            }
            for goal in goals
        ]
    
    def _extract_th_events(self, th_data: Optional[Dict], team_type: str) -> List[Dict]:
        """Extract 'G' events from TH/TV data."""
        if not th_data:
            return []
        
        events = []
        for player in th_data.get('player_time_on_ice', {}).get(team_type, []):
            for shift in player.get('shifts', []):
                if 'G' in shift.get('event', ''):
                    events.append({
                        'player': player.get('name'),
                        'sweater': player.get('sweater_number'),
                        'team': team_type,
                        'period': shift.get('period'),
                        'shift_number': shift.get('shift_number'),
                        'start_time': shift.get('start', {}).get('elapsed'),
                        'end_time': shift.get('end', {}).get('elapsed'),
                        'event': shift.get('event')
                    })
        return events
    
    def _validate_goal_counts(self, results: Dict[str, Any]):
        """Validate goal counts between sources."""
        gs_goals = results['gs_goals']
        th_events = results['th_events']
        tv_events = results['tv_events']
        
        # Count goals by team from GS
        gs_home_goals = len([g for g in gs_goals if g['team'] == 'BUF'])
        gs_visitor_goals = len([g for g in gs_goals if g['team'] == 'NJD'])
        
        # Count 'G' events from TH/TV
        th_g_events = len(th_events)
        tv_g_events = len(tv_events)
        
        # Check for discrepancies
        if th_g_events != gs_home_goals:
            results['discrepancies'].append({
                'type': 'goal_count_mismatch',
                'source': 'TH vs GS',
                'expected': gs_home_goals,
                'actual': th_g_events,
                'message': f"TH report has {th_g_events} 'G' events but GS shows {gs_home_goals} home goals"
            })
        
        if tv_g_events != gs_visitor_goals:
            results['discrepancies'].append({
                'type': 'goal_count_mismatch',
                'source': 'TV vs GS',
                'expected': gs_visitor_goals,
                'actual': tv_g_events,
                'message': f"TV report has {tv_g_events} 'G' events but GS shows {gs_visitor_goals} visitor goals"
            })
    
    def _validate_goal_details(self, results: Dict[str, Any]):
        """Validate goal details and timing."""
        gs_goals = results['gs_goals']
        th_events = results['th_events']
        tv_events = results['tv_events']
        
        # Check if goal scorers have corresponding 'G' events
        for goal in gs_goals:
            scorer_sweater = goal['scorer_sweater']
            team_type = 'home' if goal['team'] == 'BUF' else 'visitor'
            events = th_events if team_type == 'home' else tv_events
            
            # Find events for this scorer around the goal time
            scorer_events = [
                e for e in events 
                if e['sweater'] == scorer_sweater and e['period'] == goal['period']
            ]
            
            if not scorer_events:
                results['warnings'].append({
                    'type': 'missing_scorer_event',
                    'goal': goal,
                    'message': f"Goal scorer {goal['scorer']} #{scorer_sweater} has no 'G' events in TH/TV data"
                })
    
    def _check_data_quality(self, results: Dict[str, Any]):
        """Check for data quality issues."""
        th_events = results['th_events']
        tv_events = results['tv_events']
        
        # Check for excessive 'G' events (potential data quality issue)
        total_g_events = len(th_events) + len(tv_events)
        total_goals = len(results['gs_goals'])
        
        if total_g_events > total_goals * 3:  # More than 3x the actual goals
            results['warnings'].append({
                'type': 'excessive_g_events',
                'total_goals': total_goals,
                'total_g_events': total_g_events,
                'message': f"TH/TV reports have {total_g_events} 'G' events for only {total_goals} actual goals. This suggests 'G' events mark players on ice during goals, not goal scorers."
            })
        
        # Add recommendations
        if results['discrepancies'] or results['warnings']:
            results['recommendations'].append("Use GS reports as authoritative source for goal data")
            results['recommendations'].append("TH/TV 'G' events should be interpreted as 'players on ice during goals'")
            results['recommendations'].append("Implement cross-report validation in parser")
    
    def print_results(self, results: Dict[str, Any]):
        """Print validation results."""
        print(f"\n=== Validation Results for Game {results['game_id']} ===")
        
        # Summary
        print(f"GS Goals: {len(results['gs_goals'])}")
        print(f"TH 'G' Events: {len(results['th_events'])}")
        print(f"TV 'G' Events: {len(results['tv_events'])}")
        
        # Discrepancies
        if results['discrepancies']:
            print(f"\n=== DISCREPANCIES ({len(results['discrepancies'])}) ===")
            for disc in results['discrepancies']:
                print(f"  {disc['message']}")
        
        # Warnings
        if results['warnings']:
            print(f"\n=== WARNINGS ({len(results['warnings'])}) ===")
            for warning in results['warnings']:
                print(f"  {warning['message']}")
        
        # Recommendations
        if results['recommendations']:
            print(f"\n=== RECOMMENDATIONS ===")
            for rec in results['recommendations']:
                print(f"  • {rec}")
        
        # Goal details
        print(f"\n=== GOAL DETAILS ===")
        for goal in results['gs_goals']:
            print(f"  Goal {goal['goal_number']}: {goal['scorer']} #{goal['scorer_sweater']} ({goal['team']}) - Period {goal['period']}, {goal['time']} - {goal['strength']}")

def main():
    if len(sys.argv) != 2:
        print("Usage: python goal_data_validator.py <game_id>")
        print("Example: python goal_data_validator.py 2024020001")
        sys.exit(1)
    
    game_id = sys.argv[1]
    validator = GoalDataValidator()
    results = validator.validate_game(game_id)
    validator.print_results(results)
    
    # Save results
    output_path = Path(f'goal_validation_{game_id}.json')
    with open(output_path, 'w') as f:
        json.dump(results, f, indent=2)
    print(f"\nResults saved to: {output_path}")

if __name__ == '__main__':
    main()
