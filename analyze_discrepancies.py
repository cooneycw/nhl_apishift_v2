#!/usr/bin/env python3
"""
Analyze goal reconciliation discrepancies to find the missing cases for 100% reconciliation.
"""

import json
import sys
from pathlib import Path
from src.curate.player_team_goal_reconciliation import PlayerTeamGoalReconciliation

def analyze_discrepancies():
    """Analyze discrepancies from the reconciliation results."""
    
    # Initialize the reconciliation system
    system = PlayerTeamGoalReconciliation()
    
    # Get all game IDs from boxscores directory
    boxscore_dir = Path('storage/20242025/json/boxscores')
    game_files = list(boxscore_dir.glob('*.json'))
    
    print("=== ANALYZING DISCREPANCIES FOR 100% RECONCILIATION ===")
    print(f"Total games to analyze: {len(game_files)}")
    print()
    
    games_with_discrepancies = []
    total_discrepancies = 0
    
    for i, game_file in enumerate(game_files):
        game_id = game_file.stem
        
        if i % 100 == 0:
            print(f"Processing game {i+1}/{len(game_files)}: {game_id}")
        
        try:
            result = system.reconcile_game(game_id)
            if result:
                # Find minor discrepancies
                minor_discrepancies = [p for p in result.player_results if p.reconciliation_status == 'minor_discrepancy']
                
                if minor_discrepancies:
                    games_with_discrepancies.append({
                        'game_id': game_id,
                        'date': result.game_date,
                        'teams': f'{result.away_team} @ {result.home_team}',
                        'discrepancies': minor_discrepancies
                    })
                    total_discrepancies += len(minor_discrepancies)
        
        except Exception as e:
            print(f"Error processing game {game_id}: {e}")
    
    print(f"\n=== SUMMARY ===")
    print(f"Games with minor discrepancies: {len(games_with_discrepancies)}")
    print(f"Total minor discrepancies: {total_discrepancies}")
    print()
    
    # Show first 10 cases
    print("=== FIRST 10 DISCREPANCY CASES ===")
    for i, game in enumerate(games_with_discrepancies[:10]):
        print(f"\n--- CASE {i+1}: Game {game['game_id']} ---")
        print(f"Date: {game['date']}")
        print(f"Teams: {game['teams']}")
        print("Discrepancies:")
        
        for disc in game['discrepancies']:
            print(f"  {disc.player_name} #{disc.sweater_number} ({disc.team}):")
            print(f"    Goals: Auth={disc.authoritative_goals}, HTML={disc.html_goals} (Δ{disc.goal_discrepancy})")
            print(f"    Assists: Auth={disc.authoritative_assists}, HTML={disc.html_assists} (Δ{disc.assist_discrepancy})")
    
    return games_with_discrepancies

if __name__ == '__main__':
    discrepancies = analyze_discrepancies()
