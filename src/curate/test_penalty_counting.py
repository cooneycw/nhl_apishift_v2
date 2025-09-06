#!/usr/bin/env python3
"""
Test penalty counting across all data sources for a specific game.
"""

import json
import sys
from pathlib import Path

def count_penalties_in_boxscore(boxscore_file):
    """Count penalties in boxscore data."""
    try:
        with open(boxscore_file, 'r') as f:
            data = json.load(f)
        
        total_pim = 0
        players_with_penalties = 0
        
        # Count PIM from player stats
        for team in ['homeTeam', 'awayTeam']:
            if team in data:
                team_data = data[team]
                if 'players' in team_data:
                    for player in team_data['players']:
                        pim = player.get('pim', 0)
                        if pim > 0:
                            total_pim += pim
                            players_with_penalties += 1
                            print(f"  Player {player.get('name', 'Unknown')} (#{player.get('sweaterNumber', '?')}): {pim} PIM")
        
        return total_pim, players_with_penalties
    except Exception as e:
        print(f"Error reading boxscore: {e}")
        return 0, 0

def count_penalties_in_curated_gs(gs_file):
    """Count penalties in curated GS data."""
    try:
        with open(gs_file, 'r') as f:
            data = json.load(f)
        
        total_penalties = 0
        total_pim = 0
        
        if 'penalties' in data and 'all_penalties' in data['penalties']:
            for penalty in data['penalties']['all_penalties']:
                total_penalties += 1
                pim = penalty.get('pim', 0)
                total_pim += pim
                player = penalty.get('player', {})
                print(f"  Penalty #{penalty.get('penalty_number', '?')} - {player.get('name', 'Unknown')} (#{player.get('sweater_number', '?')}): {penalty.get('penalty_type', 'Unknown')} - {pim} PIM")
        
        return total_penalties, total_pim
    except Exception as e:
        print(f"Error reading curated GS: {e}")
        return 0, 0

def main():
    game_id = "2024020122"
    season = "20242025"
    
    print(f"=== Penalty Counting Analysis for Game {game_id} ===")
    print()
    
    # Check boxscore data
    boxscore_file = Path(f"storage/{season}/json/boxscores/{game_id}.json")
    if boxscore_file.exists():
        print("📊 BOXSCORE DATA:")
        total_pim, players_with_penalties = count_penalties_in_boxscore(boxscore_file)
        print(f"  Total PIM: {total_pim}")
        print(f"  Players with penalties: {players_with_penalties}")
        print()
    else:
        print("❌ Boxscore file not found")
        print()
    
    # Check curated GS data
    gs_file = Path(f"storage/{season}/json/curate/gs/gs_{game_id[-6:]}.json")
    if gs_file.exists():
        print("📋 CURATED GS DATA:")
        total_penalties, total_pim = count_penalties_in_curated_gs(gs_file)
        print(f"  Total penalties: {total_penalties}")
        print(f"  Total PIM: {total_pim}")
        print()
    else:
        print("❌ Curated GS file not found")
        print()
    
    # Check if we have playbyplay data
    playbyplay_file = Path(f"storage/{season}/json/playbyplay/{game_id}.json")
    if playbyplay_file.exists():
        print("🎬 PLAYBYPLAY DATA:")
        try:
            with open(playbyplay_file, 'r') as f:
                data = json.load(f)
            
            penalty_events = 0
            if 'plays' in data:
                for play in data['plays']:
                    if play.get('typeDescKey') == 'penalty':
                        penalty_events += 1
                        print(f"  Penalty event: {play.get('details', {}).get('description', 'Unknown')}")
            
            print(f"  Total penalty events: {penalty_events}")
        except Exception as e:
            print(f"  Error reading playbyplay: {e}")
        print()
    else:
        print("❌ Playbyplay file not found")
        print()

if __name__ == "__main__":
    main()
