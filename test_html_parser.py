#!/usr/bin/env python3
"""
Test script for HTML Penalty Parser
Tests the comprehensive HTML parsing functionality with existing HTML files.
"""

import sys
from pathlib import Path

# Add src to path
sys.path.append('src')

from parse.html_penalty_parser import HTMLPenaltyParser

def test_html_parser():
    """Test the HTML penalty parser with existing data."""
    print("Testing HTML Penalty Parser...")
    
    # Initialize parser
    parser = HTMLPenaltyParser()
    
    # Test with existing HTML files
    html_dir = Path("storage/20242025/html/reports")
    
    if not html_dir.exists():
        print(f"HTML directory not found: {html_dir}")
        return
    
    # Find a game with HTML files
    html_files = list(html_dir.glob("GS*.HTM"))
    if not html_files:
        print("No HTML files found")
        return
    
    # Test with first available game
    test_file = html_files[0]
    game_id = test_file.stem[2:]  # Remove 'GS' prefix
    print(f"Testing with game: {game_id}")
    
    try:
        # Test comprehensive parsing
        print("\n1. Testing comprehensive game data parsing...")
        game_data = parser.parse_game_data("20242025", game_id, html_dir)
        
        print(f"   Reports parsed: {game_data['parsing_metadata']['reports_parsed']}")
        print(f"   Total records: {game_data['parsing_metadata']['total_records_found']}")
        
        if 'consolidated_data' in game_data:
            print(f"   Penalties found: {len(game_data['consolidated_data'].get('penalties', []))}")
            print(f"   Game summary data: {'Yes' if game_data['consolidated_data'].get('game_summary') else 'No'}")
            print(f"   Roster data: {'Yes' if game_data['consolidated_data'].get('roster') else 'No'}")
        
        # Test penalty-specific parsing
        print("\n2. Testing penalty-specific parsing...")
        penalties = parser.parse_game_penalties("20242025", game_id, html_dir)
        
        print(f"   Penalties found: {len(penalties['consolidated_penalties'])}")
        print(f"   Complex scenarios: {len(penalties['complex_scenarios'])}")
        
        # Show some penalty details
        if penalties['consolidated_penalties']:
            print("\n   Sample penalties:")
            for i, penalty in enumerate(penalties['consolidated_penalties'][:3]):
                print(f"     {i+1}. {penalty['time']} - {penalty['team']} - {penalty['description']}")
                if penalty.get('penalty_minutes_served'):
                    pms = penalty['penalty_minutes_served']
                    if pms.get('is_team_penalty') and pms.get('serving_player_identified'):
                        print(f"        Served by: {pms['player_name']}")
        
        # Show complex scenarios
        if penalties['complex_scenarios']:
            print("\n   Complex scenarios found:")
            for scenario in penalties['complex_scenarios']:
                print(f"     - {scenario['type']}: {scenario['description']}")
        
        # Test individual report parsing
        print("\n3. Testing individual report parsing...")
        for report_type in ['GS', 'PL', 'ES']:
            html_file = html_dir / f"{report_type}{game_id}.HTM"
            if html_file.exists():
                try:
                    report_data = parser.parse_report_data(html_file, report_type)
                    print(f"   {report_type}: {'Success' if 'error' not in report_data else 'Error'}")
                except Exception as e:
                    print(f"   {report_type}: Error - {e}")
            else:
                print(f"   {report_type}: File not found")
        
        print("\nHTML Parser test completed successfully!")
        
    except Exception as e:
        print(f"Error testing HTML parser: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_html_parser()

