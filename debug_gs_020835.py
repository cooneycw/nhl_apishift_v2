#!/usr/bin/env python3
"""
Debug script to investigate why J.ZUCKER's goal is not being parsed from GS020835.HTM
"""

import sys
from pathlib import Path
from bs4 import BeautifulSoup

# Add the src directory to the path
sys.path.append(str(Path(__file__).parent / "src"))

from parse.html_report_parser import HTMLReportParser
from config.nhl_config import NHLConfig

def debug_gs_parsing():
    """Debug the Game Summary parsing for GS020835.HTM"""
    
    # Initialize the parser
    config = NHLConfig()
    parser = HTMLReportParser(config)
    
    # Load the HTML file
    html_file = Path("storage/20242025/html/reports/GS/GS020835.HTM")
    with open(html_file, 'r', encoding='utf-8') as f:
        html_content = f.read()
    
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Parse the game summary data
    result = parser.parse_game_summary_data(soup, str(html_file))
    
    # Check the goals
    goals = result.get('scoring_summary', {}).get('goals', [])
    print(f"Total goals parsed: {len(goals)}")
    
    for i, goal in enumerate(goals, 1):
        scorer = goal.get('scorer', {})
        print(f"Goal {i}: {scorer.get('name', 'Unknown')} ({scorer.get('sweater_number', 'N/A')}) - Period {goal.get('period')} {goal.get('time')}")
    
    # Check if J.ZUCKER's goal is missing
    zucker_goals = [g for g in goals if g.get('scorer', {}).get('name') == 'J.ZUCKER']
    print(f"\nJ.ZUCKER goals found: {len(zucker_goals)}")
    
    if len(zucker_goals) == 0:
        print("❌ J.ZUCKER's goal is missing from the parsed data!")
        
        # Let's manually check the HTML table rows
        print("\n🔍 Manually checking HTML table rows...")
        table = soup.find('table', {'border': '0', 'cellpadding': '0', 'cellspacing': '0', 'width': '100%'})
        if table:
            rows = table.find_all('tr')
            print(f"Found {len(rows)} table rows")
            
            for i, row in enumerate(rows):
                cells = row.find_all('td')
                if len(cells) >= 6:  # Should have goal data
                    goal_num = cells[0].get_text(strip=True)
                    period = cells[1].get_text(strip=True)
                    time = cells[2].get_text(strip=True)
                    team = cells[4].get_text(strip=True)
                    scorer = cells[5].get_text(strip=True)
                    
                    if goal_num.isdigit():
                        print(f"Row {i}: Goal {goal_num}, Period {period}, Time {time}, Team {team}, Scorer {scorer}")
                        
                        if 'ZUCKER' in scorer:
                            print(f"  🎯 Found ZUCKER in row {i}!")
                            assist1 = cells[6].get_text(strip=True) if len(cells) > 6 else ''
                            assist2 = cells[7].get_text(strip=True) if len(cells) > 7 else ''
                            print(f"  Assist1: '{assist1}'")
                            print(f"  Assist2: '{assist2}'")
                            
                            # Test the _is_legitimate_goal method
                            is_legitimate = parser._is_legitimate_goal(assist1, assist2)
                            print(f"  Is legitimate goal: {is_legitimate}")
                            
                            # Try to extract the goal manually
                            print(f"  Attempting manual goal extraction...")
                            try:
                                goal_data = parser._extract_goal_from_row(cells)
                                if goal_data:
                                    print(f"  ✅ Manual extraction successful: {goal_data.get('scorer', {}).get('name')}")
                                else:
                                    print(f"  ❌ Manual extraction returned None")
                            except Exception as e:
                                print(f"  ❌ Manual extraction failed: {e}")
    else:
        print("✅ J.ZUCKER's goal was found in the parsed data!")

if __name__ == "__main__":
    debug_gs_parsing()
