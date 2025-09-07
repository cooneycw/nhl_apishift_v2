#!/usr/bin/env python3
"""
Simple debug script to check HTML table structure
"""

from pathlib import Path
from bs4 import BeautifulSoup

def debug_html_structure():
    """Debug the HTML table structure"""
    
    # Load the HTML file
    html_file = Path("storage/20242025/html/reports/GS/GS020835.HTM")
    with open(html_file, 'r', encoding='utf-8') as f:
        html_content = f.read()
    
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Find the scoring summary table
    tables = soup.find_all('table')
    print(f"Found {len(tables)} tables")
    
    for i, table in enumerate(tables):
        rows = table.find_all('tr')
        print(f"Table {i}: {len(rows)} rows")
        
        # Look for tables with goal data
        for j, row in enumerate(rows):
            cells = row.find_all('td')
            if len(cells) >= 6:
                first_cell = cells[0].get_text(strip=True)
                if first_cell.isdigit() and int(first_cell) <= 10:  # Likely a goal row
                    goal_num = first_cell
                    period = cells[1].get_text(strip=True)
                    time = cells[2].get_text(strip=True)
                    team = cells[4].get_text(strip=True)
                    scorer = cells[5].get_text(strip=True)
                    
                    print(f"  Table {i}, Row {j}: Goal {goal_num}, Period {period}, Time {time}, Team {team}, Scorer {scorer}")
                    
                    if 'ZUCKER' in scorer:
                        print(f"    🎯 FOUND ZUCKER!")
                        assist1 = cells[6].get_text(strip=True) if len(cells) > 6 else ''
                        assist2 = cells[7].get_text(strip=True) if len(cells) > 7 else ''
                        print(f"    Assist1: '{assist1}'")
                        print(f"    Assist2: '{assist2}'")

if __name__ == "__main__":
    debug_html_structure()
