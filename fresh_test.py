#!/usr/bin/env python3
"""
Fresh test of the _is_legitimate_goal method
"""

import sys
import re
from pathlib import Path

# Add the src directory to the path
sys.path.append(str(Path(__file__).parent / "src"))

# Import the specific method directly
from parse.html_report_parser import HTMLReportParser
from config.nhl_config import NHLConfig

def test_fresh():
    """Fresh test of the method"""
    
    # Create a new parser instance
    config = NHLConfig()
    parser = HTMLReportParser(config)
    
    # Test the method directly
    assist1 = "72 T.THOMPSON(22)"
    assist2 = "26 R.DAHLIN(33)"
    
    print(f"Testing with fresh parser instance:")
    print(f"  Assist1: '{assist1}'")
    print(f"  Assist2: '{assist2}'")
    
    # Call the method directly
    result = parser._is_legitimate_goal(assist1, assist2)
    print(f"  Result: {result}")
    
    # Let's also test the regex directly
    combined_text = f"{assist1} {assist2}".strip()
    pattern = r'^[\d\s,\.A-Z()]+$'
    regex_match = re.match(pattern, combined_text)
    print(f"  Direct regex test: {bool(regex_match)}")
    
    # Test the method step by step
    print(f"\nStep-by-step test:")
    print(f"  Combined text: '{combined_text}'")
    
    # Check exception codes
    exception_codes = [
        "Unsuccessful Penalty Shot",
        "No Goal", 
        "Missed",
        "Failed",
        "Penalty Shot",
        "PS"
    ]
    
    has_exception = False
    for exception in exception_codes:
        if exception.lower() in combined_text.lower():
            print(f"  Found exception code: '{exception}'")
            has_exception = True
            break
    
    if not has_exception:
        print(f"  No exception codes found")
        
        # Check legitimate indicators
        legitimate_indicators = ["unassisted", "EMPTY NET"]
        has_legitimate = False
        for indicator in legitimate_indicators:
            if indicator.lower() in combined_text.lower():
                print(f"  Found legitimate indicator: '{indicator}'")
                has_legitimate = True
                break
        
        if not has_legitimate:
            print(f"  No legitimate indicators found")
            
            # Check regex
            if not combined_text or re.match(r'^[\d\s,\.A-Z()]+$', combined_text):
                print(f"  Regex pattern matches - should return True")
            else:
                print(f"  Regex pattern does not match - should return False")

if __name__ == "__main__":
    test_fresh()
