#!/usr/bin/env python3
"""
Test the _is_legitimate_goal method with J.ZUCKER's assist data
"""

import sys
from pathlib import Path

# Add the src directory to the path
sys.path.append(str(Path(__file__).parent / "src"))

from parse.html_report_parser import HTMLReportParser
from config.nhl_config import NHLConfig

def test_legitimate_goal():
    """Test the _is_legitimate_goal method"""
    
    # Initialize the parser
    config = NHLConfig()
    parser = HTMLReportParser(config)
    
    # Test with J.ZUCKER's assist data
    assist1 = "72 T.THOMPSON(22)"
    assist2 = "26 R.DAHLIN(33)"
    
    print(f"Testing _is_legitimate_goal with:")
    print(f"  Assist1: '{assist1}'")
    print(f"  Assist2: '{assist2}'")
    
    is_legitimate = parser._is_legitimate_goal(assist1, assist2)
    print(f"  Result: {is_legitimate}")
    
    # Test with some other examples
    test_cases = [
        ("unassisted", ""),
        ("72 T.THOMPSON(22)", "26 R.DAHLIN(33)"),
        ("Unsuccessful Penalty Shot", ""),
        ("No Goal", ""),
        ("EMPTY NET", ""),
        ("", ""),
    ]
    
    print(f"\nTesting other cases:")
    for assist1_test, assist2_test in test_cases:
        result = parser._is_legitimate_goal(assist1_test, assist2_test)
        print(f"  '{assist1_test}' | '{assist2_test}' -> {result}")

if __name__ == "__main__":
    test_legitimate_goal()
