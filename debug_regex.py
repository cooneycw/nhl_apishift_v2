#!/usr/bin/env python3
"""
Debug the regex pattern issue
"""

import re

def debug_regex():
    """Debug the regex pattern"""
    
    assist1 = "72 T.THOMPSON(22)"
    assist2 = "26 R.DAHLIN(33)"
    combined_text = f"{assist1} {assist2}".strip()
    
    print(f"Assist1: '{assist1}'")
    print(f"Assist2: '{assist2}'")
    print(f"Combined: '{combined_text}'")
    print(f"Length: {len(combined_text)}")
    
    # Test the original regex
    pattern_old = r'^[\d\s,\.A-Z]+$'
    match_old = re.match(pattern_old, combined_text)
    print(f"Old pattern '{pattern_old}' matches: {bool(match_old)}")
    
    # Test the new regex
    pattern_new = r'^[\d\s,\.A-Z()]+$'
    match_new = re.match(pattern_new, combined_text)
    print(f"New pattern '{pattern_new}' matches: {bool(match_new)}")
    
    # Let's see what characters are in the string
    print(f"Characters in combined text:")
    for i, char in enumerate(combined_text):
        print(f"  {i}: '{char}' (ord: {ord(char)})")
    
    # Test with a simpler pattern
    pattern_simple = r'^[\d\s,\.A-Z()]+$'
    match_simple = re.match(pattern_simple, combined_text)
    print(f"Simple pattern matches: {bool(match_simple)}")
    
    # Test if it's just the parentheses
    test_without_parens = combined_text.replace('(', '').replace(')', '')
    print(f"Without parentheses: '{test_without_parens}'")
    match_no_parens = re.match(r'^[\d\s,\.A-Z]+$', test_without_parens)
    print(f"Old pattern without parentheses matches: {bool(match_no_parens)}")

if __name__ == "__main__":
    debug_regex()
