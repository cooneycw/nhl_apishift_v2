#!/usr/bin/env python3
"""
Demonstration of the NHL Data Reconciliation Review System
=========================================================
Shows how the system works and what it can do.
"""

import json
import sys
from pathlib import Path

# Add src to path
sys.path.append('src')

from reconciliation_review import ReconciliationReviewer

def demo_reconciliation_review():
    """Demonstrate the reconciliation review system."""
    print("NHL Data Reconciliation Review System - Demonstration")
    print("=" * 70)
    
    # Initialize the reviewer
    reviewer = ReconciliationReviewer()
    
    # Find an available game
    season = "20242025"
    available_games = reviewer.find_available_games(season)
    
    if not available_games:
        print("No games found for analysis.")
        return
    
    # Use the first available game
    game_id = available_games[0]
    print(f"Analyzing game: {game_id}")
    print(f"Season: {season}")
    print("-" * 70)
    
    # Perform reconciliation analysis
    print("1. Loading and analyzing data sources...")
    analysis = reviewer.analyze_game_reconciliation(season, game_id)
    
    if 'error' in analysis:
        print(f"Error during analysis: {analysis['error']}")
        return
    
    # Display summary
    print("\n2. Analysis Summary:")
    print(f"   Data sources available: {len(analysis.get('data_sources', {}))}")
    
    penalty_analysis = analysis.get('penalty_analysis', {})
    if penalty_analysis:
        counts = penalty_analysis.get('penalty_counts', {})
        print(f"   Penalty counts by source: {counts}")
        
        quality = penalty_analysis.get('data_quality', {})
        if quality:
            score = quality.get('overall_score', 0)
            print(f"   Data quality score: {score:.1f}%")
    
    # Check for issues
    issues = analysis.get('reconciliation_issues', [])
    if issues:
        print(f"   Reconciliation issues found: {len(issues)}")
        for i, issue in enumerate(issues[:3], 1):  # Show first 3
            print(f"     {i}. {issue['type']}: {issue['description']}")
    else:
        print("   No reconciliation issues found")
    
    # Generate report
    print("\n3. Generating detailed report...")
    report = reviewer.generate_review_report(analysis)
    
    # Save report
    output_dir = Path("reconciliation_reviews")
    output_dir.mkdir(exist_ok=True)
    
    report_file = output_dir / f"demo_report_{game_id}.txt"
    with open(report_file, 'w') as f:
        f.write(report)
    
    print(f"   Report saved to: {report_file}")
    
    # Show what the interactive review would look like
    print("\n4. Interactive Review Interface Preview:")
    print("   The interactive interface would allow you to:")
    print("   - Review each discrepancy in detail")
    print("   - Add context notes explaining differences")
    print("   - Review complex penalty scenarios")
    print("   - Assess data quality issues")
    print("   - Generate enhanced reports with your insights")
    
    # Show sample context notes that could be added
    print("\n5. Sample Context Notes You Could Add:")
    print("   - 'Penalty was offsetting, resulting in 4-on-4 play'")
    print("   - 'Team penalty served by designated player'")
    print("   - 'HTML reports have more detailed penalty descriptions'")
    print("   - 'Gamecenter Landing has most accurate timing'")
    
    # Show recommendations
    recommendations = analysis.get('recommendations', [])
    if recommendations:
        print(f"\n6. System Recommendations ({len(recommendations)} generated):")
        for i, rec in enumerate(recommendations[:3], 1):  # Show first 3
            print(f"   {i}. {rec['action']}")
            print(f"      Priority: {rec['priority']}")
            if 'impact' in rec:
                print(f"      Impact: {rec['impact']}")
            if 'description' in rec:
                print(f"      Description: {rec['description']}")
    
    print("\n" + "=" * 70)
    print("DEMONSTRATION COMPLETED")
    print("=" * 70)
    print(f"To start an interactive review session, run:")
    print(f"  python interactive_review.py")
    print(f"\nTo process multiple games for pattern analysis, run:")
    print(f"  python batch_reconciliation_review.py")
    print(f"\nFor detailed analysis of this game, see:")
    print(f"  {report_file}")

def show_available_games():
    """Show what games are available for analysis."""
    print("\nAvailable Games for Analysis:")
    print("-" * 50)
    
    reviewer = ReconciliationReviewer()
    season = "20242025"
    games = reviewer.find_available_games(season)
    
    if not games:
        print("No games found.")
        return
    
    print(f"Found {len(games)} games in season {season}")
    print("\nFirst 10 games:")
    for i, game_id in enumerate(games[:10], 1):
        print(f"  {i:2d}. {game_id}")
    
    if len(games) > 10:
        print(f"  ... and {len(games) - 10} more")
    
    print(f"\nTo analyze a specific game, use:")
    print(f"  python interactive_review.py")
    print(f"  Then enter season: {season}")
    print(f"  And game ID from the list above")

if __name__ == "__main__":
    print("NHL Data Reconciliation Review System")
    print("Demonstration and Information")
    print("=" * 70)
    
    # Show available games
    show_available_games()
    
    # Run demonstration
    print("\n" + "=" * 70)
    demo_reconciliation_review()
