#!/usr/bin/env python3
"""
Collect TOI Reports (TV/TH)
===========================

This script collects Time on Ice reports using the corrected TV/TH codes
instead of the non-existent TO codes.
"""

import json
import os
import sys
import time
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime

# Add the project root to the path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.collect.html_collector import HTMLReportCollector
from config.nhl_config import create_default_config
from config.config import EnhancedConfig

def load_games_data(season: str) -> List[Dict[str, Any]]:
    """Load games data for a season."""
    games_file = f"storage/{season}/json/games.json"
    if os.path.exists(games_file):
        with open(games_file, 'r') as f:
            return json.load(f)
    return []

def save_html_report(season: str, report_type: str, game_id: str, content: str) -> bool:
    """Save HTML report to the correct storage location."""
    try:
        # Create directory structure: storage/{season}/html/reports/{report_type}/
        report_dir = f"storage/{season}/html/reports/{report_type}"
        os.makedirs(report_dir, exist_ok=True)
        
        # Save file with .HTM extension (as NHL provides them)
        filename = f"{report_type}{str(game_id)[-6:]}.HTM"
        filepath = os.path.join(report_dir, filename)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        
        return True
    except Exception as e:
        print(f"Error saving {report_type} report: {e}")
        return False

def main():
    """Main TOI collection function."""
    print("📊 NHL TOI Reports Collection (TV/TH)")
    print("=" * 60)
    print("Collecting Time on Ice reports using corrected TV/TH codes")
    print("Storing as HTML files in storage/{season}/html/reports/")
    print("=" * 60)
    
    season = "20242025"
    
    # Load games data
    games = load_games_data(season)
    if not games:
        print(f"❌ No games data found for season {season}")
        return
    
    # Filter for regular season games
    regular_games = [game for game in games if game.get('gameType') == 2]
    print(f"📊 Found {len(regular_games)} regular season games")
    
    # Create configuration and collector
    config_dict = create_default_config()
    config_dict.update({
        'verbose': True,
        'max_workers': 2,
        'default_season': season
    })
    
    config = EnhancedConfig(config_dict)
    collector = HTMLReportCollector(config)
    
    # Create storage directories
    config.create_storage_directories()
    
    print(f"🔄 Starting TOI report collection...")
    print(f"📈 Expected total TOI reports: {len(regular_games) * 2} (TV + TH for each game)")
    
    # Test with a few games first
    print(f"\n🧪 Testing with first 5 games...")
    test_games = regular_games[:5]
    
    successful_tests = 0
    total_tests = 0
    
    for i, game in enumerate(test_games, 1):
        game_id = game['id']
        print(f"\n📊 Testing game {i}/{len(test_games)}: {game_id}")
        
        # Test TV report (Away team TOI)
        tv_content = collector.fetch_html_report(season, 'TV', f'{game_id:06d}'[-6:])
        total_tests += 1
        if tv_content:
            # Save the HTML report
            if save_html_report(season, 'TV', game_id, tv_content):
                print(f"  ✅ TV report: {len(tv_content)} bytes (saved)")
                successful_tests += 1
            else:
                print(f"  ❌ TV report: {len(tv_content)} bytes (save failed)")
        else:
            print(f"  ❌ TV report failed")
        
        # Test TH report (Home team TOI)
        th_content = collector.fetch_html_report(season, 'TH', f'{game_id:06d}'[-6:])
        total_tests += 1
        if th_content:
            # Save the HTML report
            if save_html_report(season, 'TH', game_id, th_content):
                print(f"  ✅ TH report: {len(th_content)} bytes (saved)")
                successful_tests += 1
            else:
                print(f"  ❌ TH report: {len(th_content)} bytes (save failed)")
        else:
            print(f"  ❌ TH report failed")
    
    print(f"\n{'='*60}")
    print("🧪 TEST COMPLETE")
    print(f"{'='*60}")
    
    success_rate = round((successful_tests / total_tests) * 100, 1) if total_tests > 0 else 0
    print(f"📊 Test Results:")
    print(f"   Successful tests: {successful_tests}/{total_tests}")
    print(f"   Success rate: {success_rate}%")
    
    if successful_tests == total_tests:
        print(f"\n🎉 Test successful! All TOI reports are accessible.")
        print(f"💡 Ready to collect all {len(regular_games)} games.")
        
        # Continue with all games
        print(f"\n🔄 Continuing with all games...")
        
        # Collect TOI reports for each game
        total_reports = 0
        successful_reports = 0
        failed_reports = 0
        failed_games = []
        
        for i, game in enumerate(regular_games, 1):
            game_id = game['id']
            print(f"\n📊 Processing game {i}/{len(regular_games)}: {game_id}")
            
            game_success = True
            
            # Collect TV report (Away team TOI)
            tv_content = collector.fetch_html_report(season, 'TV', f'{game_id:06d}'[-6:])
            total_reports += 1
            if tv_content:
                if save_html_report(season, 'TV', game_id, tv_content):
                    successful_reports += 1
                    print(f"  ✅ TV report: {len(tv_content)} bytes (saved)")
                else:
                    failed_reports += 1
                    game_success = False
                    print(f"  ❌ TV report: {len(tv_content)} bytes (save failed)")
            else:
                failed_reports += 1
                game_success = False
                print(f"  ❌ TV report failed")
            
            # Collect TH report (Home team TOI)
            th_content = collector.fetch_html_report(season, 'TH', f'{game_id:06d}'[-6:])
            total_reports += 1
            if th_content:
                if save_html_report(season, 'TH', game_id, th_content):
                    successful_reports += 1
                    print(f"  ✅ TH report: {len(th_content)} bytes (saved)")
                else:
                    failed_reports += 1
                    game_success = False
                    print(f"  ❌ TH report: {len(th_content)} bytes (save failed)")
            else:
                failed_reports += 1
                game_success = False
                print(f"  ❌ TH report failed")
            
            if not game_success:
                failed_games.append(game_id)
            
            # Show progress every 25 games
            if i % 25 == 0 or i == len(regular_games):
                success_rate = round((successful_reports / total_reports) * 100, 1) if total_reports > 0 else 0
                print(f"\n📈 Progress: {i}/{len(regular_games)} games processed")
                print(f"   TOI reports: {successful_reports}/{total_reports} ({success_rate}% success)")
        
        # Final summary
        print(f"\n{'='*60}")
        print("🎯 TOI REPORTS COLLECTION SUMMARY")
        print(f"{'='*60}")
        
        final_success_rate = round((successful_reports / total_reports) * 100, 1) if total_reports > 0 else 0
        
        print(f"📊 Total games processed: {len(regular_games)}")
        print(f"📊 Total TOI reports attempted: {total_reports}")
        print(f"✅ Successful TOI reports: {successful_reports}")
        print(f"❌ Failed TOI reports: {failed_reports}")
        print(f"📈 TOI report success rate: {final_success_rate}%")
        
        if failed_reports > 0:
            print(f"\n⚠️  Failed games:")
            for failed_game in failed_games[:10]:  # Show first 10
                print(f"   - Game {failed_game}")
            if len(failed_games) > 10:
                print(f"   ... and {len(failed_games) - 10} more")
        
        # Save results
        results_file = f"toi_reports_collection_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        results_data = {
            'season': season,
            'total_games': len(regular_games),
            'total_reports': total_reports,
            'successful_reports': successful_reports,
            'failed_reports': failed_reports,
            'success_rate': final_success_rate,
            'failed_games': failed_games
        }
        
        with open(results_file, 'w') as f:
            json.dump(results_data, f, indent=2)
        
        print(f"\n💾 Results saved to: {results_file}")
        
        if failed_reports == 0:
            print(f"\n🎉 SUCCESS! All TOI reports collected successfully!")
        else:
            print(f"\n💡 Consider running the collection again for failed reports")
    
    else:
        print(f"\n⚠️  Some test TOI reports failed. Check the errors above.")
        print(f"💡 You may want to investigate before collecting all games.")

if __name__ == "__main__":
    main()
