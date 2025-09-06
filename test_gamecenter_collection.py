#!/usr/bin/env python3
"""
Test script for NHL Gamecenter Landing data collection
=====================================================

This script tests the new gamecenter landing endpoint collection functionality.
"""

import json
import os
import sys
from pathlib import Path

# Add the project root to the path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config.nhl_config import NHLConfig, create_default_config
from src.collect.collect_json import NHLJSONCollector

def test_gamecenter_landing_collection():
    """Test the gamecenter landing collection functionality."""
    print("🧪 Testing NHL Gamecenter Landing Collection")
    print("=" * 60)
    
    # Create configuration
    config_dict = create_default_config()
    config_dict.update({
        'verbose': True,
        'max_workers': 2,
        'default_season': '20232024'
    })
    
    config = NHLConfig(config_dict)
    collector = NHLJSONCollector(config)
    
    # Test game ID from the example URL
    test_game_id = 2023020204
    test_season = '20232024'
    
    print(f"🎯 Testing collection for game {test_game_id} in season {test_season}")
    
    # Test the endpoint URL construction
    endpoint_url = config.get_endpoint("gamecenter_landing", game_id=test_game_id)
    print(f"🔗 Endpoint URL: {endpoint_url}")
    
    # Test the file path construction
    file_path = config.get_gamecenter_landing_file_path(test_season, test_game_id)
    print(f"📁 File path: {file_path}")
    
    # Test the collection
    print(f"\n📊 Attempting to collect gamecenter landing data...")
    success = collector.collect_gamecenter_landing(test_game_id, test_season)
    
    if success:
        print(f"✅ Successfully collected gamecenter landing data!")
        
        # Verify the file was created
        if os.path.exists(file_path):
            print(f"✅ File created successfully at: {file_path}")
            
            # Read and display some basic info about the collected data
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                print(f"\n📋 Collected Data Summary:")
                print(f"   Game ID: {data.get('id', 'N/A')}")
                print(f"   Season: {data.get('season', 'N/A')}")
                print(f"   Game Date: {data.get('gameDate', 'N/A')}")
                print(f"   Away Team: {data.get('awayTeam', {}).get('commonName', {}).get('default', 'N/A')}")
                print(f"   Home Team: {data.get('homeTeam', {}).get('commonName', {}).get('default', 'N/A')}")
                print(f"   Game State: {data.get('gameState', 'N/A')}")
                print(f"   Period: {data.get('periodDescriptor', {}).get('number', 'N/A')}")
                
                # Check for key data structures
                if 'summary' in data:
                    summary = data['summary']
                    if 'scoring' in summary:
                        print(f"   Scoring Periods: {len(summary['scoring'])}")
                    if 'penalties' in summary:
                        print(f"   Penalty Periods: {len(summary['penalties'])}")
                    if 'threeStars' in summary:
                        print(f"   Three Stars: {len(summary['threeStars'])}")
                
            except Exception as e:
                print(f"❌ Error reading collected data: {e}")
        else:
            print(f"❌ File was not created at expected path")
    else:
        print(f"❌ Failed to collect gamecenter landing data")
    
    print(f"\n🎉 Test completed!")

def test_configuration():
    """Test the configuration setup for gamecenter landing."""
    print("\n🔧 Testing Configuration Setup")
    print("=" * 40)
    
    config_dict = create_default_config()
    config = NHLConfig(config_dict)
    
    # Test endpoints
    print(f"✅ Base URL: {config.base_url}")
    print(f"✅ Gamecenter Landing Endpoint: {config.endpoints.get('gamecenter_landing', 'NOT FOUND')}")
    
    # Test file paths
    print(f"✅ Gamecenter Landing File Path: {config.file_paths.get('gamecenter_landing', 'NOT FOUND')}")
    
    # Test method
    test_path = config.get_gamecenter_landing_file_path('20232024', 2023020204)
    print(f"✅ Generated File Path: {test_path}")
    
    print("✅ Configuration test passed!")

if __name__ == "__main__":
    print("🚀 Starting NHL Gamecenter Landing Collection Tests")
    print("=" * 60)
    
    try:
        # Test configuration first
        test_configuration()
        
        # Test collection functionality
        test_gamecenter_landing_collection()
        
        print("\n🎉 All tests completed successfully!")
        
    except Exception as e:
        print(f"\n❌ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
