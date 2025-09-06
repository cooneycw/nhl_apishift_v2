#!/usr/bin/env python3
"""
Step 2: Collect HTML Reports from NHL.com
=========================================

This module handles the collection of HTML reports from NHL.com as outlined 
in the NHL API data structure documentation.
"""

import logging
import sys
import os
import json
from typing import Dict, List, Any, Optional
import time

# Add current directory to path for imports
sys.path.append(os.path.dirname(__file__))

from .html_collector import HTMLReportCollector
from ..utils.storage import CSVStorageManager
from config.nhl_config import create_default_config, NHLConfig


def collect_html_reports(config: NHLConfig, seasons: List[str], full_update: bool = False) -> Dict[str, Any]:
    """
    Collect HTML reports from NHL.com.
    
    Args:
        config: Enhanced configuration object
        seasons: List of season identifiers
        full_update: Whether this is a full update (reloads all reports)
        
    Returns:
        Dictionary containing collection results
    """
    logger = logging.getLogger('step_02_collect_html')
    logger.info("Starting HTML report collection from NHL.com...")
    
    # Initialize components
    html_collector = HTMLReportCollector(config)
    storage_manager = CSVStorageManager(config)
    
    results = {
        'step': 'step_02_collect_html',
        'seasons_requested': seasons,
        'full_update': full_update,
        'reports_collected': 0,
        'reports_failed': 0,
        'seasons_processed': [],
        'report_types': ['GS', 'ES', 'PL', 'FS', 'FC', 'RO', 'SS', 'TV', 'TH'],
        'execution_time': {},
        'success': True
    }
    
    total_start_time = time.time()
    
    try:
        for season in seasons:
            season_start_time = time.time()
            logger.info(f"Processing HTML reports for season {season}")
            
            season_results = {
                'season': season,
                'games_processed': 0,
                'reports_collected': 0,
                'reports_failed': 0,
                'report_details': []
            }
            
            try:
                # Get games for this season - this would need to be implemented
                # For now, we'll use a placeholder that gets games from collected JSON data
                season_games = _get_games_for_season(season, storage_manager)
                
                for game in season_games:
                    game_id = game.get('id') or game.get('game_id')
                    if not game_id:
                        continue
                        
                    season_results['games_processed'] += 1
                    
                    # Collect all report types for this game
                    # Use last 6 digits of game_id for HTML URLs (e.g., 2024020001 -> 020001)
                    game_id_short = f'{game_id:06d}'[-6:]
                    
                    for report_type in results['report_types']:
                        try:
                            report_data = html_collector.fetch_html_report(
                                season, report_type, game_id_short
                            )
                            
                            if report_data:
                                # Save the HTML report following the documented storage pattern
                                # storage/{season}/html/reports/{report_type}/{report_type}{game_id_short}.HTM
                                save_html_report(season, report_type, game_id_short, report_data)
                                
                                season_results['reports_collected'] += 1
                                results['reports_collected'] += 1
                                
                                season_results['report_details'].append({
                                    'game_id': game_id,
                                    'report_type': report_type,
                                    'success': True,
                                    'size_bytes': len(report_data.encode('utf-8')) if isinstance(report_data, str) else len(str(report_data))
                                })
                                
                            else:
                                season_results['reports_failed'] += 1
                                results['reports_failed'] += 1
                                
                                season_results['report_details'].append({
                                    'game_id': game_id,
                                    'report_type': report_type,
                                    'success': False,
                                    'error': 'No data returned'
                                })
                                
                        except Exception as e:
                            error_msg = f"Error collecting {report_type} report for game {game_id}: {e}"
                            logger.error(error_msg)
                            
                            season_results['reports_failed'] += 1
                            results['reports_failed'] += 1
                            
                            season_results['report_details'].append({
                                'game_id': game_id,
                                'report_type': report_type,
                                'success': False,
                                'error': str(e)
                            })
                
                season_execution_time = time.time() - season_start_time
                season_results['execution_time'] = season_execution_time
                results['execution_time'][season] = season_execution_time
                
                results['seasons_processed'].append(season_results)
                
                logger.info(f"Season {season} completed: {season_results['reports_collected']} reports collected, "
                           f"{season_results['reports_failed']} failed in {season_execution_time:.2f}s")
                
            except Exception as e:
                error_msg = f"Failed to process season {season}: {e}"
                logger.error(error_msg)
                
                season_results['error'] = str(e)
                season_results['success'] = False
                results['seasons_processed'].append(season_results)
                results['success'] = False
    
        total_execution_time = time.time() - total_start_time
        results['total_execution_time'] = total_execution_time
        
        # Log summary
        logger.info(f"HTML report collection completed in {total_execution_time:.2f}s")
        logger.info(f"Total: {results['reports_collected']} reports collected, {results['reports_failed']} failed")
        
        if not results['success']:
            logger.warning("Some HTML report collections failed. Check logs for details.")
            
    except Exception as e:
        logger.error(f"Error in HTML report collection: {e}")
        results['success'] = False
        results['error'] = str(e)
        raise
    
    return results


def save_html_report(season: str, report_type: str, game_id_short: str, content: str) -> bool:
    """
    Save HTML report to the correct storage location following the documented pattern.
    
    Args:
        season: Season identifier (e.g., '20242025')
        report_type: Report type code (e.g., 'GS', 'TV', 'TH')
        game_id_short: Short game ID (e.g., '020001')
        content: HTML content to save
        
    Returns:
        True if successful, False otherwise
    """
    try:
        # Create directory structure: storage/{season}/html/reports/{report_type}/
        report_dir = f"storage/{season}/html/reports/{report_type}"
        os.makedirs(report_dir, exist_ok=True)
        
        # Save file with .HTM extension following the pattern: {report_type}{game_id_short}.HTM
        filename = f"{report_type}{game_id_short}.HTM"
        filepath = os.path.join(report_dir, filename)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        
        return True
    except Exception as e:
        logger.error(f"Error saving {report_type} report: {e}")
        return False


def _get_games_for_season(season: str, storage_manager: CSVStorageManager) -> List[Dict[str, Any]]:
    """
    Get games for a specific season from collected data.
    
    Args:
        season: Season identifier
        storage_manager: Storage manager instance
        
    Returns:
        List of game dictionaries
    """
    try:
        # Load games from JSON file
        games_file = f"storage/{season}/json/games.json"
        if os.path.exists(games_file):
            with open(games_file, 'r', encoding='utf-8') as f:
                games_data = json.load(f)
                # Filter for regular season games only (gameType == 2)
                regular_games = [game for game in games_data if game.get('gameType') == 2]
                return regular_games
        else:
            logger.warning(f"Games file not found: {games_file}")
            return []
    except Exception as e:
        logger.error(f"Error loading games for season {season}: {e}")
        return []


def main():
    """Main entry point for step 2."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Step 2: Collect HTML reports from NHL.com")
    parser.add_argument('--seasons', nargs='+', help='Seasons to process')
    parser.add_argument('--full-update', action='store_true', help='Force full update')
    parser.add_argument('--verbose', action='store_true', help='Enable verbose logging')
    parser.add_argument('--report-types', nargs='+', 
                       choices=['GS', 'ES', 'PL', 'FS', 'FC', 'RO', 'SS', 'TV', 'TH'],
                       help='Specific report types to collect')
    
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Create configuration
    config_dict = create_default_config()
    config = NHLConfig(config_dict)
    
    # Use default seasons if none provided
    seasons = args.seasons or ['20242025']
    
    # Execute step
    result = collect_html_reports(config, seasons, args.full_update)
    
    # Print summary
    print(f"\nStep 2 Results:")
    print(f"  Success: {result['success']}")
    print(f"  Reports collected: {result['reports_collected']}")
    print(f"  Reports failed: {result['reports_failed']}")
    print(f"  Seasons processed: {len(result['seasons_processed'])}")
    print(f"  Total execution time: {result.get('total_execution_time', 0):.2f}s")
    
    if args.verbose and result['seasons_processed']:
        print("\nDetailed Results:")
        for season_result in result['seasons_processed']:
            print(f"  Season {season_result['season']}:")
            print(f"    Games processed: {season_result['games_processed']}")
            print(f"    Reports collected: {season_result['reports_collected']}")
            print(f"    Reports failed: {season_result['reports_failed']}")
    
    if not result['success']:
        sys.exit(1)


if __name__ == '__main__':
    main()
