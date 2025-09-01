#!/usr/bin/env python3
"""
Enhanced NHL Data Retrieval System - Step-Based Processing
==========================================================

This system provides comprehensive data collection and storage for NHL statistics
and supplementary datasets using a step-based approach for modular execution.

Processing Steps:
- step_01_collect_json: Collect JSON data from NHL API endpoints
- step_02_collect_html: Collect HTML reports from NHL.com
- step_03_curate: Process and curate collected data
- step_04_validate: Validate data integrity and quality
- step_05_transform: Transform data for analysis (optional)
- step_06_export: Export data to various formats (optional)

Features:
- Modular step-based execution
- Full and incremental data updates
- Support for all NHL API endpoints
- HTML report integration
- CSV-based storage for human readability
- Comprehensive error handling and logging
- Parallel processing for efficiency
- Data validation and integrity checks
"""

import argparse
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any, Optional

# Import components from new structure
from config.nhl_config import NHLConfig, create_default_config
from config.config import EnhancedConfig
from src.collect.data_collector import DataCollector
from src.collect.html_collector import HTMLReportCollector
from src.utils.storage import CSVStorageManager
from src.utils.validator import DataValidator


class NHLDataRetrievalSystem:
    """
    Enhanced NHL Data Retrieval System with Step-Based Processing.
    
    This system provides comprehensive data collection, storage, and validation
    for all NHL datasets outlined in the API documentation using a modular
    step-based approach.
    """
    
    # Define processing steps
    PROCESSING_STEPS = [
        'step_01_collect_json',
        'step_02_collect_html', 
        'step_03_curate',
        'step_04_validate',
        'step_05_transform',
        'step_06_export'
    ]
    
    # Define step dependencies
    STEP_DEPENDENCIES = {
        'step_01_collect_json': [],
        'step_02_collect_html': ['step_01_collect_json'],
        'step_03_curate': ['step_01_collect_json'],
        'step_04_validate': ['step_03_curate'],
        'step_05_transform': ['step_04_validate'],
        'step_06_export': ['step_05_transform']
    }
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize the NHL Data Retrieval System.
        
        Args:
            config: Configuration dictionary with system parameters, or None for default
        """
        # Create enhanced configuration
        if config is None:
            self.enhanced_config = create_default_config()
        else:
            self.enhanced_config = EnhancedConfig(config)
        
        # Create NHLapiV3 compatible config
        nhlapi_config_dict = self.enhanced_config.get_nhlapi_config_dict()
        self.config = NHLConfig(nhlapi_config_dict)
        
        # Setup logging
        self.logger = self._setup_logging()
        
        # Initialize components
        self.storage_manager = CSVStorageManager(self.enhanced_config)
        self.data_collector = DataCollector(self.enhanced_config)
        self.html_collector = HTMLReportCollector(self.enhanced_config)
        self.validator = DataValidator(self.enhanced_config)
        
        # Create storage directories
        self.enhanced_config.create_storage_directories()
        
        # Get configuration values
        self.season_count = self.enhanced_config.season_count
        self.default_season = self.enhanced_config.default_season
        
        # Track completed steps for dependency management
        self.completed_steps = set()
        self.step_results = {}
        self.current_seasons = []
        
    def _setup_logging(self) -> logging.Logger:
        """Set up comprehensive logging for the system."""
        logger = logging.getLogger('NHLDataRetrieval')
        logger.setLevel(logging.INFO)
        
        # Create handlers
        console_handler = logging.StreamHandler(sys.stdout)
        file_handler = logging.FileHandler(f'nhl_retrieval_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
        
        # Create formatters and add it to handlers
        log_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(log_format)
        file_handler.setFormatter(log_format)
        
        # Add handlers to the logger
        logger.addHandler(console_handler)
        logger.addHandler(file_handler)
        
        return logger
    
    def get_selected_seasons(self) -> List[str]:
        """
        Get the list of seasons to process.
        
        Returns:
            List of season identifiers (e.g., ['20242025', '20232024', ...])
        """
        # Get all available seasons
        seasons_data = self.data_collector.get_all_seasons()
        
        if not seasons_data:
            self.logger.warning("No seasons data available, using default seasons")
            # Fallback to hardcoded recent seasons
            current_year = datetime.now().year
            seasons = []
            for i in range(self.season_count):
                year = current_year - i
                season_id = f"{year}{year+1}"
                seasons.append(season_id)
            return seasons
        
        # Sort seasons by ID (newest first) and take the most recent ones
        sorted_seasons = sorted(seasons_data, key=lambda x: x.get('id', 0), reverse=True)
        selected_seasons = [str(season['id']) for season in sorted_seasons[:self.season_count]]
        
        self.logger.info(f"Selected {len(selected_seasons)} seasons: {', '.join(selected_seasons)}")
        return selected_seasons
    
    def full_update(self, seasons: Optional[List[str]] = None) -> None:
        """
        Perform a full update of all data for specified seasons using step-based processing.
        
        Args:
            seasons: List of season identifiers. If None, uses default selection.
        """
        if seasons is None:
            seasons = self.get_selected_seasons()
            
        self.current_seasons = seasons
        self.logger.info(f"Starting full update for {len(seasons)} seasons: {', '.join(seasons)}")
        
        # Execute all steps in sequence
        for step in self.PROCESSING_STEPS:
            try:
                self.execute_step(step, seasons, full_update=True)
            except Exception as e:
                self.logger.error(f"Error in {step}: {e}")
                raise
        
        self.logger.info("Full update completed successfully")
    
    def incremental_update(self, seasons: Optional[List[str]] = None) -> None:
        """
        Perform an incremental update of data for specified seasons using step-based processing.
        
        Args:
            seasons: List of season identifiers. If None, uses default selection.
        """
        if seasons is None:
            seasons = self.get_selected_seasons()
            
        self.current_seasons = seasons
        self.logger.info(f"Starting incremental update for {len(seasons)} seasons: {', '.join(seasons)}")
        
        # Execute core steps (skip optional transform and export for incremental)
        core_steps = ['step_01_collect_json', 'step_02_collect_html', 'step_03_curate', 'step_04_validate']
        
        for step in core_steps:
            try:
                self.execute_step(step, seasons, full_update=False)
            except Exception as e:
                self.logger.error(f"Error in {step}: {e}")
                raise
        
        self.logger.info("Incremental update completed successfully")
    
    def execute_step(self, step_name: str, seasons: Optional[List[str]] = None, full_update: bool = False) -> Dict[str, Any]:
        """
        Execute a specific processing step.
        
        Args:
            step_name: Name of the step to execute
            seasons: List of season identifiers
            full_update: Whether this is a full update
            
        Returns:
            Dictionary containing step results
        """
        if seasons is None:
            seasons = self.current_seasons or self.get_selected_seasons()
            
        # Check dependencies
        if not self._check_step_dependencies(step_name):
            missing_deps = [dep for dep in self.STEP_DEPENDENCIES[step_name] if dep not in self.completed_steps]
            raise ValueError(f"Step {step_name} missing dependencies: {missing_deps}")
        
        self.logger.info(f"Executing {step_name} for seasons: {', '.join(seasons)}")
        
        # Execute the specific step
        step_method = getattr(self, step_name)
        result = step_method(seasons, full_update=full_update)
        
        # Mark step as completed and store results
        self.completed_steps.add(step_name)
        self.step_results[step_name] = result
        
        self.logger.info(f"Completed {step_name}")
        return result
    
    def _check_step_dependencies(self, step_name: str) -> bool:
        """
        Check if all dependencies for a step are satisfied.
        
        Args:
            step_name: Name of the step to check
            
        Returns:
            True if all dependencies are satisfied
        """
        required_deps = self.STEP_DEPENDENCIES.get(step_name, [])
        return all(dep in self.completed_steps for dep in required_deps)
    
    def step_01_collect_json(self, seasons: List[str], full_update: bool = False) -> Dict[str, Any]:
        """
        Step 1: Collect JSON data from NHL API endpoints.
        
        Args:
            seasons: List of season identifiers
            full_update: Whether this is a full update (reloads all data)
            
        Returns:
            Dictionary containing collection results
        """
        self.logger.info("Step 1: Collecting JSON data from NHL API endpoints...")
        
        # Update configuration for this operation
        self.config.reload_seasons = full_update
        self.config.reload_teams = full_update
        self.config.reload_games = full_update
        self.config.reload_boxscores = full_update
        self.config.reload_players = full_update
        self.config.reload_playernames = full_update
        self.config.reload_playbyplay = full_update
        self.config.reload_rosters = full_update
        
        results = {}
        
        try:
            # Use existing collection functions from NHLapiV3
            results['seasons'] = get_season_data(self.config)
            results['teams'] = get_team_list(self.config)
            results['games'] = get_game_list(self.config)
            results['boxscores'] = get_boxscore_list(self.config)
            results['player_names'] = get_player_names(self.config)
            results['playbyplay'] = get_playbyplay_data(self.config)
            
            # Store JSON data in CSV format for human readability
            self.storage_manager.save_json_data_as_csv(results, seasons)
            
            self.logger.info("Step 1: JSON data collection completed successfully")
            
        except Exception as e:
            self.logger.error(f"Error in step_01_collect_json: {e}")
            raise
            
        return results
    
    def step_02_collect_html(self, seasons: List[str], full_update: bool = False) -> Dict[str, Any]:
        """
        Step 2: Collect HTML reports from NHL.com.
        
        Args:
            seasons: List of season identifiers
            full_update: Whether this is a full update (reloads all reports)
            
        Returns:
            Dictionary containing collection results
        """
        self.logger.info("Step 2: Collecting HTML reports from NHL.com...")
        
        results = {
            'reports_collected': 0,
            'reports_failed': 0,
            'seasons_processed': [],
            'report_types': ['GS', 'ES', 'PL', 'FS', 'FC', 'RO', 'SS', 'TO']
        }
        
        try:
            for season in seasons:
                self.logger.info(f"Processing HTML reports for season {season}")
                season_results = {
                    'season': season,
                    'games_processed': 0,
                    'reports_collected': 0,
                    'reports_failed': 0
                }
                
                # Get games for this season
                season_games = self.data_collector.get_games_for_season(season)
                
                for game in season_games:
                    game_id = game['id']
                    season_results['games_processed'] += 1
                    
                    # Collect all report types
                    for report_type in results['report_types']:
                        try:
                            report_data = self.html_collector.fetch_html_report(
                                season, report_type, game_id
                            )
                            
                            if report_data:
                                self.storage_manager.save_html_report(
                                    season, report_type, game_id, report_data
                                )
                                season_results['reports_collected'] += 1
                                results['reports_collected'] += 1
                            else:
                                season_results['reports_failed'] += 1
                                results['reports_failed'] += 1
                                
                        except Exception as e:
                            self.logger.error(f"Error collecting {report_type} report for game {game_id}: {e}")
                            season_results['reports_failed'] += 1
                            results['reports_failed'] += 1
                
                results['seasons_processed'].append(season_results)
                self.logger.info(f"Season {season}: {season_results['reports_collected']} reports collected, {season_results['reports_failed']} failed")
        
            self.logger.info(f"Step 2: HTML report collection completed. Total: {results['reports_collected']} collected, {results['reports_failed']} failed")
            
        except Exception as e:
            self.logger.error(f"Error in step_02_collect_html: {e}")
            raise
            
        return results
    
    def step_03_curate(self, seasons: List[str], full_update: bool = False) -> Dict[str, Any]:
        """
        Step 3: Curate and process collected data.
        
        Args:
            seasons: List of season identifiers
            full_update: Whether this is a full update (reprocesses all data)
            
        Returns:
            Dictionary containing curation results
        """
        self.logger.info("Step 3: Curating and processing collected data...")
        
        results = {
            'seasons_processed': [],
            'curation_success': True,
            'errors': []
        }
        
        try:
            # Update configuration for curation
            self.config.reload_curate = full_update
            
            # Use existing curation function from NHLapiV3
            curate_data(self.config)
            
            # Process each season
            for season in seasons:
                season_results = {
                    'season': season,
                    'curation_complete': False,
                    'data_processed': {}
                }
                
                try:
                    # Curate season-specific data
                    season_data = self.storage_manager.curate_season_data(season)
                    season_results['data_processed'] = season_data
                    season_results['curation_complete'] = True
                    
                    self.logger.info(f"Curation completed for season {season}")
                    
                except Exception as e:
                    error_msg = f"Curation failed for season {season}: {e}"
                    self.logger.error(error_msg)
                    results['errors'].append(error_msg)
                    results['curation_success'] = False
                
                results['seasons_processed'].append(season_results)
            
            self.logger.info("Step 3: Data curation completed")
            
        except Exception as e:
            self.logger.error(f"Error in step_03_curate: {e}")
            raise
            
        return results
    
    def step_04_validate(self, seasons: List[str], full_update: bool = False) -> Dict[str, Any]:
        """
        Step 4: Validate data integrity and quality.
        
        Args:
            seasons: List of season identifiers
            full_update: Whether this is a full update
            
        Returns:
            Dictionary containing validation results
        """
        self.logger.info("Step 4: Validating data integrity and quality...")
        
        results = {
            'validation_passed': True,
            'seasons_validated': [],
            'total_errors': 0,
            'total_warnings': 0
        }
        
        try:
            for season in seasons:
                self.logger.info(f"Validating data for season {season}")
                
                season_results = {
                    'season': season,
                    'validation_passed': True,
                    'errors': [],
                    'warnings': [],
                    'data_quality_score': 0.0
                }
                
                try:
                    validation_results = self.validator.validate_season_data(season)
                    
                    season_results['errors'] = validation_results.get('errors', [])
                    season_results['warnings'] = validation_results.get('warnings', [])
                    season_results['data_quality_score'] = validation_results.get('quality_score', 0.0)
                    
                    if season_results['errors']:
                        season_results['validation_passed'] = False
                        results['validation_passed'] = False
                        self.logger.warning(f"Validation errors found for season {season}: {season_results['errors']}")
                    else:
                        self.logger.info(f"Season {season} data validation passed")
                    
                    results['total_errors'] += len(season_results['errors'])
                    results['total_warnings'] += len(season_results['warnings'])
                    
                except Exception as e:
                    error_msg = f"Validation failed for season {season}: {e}"
                    season_results['errors'].append(error_msg)
                    season_results['validation_passed'] = False
                    results['validation_passed'] = False
                    self.logger.error(error_msg)
                
                results['seasons_validated'].append(season_results)
            
            self.logger.info(f"Step 4: Validation completed. Errors: {results['total_errors']}, Warnings: {results['total_warnings']}")
            
        except Exception as e:
            self.logger.error(f"Error in step_04_validate: {e}")
            raise
            
        return results
    
    def step_05_transform(self, seasons: List[str], full_update: bool = False) -> Dict[str, Any]:
        """
        Step 5: Transform data for analysis (optional).
        
        Args:
            seasons: List of season identifiers
            full_update: Whether this is a full update
            
        Returns:
            Dictionary containing transformation results
        """
        self.logger.info("Step 5: Transforming data for analysis...")
        
        results = {
            'transformation_success': True,
            'seasons_transformed': [],
            'transformations_applied': []
        }
        
        try:
            # Apply data transformations for analysis
            transformations = [
                'normalize_player_statistics',
                'calculate_advanced_metrics',
                'create_aggregated_views',
                'generate_time_series_data'
            ]
            
            for transformation in transformations:
                try:
                    self.logger.info(f"Applying transformation: {transformation}")
                    # Transform data using storage manager
                    transform_result = self.storage_manager.apply_transformation(transformation, seasons)
                    results['transformations_applied'].append({
                        'transformation': transformation,
                        'success': True,
                        'result': transform_result
                    })
                except Exception as e:
                    self.logger.error(f"Transformation {transformation} failed: {e}")
                    results['transformation_success'] = False
                    results['transformations_applied'].append({
                        'transformation': transformation,
                        'success': False,
                        'error': str(e)
                    })
            
            results['seasons_transformed'] = seasons
            self.logger.info("Step 5: Data transformation completed")
            
        except Exception as e:
            self.logger.error(f"Error in step_05_transform: {e}")
            raise
            
        return results
    
    def step_06_export(self, seasons: List[str], full_update: bool = False) -> Dict[str, Any]:
        """
        Step 6: Export data to various formats (optional).
        
        Args:
            seasons: List of season identifiers
            full_update: Whether this is a full update
            
        Returns:
            Dictionary containing export results
        """
        self.logger.info("Step 6: Exporting data to various formats...")
        
        results = {
            'export_success': True,
            'formats_exported': [],
            'summary_reports_generated': []
        }
        
        try:
            # Export to different formats
            export_formats = ['json', 'parquet', 'excel']
            
            for format_type in export_formats:
                try:
                    self.logger.info(f"Exporting to {format_type} format")
                    export_result = self.storage_manager.export_data(seasons, format_type)
                    results['formats_exported'].append({
                        'format': format_type,
                        'success': True,
                        'files_created': export_result.get('files_created', [])
                    })
                except Exception as e:
                    self.logger.error(f"Export to {format_type} failed: {e}")
                    results['export_success'] = False
                    results['formats_exported'].append({
                        'format': format_type,
                        'success': False,
                        'error': str(e)
                    })
            
            # Generate summary reports
            report_type = "full" if full_update else "incremental"
            
            for season in seasons:
                try:
                    summary = self.storage_manager.generate_season_summary(season)
                    self.storage_manager.save_summary_report(season, summary, report_type)
                    results['summary_reports_generated'].append({
                        'season': season,
                        'success': True,
                        'report_type': report_type
                    })
                    self.logger.info(f"Generated {report_type} summary report for season {season}")
                except Exception as e:
                    self.logger.error(f"Error generating summary for season {season}: {e}")
                    results['summary_reports_generated'].append({
                        'season': season,
                        'success': False,
                        'error': str(e)
                    })
            
            # Generate overall system summary
            try:
                system_summary = self.storage_manager.generate_system_summary(seasons)
                self.storage_manager.save_system_summary(system_summary, report_type)
                self.logger.info(f"Generated {report_type} system summary report")
            except Exception as e:
                self.logger.error(f"Error generating system summary: {e}")
                results['export_success'] = False
            
            self.logger.info("Step 6: Data export completed")
            
        except Exception as e:
            self.logger.error(f"Error in step_06_export: {e}")
            raise
            
        return results
    
    def get_data_status(self, seasons: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Get the current status of data for specified seasons.
        
        Args:
            seasons: List of season identifiers. If None, uses default selection.
            
        Returns:
            Dictionary containing data status information
        """
        if seasons is None:
            seasons = self.get_selected_seasons()
        
        status = {
            'seasons': seasons,
            'data_status': {},
            'last_updated': {},
            'completeness': {}
        }
        
        for season in seasons:
            season_status = self.storage_manager.get_season_status(season)
            status['data_status'][season] = season_status['available_datasets']
            status['last_updated'][season] = season_status['last_updated']
            status['completeness'][season] = season_status['completeness_percentage']
        
        return status
    
    def cleanup_old_data(self, keep_seasons: int = 10) -> None:
        """
        Clean up old data, keeping only the specified number of most recent seasons.
        
        Args:
            keep_seasons: Number of most recent seasons to keep
        """
        self.logger.info(f"Cleaning up old data, keeping {keep_seasons} most recent seasons")
        
        all_seasons = self.data_collector.get_all_seasons()
        if not all_seasons:
            self.logger.warning("No seasons data available for cleanup")
            return
        
        # Sort seasons and identify old ones
        sorted_seasons = sorted(all_seasons, key=lambda x: x.get('id', 0), reverse=True)
        seasons_to_keep = [str(season['id']) for season in sorted_seasons[:keep_seasons]]
        seasons_to_remove = [str(season['id']) for season in sorted_seasons[keep_seasons:]]
        
        if not seasons_to_remove:
            self.logger.info("No old data to clean up")
            return
        
        self.logger.info(f"Removing data for {len(seasons_to_remove)} old seasons: {', '.join(seasons_to_remove)}")
        
        for season in seasons_to_remove:
            try:
                self.storage_manager.remove_season_data(season)
                self.logger.info(f"Removed data for season {season}")
            except Exception as e:
                self.logger.error(f"Error removing data for season {season}: {e}")


def main():
    """Main entry point for the NHL Data Retrieval System."""
    parser = argparse.ArgumentParser(
        description="NHL Data Retrieval System - Comprehensive data collection and storage"
    )
    
    parser.add_argument(
        '--mode',
        choices=['full', 'incremental', 'status', 'cleanup', 'step'],
        default='incremental',
        help='Operation mode (default: incremental)'
    )
    
    parser.add_argument(
        '--step',
        choices=['step_01_collect_json', 'step_02_collect_html', 'step_03_curate', 
                'step_04_validate', 'step_05_transform', 'step_06_export'],
        help='Specific step to execute (only used with --mode step)'
    )
    
    parser.add_argument(
        '--steps',
        nargs='+',
        choices=['step_01_collect_json', 'step_02_collect_html', 'step_03_curate', 
                'step_04_validate', 'step_05_transform', 'step_06_export'],
        help='Multiple specific steps to execute in sequence (only used with --mode step)'
    )
    
    parser.add_argument(
        '--seasons',
        nargs='+',
        help='Specific seasons to process (e.g., 20242025 20232024)'
    )
    
    parser.add_argument(
        '--season-count',
        type=int,
        default=10,
        help='Number of most recent seasons to process (default: 10)'
    )
    
    parser.add_argument(
        '--default-season',
        default='20242025',
        help='Default season when no seasons specified (default: 20242025)'
    )
    
    parser.add_argument(
        '--keep-seasons',
        type=int,
        default=10,
        help='Number of seasons to keep during cleanup (default: 10)'
    )
    
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )
    
    parser.add_argument(
        '--max-workers',
        type=int,
        default=28,
        help='Maximum number of parallel workers (default: 28)'
    )
    
    args = parser.parse_args()
    
    # Build configuration
    config = {
        'verbose': args.verbose,
        'produce_csv': True,  # Always produce CSV for human readability
        'season_count': args.season_count,
        'default_season': args.default_season,
        'max_workers': args.max_workers,
        'delete_files': False,
        'reload_seasons': False,
        'reload_teams': False,
        'reload_games': False,
        'update_game_statuses': True,
        'reload_boxscores': False,
        'reload_players': False,
        'reload_playernames': False,
        'reload_playbyplay': False,
        'reload_rosters': False,
        'reload_curate': False,
    }
    
    # Initialize system
    system = NHLDataRetrievalSystem(config)
    
    # Set logging level
    if args.verbose:
        system.logger.setLevel(logging.DEBUG)
    
    try:
        if args.mode == 'full':
            system.full_update(args.seasons)
        elif args.mode == 'incremental':
            system.incremental_update(args.seasons)
        elif args.mode == 'step':
            # Execute specific step(s)
            if args.steps:
                # Execute multiple steps in sequence
                for step in args.steps:
                    result = system.execute_step(step, args.seasons, full_update=False)
                    print(f"Step {step} completed successfully")
                    if args.verbose:
                        print(f"  Result: {result}")
            elif args.step:
                # Execute single step
                result = system.execute_step(args.step, args.seasons, full_update=False)
                print(f"Step {args.step} completed successfully")
                if args.verbose:
                    print(f"  Result: {result}")
            else:
                print("Error: --step or --steps must be specified when using --mode step")
                sys.exit(1)
        elif args.mode == 'status':
            status = system.get_data_status(args.seasons)
            print("Data Status:")
            for season, data_status in status['data_status'].items():
                print(f"  Season {season}:")
                print(f"    Available datasets: {', '.join(data_status)}")
                print(f"    Last updated: {status['last_updated'][season]}")
                print(f"    Completeness: {status['completeness'][season]:.1f}%")
            
            # Show completed steps if any
            if system.completed_steps:
                print("\nCompleted Steps:")
                for step in system.completed_steps:
                    print(f"  ✓ {step}")
        elif args.mode == 'cleanup':
            system.cleanup_old_data(args.keep_seasons)
    
    except KeyboardInterrupt:
        system.logger.info("Operation cancelled by user")
        sys.exit(1)
    except Exception as e:
        system.logger.error(f"Operation failed: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
