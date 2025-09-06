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
import json
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any, Optional

# Import components from new structure
from config.nhl_config import NHLConfig, create_default_config
from src.collect.collect_json import NHLJSONCollector
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
        'step_03_curate',  # Now includes HTML parsing
        'step_04_validate',
        'step_05_transform',
        'step_06_export'
    ]
    
    # Define step dependencies
    STEP_DEPENDENCIES = {
        'step_01_collect_json': [],
        'step_02_collect_html': ['step_01_collect_json'],
        'step_03_curate': [],  # Can run independently if data files exist
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
        # Create configuration
        if config is None:
            self.config_dict = create_default_config()
        else:
            self.config_dict = config
        
        # Create NHL config
        self.config = NHLConfig(self.config_dict)
        
        # Setup logging
        self.logger = self._setup_logging()
        
        # Initialize components
        self.storage_manager = CSVStorageManager(self.config)
        self.data_collector = DataCollector(self.config)
        self.html_collector = HTMLReportCollector(self.config)
        self.validator = DataValidator(self.config)
        
        # Create storage directories
        self.config.create_storage_directories()
        
        # Get configuration values
        self.season_count = self.config.season_count
        self.default_season = self.config.default_season
        
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
    
    def get_available_seasons_from_storage(self) -> List[str]:
        """
        Get the list of seasons available in storage (from collected data).
        
        Returns:
            List of season identifiers found in storage
        """
        available_seasons = []
        storage_path = Path(self.config.storage_root)
        
        if storage_path.exists():
            # Look for season directories
            for item in storage_path.iterdir():
                if item.is_dir() and item.name.isdigit() and len(item.name) == 8:
                    # Check if it has data (JSON or HTML)
                    json_dir = item / "json"
                    html_dir = item / "html"
                    if json_dir.exists() or html_dir.exists():
                        available_seasons.append(item.name)
        
        # Sort seasons (most recent first)
        available_seasons.sort(reverse=True)
        return available_seasons
    
    def get_selected_seasons(self) -> List[str]:
        """
        Get the list of seasons to process.
        
        Returns:
            List of season identifiers (e.g., ['20242025', '20232024', ...])
        """
        # First try to get seasons from storage
        available_seasons = self.get_available_seasons_from_storage()
        
        if available_seasons:
            self.logger.info(f"Found {len(available_seasons)} seasons in storage: {', '.join(available_seasons)}")
            return available_seasons
        
        # Fallback to API if no seasons in storage
        try:
            seasons_data = self.data_collector.get_all_seasons()
            if seasons_data:
                seasons = [season.get('id') for season in seasons_data if season.get('id')]
                seasons.sort(reverse=True)
                selected_seasons = seasons[:self.season_count]
                self.logger.info(f"Selected {len(selected_seasons)} seasons from API: {', '.join(selected_seasons)}")
                return selected_seasons
        except Exception as e:
            self.logger.warning(f"Could not get seasons from API: {e}")
        
        # Final fallback to hardcoded recent seasons
        self.logger.warning("No seasons data available, using default seasons")
        current_year = datetime.now().year
        seasons = []
        for i in range(self.season_count):
            year = current_year - i
            season_id = f"{year}{year+1}"
            seasons.append(season_id)
        return seasons
    
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
        else:
            # Use the explicitly provided seasons
            self.current_seasons = seasons
            
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
        
        # Special case: Curation can run if data files exist, even without dependencies
        if step_name == 'step_03_curate':
            # Check if HTML files exist for any of the current seasons
            for season in self.current_seasons:
                html_dir = Path(self.config.storage_root) / season / "html" / "reports"
                if html_dir.exists() and list(html_dir.glob("GS*.HTM")):
                    return True  # HTML files exist, can run curation
        
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
        self.config.reload_gamecenter_landing = full_update
        self.config.reload_players = full_update
        self.config.reload_playernames = full_update
        self.config.reload_playbyplay = full_update
        self.config.reload_rosters = full_update
        
        try:
            # Collect JSON data using the data collector
            results = {}
            season = seasons[0]
            
            # Collect team data
            self.logger.info("Collecting team data...")
            teams = self.data_collector.collect_teams(season)
            results['teams'] = teams
            
            # Collect games data
            self.logger.info("Collecting games data...")
            games = self.data_collector.collect_games_for_season(season, teams)
            results['games'] = games
            
            # Collect player data from games
            self.logger.info("Collecting player data...")
            players = self.data_collector.collect_players_from_games(season, games)
            results['players'] = players
            
            # Collect game-level data (boxscores and play-by-play)
            if games:
                self.logger.info("Collecting boxscore data...")
                boxscore_count = self.data_collector.collect_boxscores_for_games(season, games)
                results['boxscores_collected'] = boxscore_count
                
                self.logger.info("Collecting play-by-play data...")
                pbp_count = self.data_collector.collect_playbyplay_for_games(season, games)
                results['playbyplay_collected'] = pbp_count
            else:
                self.logger.warning(f"No games data found for season {season}. Skipping game-level collection.")
            
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
                
                # Get games for this season from the JSON file
                from src.collect.collect_json import load_games_data
                season_games = load_games_data(season)
                
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
        Step 3: Curate data by parsing HTML reports and extracting comprehensive game data.
        
        Note: Shift chart data (SC) is curated during the JSON collection process (step_01_collect_json)
        and saved to storage/{season}/json/curate/sc/ following the established pattern.
        
        Args:
            seasons: List of season identifiers
            full_update: Whether this is a full update (reprocesses all data)
            
        Returns:
            Dictionary containing curation results
        """
        self.logger.info("Step 3: Curating data by parsing HTML reports...")
        
        results = {
            'seasons_processed': [],
            'games_processed': 0,
            'total_penalties_parsed': 0,
            'complex_scenarios_found': 0,
            'parsing_errors': []
        }
        
        try:
            # Import the HTML penalty parser
            from src.parse.html_report_parser import HTMLReportParser
            
            parser = HTMLReportParser(self.config)
            
            for season in seasons:
                self.logger.info(f"Processing HTML penalties for season {season}")
                season_results = {
                    'season': season,
                    'games_processed': 0,
                    'penalties_parsed': 0,
                    'complex_scenarios': 0,
                    'parsing_errors': []
                }
                
                # Get HTML directory for this season
                html_dir = Path(self.config.storage_root) / season / "html" / "reports"
                if not html_dir.exists():
                    self.logger.warning(f"HTML directory not found for season {season}")
                    continue
                
                # Determine which games to process
                game_ids = set()
                # If specific games were provided via CLI, honor them (strip season prefix if present)
                cli_games = getattr(self, 'cli_games', None)
                if cli_games:
                    for gid in cli_games:
                        gid_str = str(gid)
                        # Accept both 10-digit (e.g., 2024021036) and 6-digit forms (e.g., 021036)
                        if len(gid_str) == 10:
                            # Extract the 6-digit game number from the end
                            game_ids.add(gid_str[-6:])
                        elif len(gid_str) == 6:
                            game_ids.add(gid_str)
                else:
                    # Otherwise, get list of available games from HTML files
                    # Look in the GS subdirectory for Game Summary files
                    gs_dir = html_dir / "GS"
                    if gs_dir.exists():
                        html_files = list(gs_dir.glob("GS*.HTM"))
                        for html_file in html_files:
                            # Extract game ID from filename (GS020489.HTM -> 020489)
                            game_id = html_file.stem[2:]  # Remove 'GS' prefix
                            game_ids.add(game_id)
                
                for game_id in game_ids:
                    try:
                        self.logger.debug(f"Curating reports for game {game_id}")
                        games_processed_this_round = 0

                        # Parse the GS report (Game Summary)
                        gs_file = html_dir / 'GS' / f'GS{game_id}.HTM'
                        if gs_file.exists():
                            gs_data = parser.parse_report_data(gs_file, 'GS')
                            
                            # Save curated GS JSON under json/curate/gs
                            gs_out_dir = Path(self.config.storage_root) / season / 'json' / 'curate' / 'gs'
                            gs_out_dir.mkdir(parents=True, exist_ok=True)
                            gs_out_file = gs_out_dir / f'gs_{game_id}.json'
                            with open(gs_out_file, 'w') as f:
                                json.dump(gs_data, f, indent=2)
                            games_processed_this_round += 1
                        else:
                            self.logger.warning(f"GS report not found for game {game_id}")

                        # Parse the ES report (Event Summary)
                        es_file = html_dir / 'ES' / f'ES{game_id}.HTM'
                        if es_file.exists():
                            es_data = parser.parse_report_data(es_file, 'ES')
                            
                            # Save curated ES JSON under json/curate/es
                            es_out_dir = Path(self.config.storage_root) / season / 'json' / 'curate' / 'es'
                            es_out_dir.mkdir(parents=True, exist_ok=True)
                            es_out_file = es_out_dir / f'es_{game_id}.json'
                            with open(es_out_file, 'w') as f:
                                json.dump(es_data, f, indent=2)
                            games_processed_this_round += 1
                        else:
                            self.logger.warning(f"ES report not found for game {game_id}")

                        # Parse the RO report (Roster)
                        ro_file = html_dir / 'RO' / f'RO{game_id}.HTM'
                        if ro_file.exists():
                            ro_data = parser.parse_report_data(ro_file, 'RO')
                            
                            # Save curated RO JSON under json/curate/ro
                            ro_out_dir = Path(self.config.storage_root) / season / 'json' / 'curate' / 'ro'
                            ro_out_dir.mkdir(parents=True, exist_ok=True)
                            ro_out_file = ro_out_dir / f'ro_{game_id}.json'
                            with open(ro_out_file, 'w') as f:
                                json.dump(ro_data, f, indent=2)
                            games_processed_this_round += 1
                        else:
                            self.logger.warning(f"RO report not found for game {game_id}")

                        # Parse the FS report (Faceoff Summary)
                        fs_file = html_dir / 'FS' / f'FS{game_id}.HTM'
                        if fs_file.exists():
                            fs_data = parser.parse_report_data(fs_file, 'FS')
                            
                            # Save curated FS JSON under json/curate/fs
                            fs_out_dir = Path(self.config.storage_root) / season / 'json' / 'curate' / 'fs'
                            fs_out_dir.mkdir(parents=True, exist_ok=True)
                            fs_out_file = fs_out_dir / f'fs_{game_id}.json'
                            with open(fs_out_file, 'w') as f:
                                json.dump(fs_data, f, indent=2)
                            games_processed_this_round += 1
                        else:
                            self.logger.warning(f"FS report not found for game {game_id}")

                        # Parse the TH report (Time on Ice - Home)
                        th_file = html_dir / 'TH' / f'TH{game_id}.HTM'
                        if th_file.exists():
                            th_data = parser.parse_report_data(th_file, 'TH')
                            
                            # Save curated TH JSON under json/curate/th (folder lower-case), filename mirrors HTM base
                            th_out_dir = Path(self.config.storage_root) / season / 'json' / 'curate' / 'th'
                            th_out_dir.mkdir(parents=True, exist_ok=True)
                            th_out_file = th_out_dir / f'th_{game_id}.json'
                            with open(th_out_file, 'w') as f:
                                json.dump(th_data, f, indent=2)
                            games_processed_this_round += 1
                        else:
                            self.logger.warning(f"TH report not found for game {game_id}")

                        # Parse the TV report (Time on Ice - Away)
                        tv_file = html_dir / 'TV' / f'TV{game_id}.HTM'
                        if tv_file.exists():
                            tv_data = parser.parse_report_data(tv_file, 'TV')
                            
                            # Save curated TV JSON under json/curate/tv (folder lower-case)
                            tv_out_dir = Path(self.config.storage_root) / season / 'json' / 'curate' / 'tv'
                            tv_out_dir.mkdir(parents=True, exist_ok=True)
                            tv_out_file = tv_out_dir / f'tv_{game_id}.json'
                            with open(tv_out_file, 'w') as f:
                                json.dump(tv_data, f, indent=2)
                            games_processed_this_round += 1
                        else:
                            self.logger.warning(f"TV report not found for game {game_id}")

                        if games_processed_this_round > 0:
                            season_results['games_processed'] += 1
                            results['games_processed'] += 1
                        
                    except Exception as e:
                        error_msg = f"Error parsing reports for game {game_id}: {e}"
                        season_results['parsing_errors'].append(error_msg)
                        results['parsing_errors'].append(error_msg)
                        self.logger.error(error_msg)
                
                results['seasons_processed'].append(season_results)
                self.logger.info(f"Season {season}: {season_results['games_processed']} games curated (GS, ES, RO, and FS reports)")
        
            self.logger.info(f"Step 3: HTML report curation completed. Total games curated: {results['games_processed']}")
            
        except Exception as e:
            self.logger.error(f"Error in step_03_curate: {e}")
            raise
            
        return results
    
    def detect_complex_penalty_scenarios(self, penalties: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Detect complex penalty scenarios from penalty data.
        
        Args:
            penalties: List of penalty dictionaries
            
        Returns:
            List of detected complex scenarios
        """
        scenarios = []
        
        try:
            # 1. Simultaneous penalties (same time, multiple teams)
            time_groups = {}
            for penalty in penalties:
                time = penalty.get('time', '')
                if time not in time_groups:
                    time_groups[time] = []
                time_groups[time].append(penalty)
            
            for time, time_penalties in time_groups.items():
                if len(time_penalties) > 1:
                    teams = set(p.get('team', '') for p in time_penalties)
                    if len(teams) > 1:
                        scenarios.append({
                            'type': 'simultaneous_penalties',
                            'time': time,
                            'penalties': time_penalties,
                            'description': f'Multiple penalties at {time} to different teams',
                            'impact': '4-on-4 even strength (no power play)'
                        })
                    else:
                        scenarios.append({
                            'type': 'multiple_team_penalties',
                            'time': time,
                            'penalties': time_penalties,
                            'description': f'Multiple penalties at {time} to same team',
                            'impact': 'Extended power play for opponent'
                        })
            
            # 2. Team penalties (no specific player)
            team_penalties = [p for p in penalties if not p.get('player') or p.get('penalty_type') == 'BEN']
            if team_penalties:
                scenarios.append({
                    'type': 'team_penalties',
                    'penalties': team_penalties,
                    'description': 'Penalties without specific player assignment',
                    'impact': 'Penalty served by designated player, affects team statistics'
                })
            
            # 3. Non-power play penalties
            non_pp_penalties = [p for p in penalties if not p.get('is_power_play', True)]
            if non_pp_penalties:
                scenarios.append({
                    'type': 'non_power_play_penalties',
                    'penalties': non_pp_penalties,
                    'description': 'Penalties that do not lead to power plays',
                    'impact': 'No numerical advantage, different statistical treatment'
                })
                
        except Exception as e:
            self.logger.error(f"Error detecting complex penalty scenarios: {e}")
        
        return scenarios
    
    def step_04_validate(self, seasons: List[str], full_update: bool = False) -> Dict[str, Any]:
        """
        Step 4: Validate curated data for quality and consistency.
        
        Args:
            seasons: List of season identifiers
            full_update: Whether this is a full update (revalidates all data)
            
        Returns:
            Dictionary containing validation results
        """
        self.logger.info("Step 4: Validating curated data...")
        
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

    # Optional: restrict to specific game IDs when curating
    parser.add_argument(
        '--games',
        nargs='+',
        help='Optional list of game IDs to process (e.g., 2024021036). Applies to curate/validate steps.'
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
        'update_game_statuses': True,
        
        # Current working collectors
        'collect_json': True,  # Includes boxscores, playbyplay, shift charts
        'collect_html': False,  # Not implemented yet
        'curate': False,        # Not implemented yet
        
        # Shift charts specific config
        'shift_charts': {
            'enabled': True,
            'rate_limit_delay': 1.0,
            'max_retries': 3
        }
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
            # Handle season selection for step mode
            if args.seasons:
                # Use explicitly provided seasons
                selected_seasons = args.seasons
            else:
                # Auto-detect available seasons from storage
                available_seasons = system.get_available_seasons_from_storage()
                
                if not available_seasons:
                    print("No seasons found in storage. Please specify seasons with --seasons or collect data first.")
                    sys.exit(1)
                elif len(available_seasons) == 1:
                    selected_seasons = available_seasons
                    print(f"Found 1 season in storage: {available_seasons[0]}")
                    print(f"Using season: {available_seasons[0]}")
                else:
                    print(f"Found {len(available_seasons)} seasons in storage:")
                    for i, season in enumerate(available_seasons, 1):
                        print(f"  {i}. {season}")
                    
                    while True:
                        try:
                            choice = input(f"\nSelect season (1-{len(available_seasons)}) or 'all' for all seasons: ").strip()
                            if choice.lower() == 'all':
                                selected_seasons = available_seasons
                                break
                            else:
                                choice_num = int(choice)
                                if 1 <= choice_num <= len(available_seasons):
                                    selected_seasons = [available_seasons[choice_num - 1]]
                                    break
                                else:
                                    print(f"Please enter a number between 1 and {len(available_seasons)}")
                        except ValueError:
                            print("Please enter a valid number or 'all'")
                        except KeyboardInterrupt:
                            print("\nOperation cancelled")
                            sys.exit(0)
            
            # Execute specific step(s)
            if args.steps:
                # Execute multiple steps in sequence
                for step in args.steps:
                    result = system.execute_step(step, selected_seasons, full_update=False)
                    print(f"Step {step} completed successfully")
                    if args.verbose:
                        print(f"  Result: {result}")
            elif args.step:
                # Pass CLI games into system for steps that support game filtering
                if args.games:
                    system.cli_games = args.games
                # Execute single step
                result = system.execute_step(args.step, selected_seasons, full_update=False)
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
