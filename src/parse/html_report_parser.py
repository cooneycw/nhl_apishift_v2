#!/usr/bin/env python3
"""
HTML Penalty Parser for NHL Data Reconciliation
===============================================

This module provides comprehensive penalty data extraction from NHL HTML reports
to achieve 100% reconciliation with JSON data sources. It handles complex penalty
scenarios including simultaneous penalties, team penalties, and non-power play penalties.

Key Features:
- Multi-report type parsing (GS, PL, ES)
- Complex penalty scenario detection
- Event code and situation code extraction
- Power play calculation validation
- Team penalty linking
- High-accuracy penalty data extraction
"""

import re
import logging
from typing import Dict, List, Any, Optional, Tuple
from bs4 import BeautifulSoup
from datetime import datetime
from pathlib import Path
import json
import sys

# Import reference data loader
sys.path.append(str(Path(__file__).parent.parent))
from utils.reference_data import ReferenceDataLoader


class HTMLReportParser:
    """
    Comprehensive HTML report parser for NHL data reconciliation.
    
    This parser extracts structured data from multiple HTML report types (GS, RO, PL, ES, SS, FS, FC, TH, TV)
    to achieve 100% reconciliation with JSON data sources. It handles complex scenarios including
    simultaneous penalties, non-player specific penalties, and penalties that don't lead to power plays.
    
    Supported Report Types:
    - GS: Game Summary (scoring, penalties, team stats, officials, three stars)
    - RO: Roster (active players, goalies, scratches)
    - PL: Play-by-Play (placeholder)
    - ES: Event Summary (placeholder)
    - SS: Shot Summary (placeholder)
    - FS: Faceoff Summary (placeholder)
    - FC: Faceoff Comparison (placeholder)
    - TH: Time on Ice (placeholder)
    - TV: Time on Ice Comparison (placeholder)
    """
    
    def __init__(self, config=None, storage_path: str = "storage/20242025/json"):
        """
        Initialize the HTML penalty parser.
        
        Args:
            config: Configuration object
            storage_path: Path to the JSON storage directory for reference data
        """
        self.config = config
        self.logger = logging.getLogger('HTMLPenaltyParser')
        self.reference_data = ReferenceDataLoader(storage_path)
        
        # Penalty type mappings
        self.penalty_types = {
            'MIN': 'Minor',
            'MAJ': 'Major', 
            'BEN': 'Bench',
            'MIS': 'Misconduct',
            'MAT': 'Match'
        }
        
        # Non-power play penalties
        self.non_power_play_penalties = {
            'fighting', 'misconduct', 'game-misconduct', 'match-penalty',
            'too-many-men-on-the-ice', 'delay-of-game', 'unsportsmanlike-conduct',
            'instigator', 'instigator-misconduct'
        }
        
        # Penalty duration patterns
        self.duration_patterns = [
            r'(\d+)\s*min',  # "2 min", "5 min"
            r'(\d+)\s*minutes',  # "2 minutes"
            r'(\d+)\s*minute',  # "1 minute"
        ]
        
        # Penalty description patterns
        self.description_patterns = [
            r'penalty\s*[:\-]?\s*([^,]+)',  # "penalty: tripping"
            r'([a-z\-]+)\s*penalty',  # "tripping penalty"
            r'penalty\s*for\s*([^,]+)',  # "penalty for tripping"
        ]
    
    def parse_game_data(self, season: str, game_id: str, html_dir: Path) -> Dict[str, Any]:
        """
        Parse complete game data from all available HTML reports for a game.
        
        Args:
            season: Season identifier
            game_id: Game ID
            html_dir: Directory containing HTML reports
            
        Returns:
            Dictionary containing complete parsed game data from all sources
        """
        game_data = {
            'game_id': game_id,
            'season': season,
            'game_metadata': {},
            'sources': {},
            'consolidated_data': {},
            'parsing_metadata': {
                'reports_parsed': [],
                'total_records_found': 0,
                'parsing_errors': []
            }
        }
        
        # Parse each report type
        report_types = ['GS', 'PL', 'ES', 'RO', 'SS', 'FS', 'FC', 'TH', 'TV']  # All available report types
        
        for report_type in report_types:
            html_file = html_dir / f"{report_type}{game_id}.HTM"
            if html_file.exists():
                try:
                    report_data = self.parse_report_data(html_file, report_type)
                    if report_data:
                        game_data['sources'][report_type] = report_data
                        game_data['parsing_metadata']['reports_parsed'].append(report_type)
                        game_data['parsing_metadata']['total_records_found'] += self.count_records(report_data)
                except Exception as e:
                    error_msg = f"Error parsing {report_type} report: {e}"
                    game_data['parsing_metadata']['parsing_errors'].append(error_msg)
                    self.logger.error(error_msg)
        
        # Consolidate data from all sources
        if game_data['sources']:
            game_data['consolidated_data'] = self.consolidate_game_data(game_data['sources'])
        
        return game_data
    
    def parse_game_penalties(self, season: str, game_id: str, html_dir: Path) -> Dict[str, Any]:
        """
        Parse penalty data from all available HTML reports for a game.
        
        Args:
            season: Season identifier
            game_id: Game ID
            html_dir: Directory containing HTML reports
            
        Returns:
            Dictionary containing parsed penalty data from all sources
        """
        game_penalties = {
            'game_id': game_id,
            'season': season,
            'sources': {},
            'consolidated_penalties': [],
            'complex_scenarios': [],
            'parsing_metadata': {
                'reports_parsed': [],
                'total_penalties_found': 0,
                'parsing_errors': []
            }
        }
        
        # Parse each report type
        report_types = ['GS', 'PL', 'ES']  # Game Summary, Play-by-Play, Event Summary
        
        for report_type in report_types:
            html_file = html_dir / report_type / f"{report_type}{game_id}.HTM"
            if html_file.exists():
                try:
                    # Use the advanced penalty parsing for GS reports
                    if report_type == 'GS':
                        # Parse the full report data and extract penalties from it
                        report_data = self.parse_report_data(html_file, report_type)
                        penalties_data = report_data.get('penalties', {})
                        # Extract penalties from by_period structure
                        penalties = []
                        for period, period_penalties in penalties_data.get('by_period', {}).items():
                            penalties.extend(period_penalties)
                    else:
                        # Use the existing method for other report types
                        penalties = self.parse_report_penalties(html_file, report_type)
                    
                    if penalties:
                        game_penalties['sources'][report_type] = penalties
                        game_penalties['parsing_metadata']['reports_parsed'].append(report_type)
                        game_penalties['parsing_metadata']['total_penalties_found'] += len(penalties)
                except Exception as e:
                    error_msg = f"Error parsing {report_type} report: {e}"
                    game_penalties['parsing_metadata']['parsing_errors'].append(error_msg)
                    self.logger.error(error_msg)
        
        # Consolidate penalties from all sources
        if game_penalties['sources']:
            game_penalties['consolidated_penalties'] = self.consolidate_penalties(game_penalties['sources'])
            game_penalties['complex_scenarios'] = self.detect_complex_scenarios(game_penalties['consolidated_penalties'])
        
        return game_penalties
    
    def consolidate_penalties(self, sources: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
        """
        Consolidate penalties from multiple sources (GS, PL, ES) to create a unified penalty list.
        
        Args:
            sources: Dictionary with report type as key and penalty list as value
            
        Returns:
            List of consolidated penalty dictionaries
        """
        consolidated = []
        penalty_keys = set()
        
        try:
            # Process each source, with GS taking priority for conflicts
            source_priority = ['GS', 'PL', 'ES']
            
            for source_type in source_priority:
                if source_type in sources:
                    for penalty in sources[source_type]:
                        # Create unique key for this penalty
                        penalty_key = (
                            penalty.get('penalty_number'),
                            penalty.get('period'),
                            penalty.get('time'),
                            penalty.get('player', {}).get('name'),
                            penalty.get('penalty_type')
                        )
                        
                        if penalty_key not in penalty_keys:
                            penalty_keys.add(penalty_key)
                            # Add source information
                            penalty['source'] = source_type
                            consolidated.append(penalty)
            
            self.logger.debug(f"Consolidated {len(consolidated)} unique penalties from {len(sources)} sources")
            
        except Exception as e:
            self.logger.error(f"Error consolidating penalties: {e}")
        
        return consolidated
    
    def detect_complex_scenarios(self, penalties: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Detect complex penalty scenarios from consolidated penalty data.
        
        Args:
            penalties: List of consolidated penalty dictionaries
            
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
            
            # 3. Non-power play penalties (fighting, misconducts)
            non_pp_penalties = [p for p in penalties if p.get('penalty_type') in ['Fighting', 'Misconduct', 'Game Misconduct']]
            if non_pp_penalties:
                scenarios.append({
                    'type': 'non_power_play_penalties',
                    'penalties': non_pp_penalties,
                    'description': 'Penalties that do not lead to power plays',
                    'impact': 'No numerical advantage, different statistical treatment'
                })
            
            self.logger.debug(f"Detected {len(scenarios)} complex penalty scenarios")
                
        except Exception as e:
            self.logger.error(f"Error detecting complex penalty scenarios: {e}")
        
        return scenarios
    
    def parse_report_data(self, html_file: Path, report_type: str) -> Dict[str, Any]:
        """
        Parse complete data from a specific HTML report type using BeautifulSoup.
        
        Args:
            html_file: Path to HTML file
            report_type: Type of report (GS, PL, ES, RO, SS, FS, FC, TH, TV)
            
        Returns:
            Dictionary containing parsed data from the report
        """
        try:
            with open(html_file, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
            
            soup = BeautifulSoup(content, 'html.parser')
            
            if report_type == 'GS':
                return self.parse_game_summary_data(soup, str(html_file))
            elif report_type == 'PL':
                return self.parse_playbyplay_data(soup)
            elif report_type == 'ES':
                return self.parse_event_summary_data(soup, str(html_file))
            elif report_type == 'RO':
                return self._parse_roster_data(soup, str(html_file))
            elif report_type == 'SS':
                return self.parse_shot_summary_data(soup)
            elif report_type == 'FS':
                return self.parse_faceoff_summary_data(soup, str(html_file))
            elif report_type == 'FC':
                return self.parse_faceoff_comparison_data(soup)
            elif report_type in ['TH', 'TV']:
                return self.parse_time_on_ice_data(soup, report_type, str(html_file))
            else:
                return {'report_type': report_type, 'data': {}}
                
        except Exception as e:
            self.logger.error(f"Error parsing {report_type} report {html_file}: {e}")
            return {'report_type': report_type, 'error': str(e)}
    
    def parse_report_penalties(self, html_file: Path, report_type: str) -> List[Dict[str, Any]]:
        """
        Parse penalties from a specific HTML report type.
        
        Args:
            html_file: Path to HTML file
            report_type: Type of report (GS, PL, ES)
            
        Returns:
            List of parsed penalty dictionaries
        """
        try:
            with open(html_file, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
            
            if report_type == 'GS':
                return self.parse_game_summary_penalties(content)
            elif report_type == 'PL':
                return self.parse_playbyplay_penalties(content)
            elif report_type == 'ES':
                return self.parse_event_summary_penalties(content)
            else:
                return []
                
        except Exception as e:
            self.logger.error(f"Error parsing {report_type} report {html_file}: {e}")
            return []
    
    def parse_game_summary_data(self, soup: BeautifulSoup, file_path: Optional[str] = None) -> Dict[str, Any]:
        """
        Parse complete Game Summary (GS) data using BeautifulSoup with proper section structure.
        
        Args:
            soup: BeautifulSoup object of the HTML content
            file_path: Optional file path for game ID extraction from filename
            
        Returns:
            Dictionary containing all parsed game summary data with proper structure
        """
        data = {
            'report_type': 'GS',
            'game_header': {},
            'scoring_summary': {},
            'penalties': {},
            'team_stats': {},
            'officials': {},
            'three_stars': {}
        }
        
        try:
            # Parse game header (teams, score, date, venue)
            data['game_header'] = self._parse_game_header(soup, file_path)
            
            # Parse scoring summary with proper goal structure
            data['scoring_summary'] = self._parse_scoring_summary(soup)
            
            # Parse penalties with proper penalty structure
            data['penalties'] = self._parse_penalties_section(soup)
            
            # Parse team statistics
            data['team_stats'] = self._parse_team_stats(soup)
            
            # Parse officials and three stars
            data['officials'] = self._parse_officials(soup)
            data['three_stars'] = self._parse_three_stars(soup)
            
        except Exception as e:
            self.logger.error(f"Error parsing game summary data: {e}")
            data['error'] = str(e)
        
        return data
    
    def _extract_game_id_from_html(self, soup: BeautifulSoup, file_path: Optional[str] = None) -> Optional[int]:
        """Extract game ID from HTML content or filename."""
        try:
            # First try to extract from filename if provided
            if file_path:
                filename = Path(file_path).stem
                # Pattern: GS020001 -> 2024020001
                if filename.startswith(('GS', 'ES', 'PL', 'RO', 'SS', 'FS', 'FC', 'TH', 'TV')):
                    game_number = filename[2:]  # Remove prefix
                    if game_number.isdigit():
                        # Convert to full game ID format
                        game_id = int(f"2024{game_number}")
                        return game_id
            
            # Look for game ID in various places in HTML
            # Check title tag
            title_tag = soup.find('title')
            if title_tag:
                title_text = title_tag.get_text()
                # Look for pattern like "Game 2024020031" or similar
                game_id_match = re.search(r'Game\s+(\d+)', title_text)
                if game_id_match:
                    return int(game_id_match.group(1))
            
            # Check for game ID in script tags or other elements
            scripts = soup.find_all('script')
            for script in scripts:
                if script.string:
                    game_id_match = re.search(r'gameId["\']?\s*[:=]\s*["\']?(\d+)', script.string)
                    if game_id_match:
                        return int(game_id_match.group(1))
            
            # Check for game ID in data attributes
            elements_with_data = soup.find_all(attrs={'data-game-id': True})
            for elem in elements_with_data:
                game_id = elem.get('data-game-id')
                if game_id and game_id.isdigit():
                    return int(game_id)
                    
        except Exception as e:
            self.logger.error(f"Error extracting game ID: {e}")
        
        return None

    def _lookup_player_id_by_sweater(self, sweater_number: int, player_name: str = "") -> Optional[int]:
        """
        Look up player ID using sweater number and player name from reference data.
        
        Args:
            sweater_number: Player sweater number
            player_name: Player name for additional matching
            
        Returns:
            Player ID if found, None otherwise
        """
        try:
            if not hasattr(self, 'reference_data') or not self.reference_data:
                return None
            
            # Load players data if not already loaded
            if not hasattr(self, '_players_cache'):
                self._players_cache = {}
                players_file = Path(self.storage_path) / "players.json"
                if players_file.exists():
                    with open(players_file, 'r', encoding='utf-8') as f:
                        players_data = json.load(f)
                        for player in players_data:
                            player_id = player.get('id')
                            sweater_num = player.get('sweaterNumber')
                            if player_id and sweater_num:
                                self._players_cache[sweater_num] = player_id
            
            # Look up by sweater number
            if sweater_number in self._players_cache:
                return self._players_cache[sweater_number]
            
            # If not found by sweater number, try to match by name
            if player_name:
                # This would require additional name matching logic
                # For now, return None if sweater number lookup fails
                pass
            
            return None
            
        except Exception as e:
            self.logger.debug(f"Error looking up player ID for sweater {sweater_number}: {e}")
            return None

    def _resolve_player_name(self, team_id: int, sweater_number: int, fallback_name: str = "") -> str:
        """
        Resolve player name using sweater number lookup from reference data.
        
        Args:
            team_id: Team ID
            sweater_number: Player sweater number
            fallback_name: Fallback name if lookup fails
            
        Returns:
            Resolved player name or fallback name
        """
        return self.reference_data.resolve_player_name(team_id, sweater_number, fallback_name)
    
    def _resolve_team_name(self, team_id: int, fallback_name: str = "") -> str:
        """
        Resolve team name using team ID lookup from reference data.
        
        Args:
            team_id: Team ID
            fallback_name: Fallback name if lookup fails
            
        Returns:
            Resolved team name or fallback name
        """
        return self.reference_data.resolve_team_name(team_id, fallback_name)

    def _parse_game_header(self, soup: BeautifulSoup, file_path: Optional[str] = None) -> Dict[str, Any]:
        """Parse game header information including teams, scores, date, and venue."""
        header = {
            'visitor_team': {},
            'home_team': {},
            'game_info': {}
        }
        
        try:
            # Extract game ID for reference data lookup
            game_id = self._extract_game_id_from_html(soup, file_path)
            if game_id:
                header['game_info']['game_id'] = game_id
                
                # Get game data from reference
                game_data = self.reference_data.get_game_by_id(game_id)
                boxscore_data = self.reference_data.get_boxscore_by_id(game_id)
                
                if game_data:
                    # Use reference data for team info
                    away_team_data = game_data.get('awayTeam', {})
                    home_team_data = game_data.get('homeTeam', {})
                    
                    header['visitor_team']['id'] = away_team_data.get('id')
                    header['visitor_team']['name'] = away_team_data.get('placeName', {}).get('default', '')
                    header['visitor_team']['abbrev'] = away_team_data.get('abbrev', '')
                    header['visitor_team']['score'] = away_team_data.get('score', 0)
                    
                    header['home_team']['id'] = home_team_data.get('id')
                    header['home_team']['name'] = home_team_data.get('placeName', {}).get('default', '')
                    header['home_team']['abbrev'] = home_team_data.get('abbrev', '')
                    header['home_team']['score'] = home_team_data.get('score', 0)
                    
                    # Game info from reference data
                    header['game_info']['date'] = game_data.get('gameDate', '')
                    header['game_info']['venue'] = game_data.get('venue', {}).get('default', '')
                    header['game_info']['start_time'] = game_data.get('startTimeUTC', '')
                    
                    return header
            
            # Fallback to HTML parsing if reference data not available
            # Parse visitor team info
            visitor_table = soup.find('table', {'id': 'Visitor'})
            if visitor_table:
                # Team name from alt attribute of logo image
                logo_img = visitor_table.find('img')
                if logo_img and logo_img.get('alt'):
                    header['visitor_team']['name'] = logo_img.get('alt')
                
                # Score from the large font element
                score_elem = visitor_table.find('td', style=lambda x: x and 'font-size: 40px' in x)
                if score_elem:
                    header['visitor_team']['score'] = int(score_elem.get_text(strip=True))
                
                # Team name from text content as backup
                team_name_elem = visitor_table.find('td', string=lambda text: text and any(team in text.upper() for team in ['DEVILS', 'SABRES', 'RANGERS', 'ISLANDERS', 'FLYERS', 'PENGUINS', 'CAPITALS', 'HURRICANES', 'PANTHERS', 'LIGHTNING', 'BRUINS', 'MAPLE LEAFS', 'SENATORS', 'CANADIENS', 'RED WINGS', 'BLACKHAWKS', 'BLUE JACKETS', 'STARS', 'WILD', 'PREDATORS', 'BLUES', 'JETS', 'AVALANCHE', 'COYOTES', 'DUCKS', 'KINGS', 'SHARKS', 'GOLDEN KNIGHTS', 'FLAMES', 'OILERS', 'CANUCKS', 'KRAKEN']))
                if team_name_elem:
                    header['visitor_team']['name'] = team_name_elem.get_text(strip=True).split('\n')[0]
            
            # Parse home team info
            home_table = soup.find('table', {'id': 'Home'})
            if home_table:
                # Team name from alt attribute of logo image
                logo_img = home_table.find('img')
                if logo_img and logo_img.get('alt'):
                    header['home_team']['name'] = logo_img.get('alt')
                
                # Score from the large font element
                score_elem = home_table.find('td', style=lambda x: x and 'font-size: 40px' in x)
                if score_elem:
                    header['home_team']['score'] = int(score_elem.get_text(strip=True))
                
                # Team name from text content as backup
                team_name_elem = home_table.find('td', string=lambda text: text and any(team in text.upper() for team in ['DEVILS', 'SABRES', 'RANGERS', 'ISLANDERS', 'FLYERS', 'PENGUINS', 'CAPITALS', 'HURRICANES', 'PANTHERS', 'LIGHTNING', 'BRUINS', 'MAPLE LEAFS', 'SENATORS', 'CANADIENS', 'RED WINGS', 'BLACKHAWKS', 'BLUE JACKETS', 'STARS', 'WILD', 'PREDATORS', 'BLUES', 'JETS', 'AVALANCHE', 'COYOTES', 'DUCKS', 'KINGS', 'SHARKS', 'GOLDEN KNIGHTS', 'FLAMES', 'OILERS', 'CANUCKS', 'KRAKEN']))
                if team_name_elem:
                    header['home_team']['name'] = team_name_elem.get_text(strip=True).split('\n')[0]
            
            # Parse game info
            game_info_table = soup.find('table', {'id': 'GameInfo'})
            if game_info_table:
                rows = game_info_table.find_all('tr')
                for row in rows:
                    text = row.get_text(strip=True)
                    if 'NHL Global Series' in text or 'NHL' in text:
                        header['game_info']['event'] = text
                    elif any(day in text for day in ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']):
                        header['game_info']['date'] = text
                    elif 'Attendance' in text:
                        header['game_info']['attendance'] = text
                    elif 'Start' in text and 'End' in text:
                        header['game_info']['time_info'] = text
                    elif 'Game' in text and any(char.isdigit() for char in text):
                        header['game_info']['game_number'] = text
                    elif text in ['Final', 'Live', 'Scheduled']:
                        header['game_info']['status'] = text
                        
        except Exception as e:
            self.logger.error(f"Error parsing game header: {e}")
            header['error'] = str(e)
        
        return header
    
    
    
    def _parse_team_stats(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Parse team statistics section."""
        team_stats = {
            'visitor_stats': {},
            'home_stats': {}
        }
        
        try:
            # Find team stats section
            stats_section = soup.find('td', string='TEAM STATS')
            if stats_section:
                # Find the team stats table
                table = stats_section.find_parent().find_next('table')
                if table:
                    rows = table.find_all('tr')
                    for row in rows[1:]:  # Skip header row
                        cells = row.find_all('td')
                        if len(cells) >= 3:
                            stat_name = cells[0].get_text(strip=True)
                            visitor_value = cells[1].get_text(strip=True)
                            home_value = cells[2].get_text(strip=True)
                            
                            team_stats['visitor_stats'][stat_name] = visitor_value
                            team_stats['home_stats'][stat_name] = home_value
                            
        except Exception as e:
            self.logger.error(f"Error parsing team stats: {e}")
            team_stats['error'] = str(e)
        
        return team_stats
    
    def _parse_officials(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Parse officials section."""
        officials = {
            'referees': [],
            'linesmen': []
        }
        
        try:
            # Find officials section
            officials_section = soup.find('td', string='OFFICIALS')
            if officials_section:
                # Find the officials table
                table = officials_section.find_parent().find_next('table')
                if table:
                    rows = table.find_all('tr')
                    for row in rows[1:]:  # Skip header row
                        cells = row.find_all('td')
                        if len(cells) >= 2:
                            official_type = cells[0].get_text(strip=True)
                            official_name = cells[1].get_text(strip=True)
                            
                            if 'Referee' in official_type:
                                officials['referees'].append(official_name)
                            elif 'Linesman' in official_type:
                                officials['linesmen'].append(official_name)
                                
        except Exception as e:
            self.logger.error(f"Error parsing officials: {e}")
            officials['error'] = str(e)
        
        return officials
    
    def _parse_three_stars(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Parse three stars section."""
        three_stars = {
            'stars': []
        }
        
        try:
            # Find three stars section
            stars_section = soup.find('td', string='THREE STARS')
            if stars_section:
                # Find the three stars table
                table = stars_section.find_parent().find_next('table')
                if table:
                    rows = table.find_all('tr')
                    for row in rows[1:]:  # Skip header row
                        cells = row.find_all('td')
                        if len(cells) >= 2:
                            star_number = cells[0].get_text(strip=True)
                            player_name = cells[1].get_text(strip=True)
                            
                            three_stars['stars'].append({
                                'star': star_number,
                                'player': player_name
                            })
                            
        except Exception as e:
            self.logger.error(f"Error parsing three stars: {e}")
            three_stars['error'] = str(e)
        
        return three_stars
    
    def parse_game_summary_penalties(self, content: str) -> List[Dict[str, Any]]:
        """
        Parse penalties from Game Summary (GS) HTML report.
        
        Game Summary reports contain penalty summaries organized by period.
        """
        penalties = []
        soup = BeautifulSoup(content, 'html.parser')
        
        # Look for penalty sections
        penalty_sections = soup.find_all('table', class_='border')
        
        for section in penalty_sections:
            # Look for penalty rows
            rows = section.find_all('tr')
            for row in rows:
                cells = row.find_all('td')
                if len(cells) >= 3:
                    penalty_data = self.extract_penalty_from_gs_row(cells)
                    if penalty_data:
                        penalties.append(penalty_data)
        
        # Also look for penalty patterns in text
        text_penalties = self.extract_penalties_from_text(content)
        penalties.extend(text_penalties)
        
        return penalties
    
    def parse_playbyplay_penalties(self, content: str) -> List[Dict[str, Any]]:
        """
        Parse penalties from Play-by-Play (PL) HTML report.
        
        Play-by-Play reports contain detailed penalty events with timing.
        """
        penalties = []
        
        # Use BeautifulSoup for structured parsing
        soup = BeautifulSoup(content, 'html.parser')
        
        # Look for penalty rows in play-by-play tables
        penalty_rows = soup.find_all('tr', class_='penalty')
        for row in penalty_rows:
            penalty_data = self.extract_penalty_from_pl_row(row)
            if penalty_data:
                penalties.append(penalty_data)
        
        # Also extract from text patterns
        text_penalties = self.extract_penalties_from_text(content)
        penalties.extend(text_penalties)
        
        return penalties
    
    def parse_event_summary_penalties(self, content: str) -> List[Dict[str, Any]]:
        """
        Parse penalties from Event Summary (ES) HTML report.
        
        Event Summary reports contain penalty event details.
        """
        penalties = []
        
        # Extract penalties from text patterns
        text_penalties = self.extract_penalties_from_text(content)
        penalties.extend(text_penalties)
        
        return penalties
    
    def extract_penalty_minutes_served(self, description: str, cells: List) -> Dict[str, Any]:
        """
        Extract penalty minutes served information from penalty description and table cells.
        
        Args:
            description: Penalty description text
            cells: Table cells containing penalty information
            
        Returns:
            Dictionary with penalty minutes served information
        """
        penalty_minutes_served = {
            'player_name': '',
            'player_id': '',
            'minutes_served': 0,
            'is_team_penalty': False,
            'serving_player_identified': False
        }
        
        try:
            desc_lower = description.lower()
            
            # Check if this is a team penalty (bench penalty)
            if any(team_penalty in desc_lower for team_penalty in [
                'too many men', 'bench', 'team', 'delay of game', 'unsportsmanlike'
            ]):
                penalty_minutes_served['is_team_penalty'] = True
                
                # Look for "served by" information in the description
                served_by_match = re.search(r'served\s+by\s+([A-Z][a-z]+\s+[A-Z][a-z]+)', description, re.IGNORECASE)
                if served_by_match:
                    penalty_minutes_served['player_name'] = served_by_match.group(1).strip()
                    penalty_minutes_served['serving_player_identified'] = True
                
                # Look for serving player in additional cells
                if len(cells) > 3:
                    for i in range(3, len(cells)):
                        cell_text = cells[i].get_text(strip=True)
                        if cell_text and len(cell_text) > 2 and not cell_text.isdigit():
                            # This might be the serving player name
                            if not penalty_minutes_served['player_name']:
                                penalty_minutes_served['player_name'] = cell_text
                                penalty_minutes_served['serving_player_identified'] = True
                            break
            
            # Extract penalty minutes served (usually matches the penalty duration)
            penalty_minutes_served['minutes_served'] = self.extract_penalty_duration(description)
            
        except Exception as e:
            self.logger.debug(f"Error extracting penalty minutes served: {e}")
        
        return penalty_minutes_served
    
    def extract_penalty_duration(self, description: str) -> int:
        """Extract penalty duration from description text."""
        try:
            # Look for duration patterns
            for pattern in self.duration_patterns:
                duration_match = re.search(pattern, description, re.IGNORECASE)
                if duration_match:
                    return int(duration_match.group(1))
            
            # Default duration based on penalty type
            desc_lower = description.lower()
            if any(major in desc_lower for major in ['major', '5 min', '5 minute']):
                return 5
            elif any(mis in desc_lower for mis in ['misconduct', '10 min', '10 minute']):
                return 10
            else:
                return 2  # Default to minor penalty
                
        except Exception as e:
            self.logger.debug(f"Error extracting penalty duration: {e}")
            return 2
    
    def extract_penalty_from_gs_row(self, cells: List) -> Optional[Dict[str, Any]]:
        """Extract penalty data from Game Summary table row."""
        try:
            if len(cells) < 3:
                return None
            
            # Extract time, team, and description
            time_cell = cells[0].get_text(strip=True)
            team_cell = cells[1].get_text(strip=True) if len(cells) > 1 else ""
            desc_cell = cells[2].get_text(strip=True) if len(cells) > 2 else ""
            
            # Parse time
            time_match = re.search(r'(\d{1,2}:\d{2})', time_cell)
            if not time_match:
                return None
            
            time = time_match.group(1)
            
            # Parse penalty details
            penalty_info = self.parse_penalty_description(desc_cell)
            if not penalty_info:
                return None
            
            # Look for penalty minutes served information
            penalty_minutes_served = self.extract_penalty_minutes_served(desc_cell, cells)
            
            return {
                'time': time,
                'team': team_cell.strip(),
                'description': penalty_info['description'],
                'duration': penalty_info['duration'],
                'penalty_type': penalty_info['type'],
                'penalty_minutes_served': penalty_minutes_served,
                'source': 'game_summary',
                'raw_text': desc_cell
            }
            
        except Exception as e:
            self.logger.debug(f"Error extracting penalty from GS row: {e}")
            return None
    
    def extract_penalty_from_pl_row(self, row) -> Optional[Dict[str, Any]]:
        """Extract penalty data from Play-by-Play table row."""
        try:
            cells = row.find_all('td')
            if len(cells) < 4:
                return None
            
            # Extract time, team, player, and description
            time_cell = cells[0].get_text(strip=True)
            team_cell = cells[1].get_text(strip=True) if len(cells) > 1 else ""
            player_cell = cells[2].get_text(strip=True) if len(cells) > 2 else ""
            desc_cell = cells[3].get_text(strip=True) if len(cells) > 3 else ""
            
            # Parse time
            time_match = re.search(r'(\d{1,2}:\d{2})', time_cell)
            if not time_match:
                return None
            
            time = time_match.group(1)
            
            # Parse penalty details
            penalty_info = self.parse_penalty_description(desc_cell)
            if not penalty_info:
                return None
            
            return {
                'time': time,
                'team': team_cell.strip(),
                'player': player_cell.strip(),
                'description': penalty_info['description'],
                'duration': penalty_info['duration'],
                'penalty_type': penalty_info['type'],
                'penalty_minutes_served': {
                    'player_name': player_cell.strip(),
                    'player_id': '',
                    'minutes_served': penalty_info['duration'],
                    'is_team_penalty': False,
                    'serving_player_identified': True
                },
                'source': 'playbyplay',
                'raw_text': desc_cell
            }
            
        except Exception as e:
            self.logger.debug(f"Error extracting penalty from PL row: {e}")
            return None
    
    def extract_penalties_from_text(self, content: str) -> List[Dict[str, Any]]:
        """
        Extract penalties from text content using regex patterns.
        
        This is a fallback method for when structured parsing fails.
        """
        penalties = []
        
        # Look for penalty patterns in text
        penalty_patterns = [
            # Time + Team + Player + Penalty
            r'(\d{1,2}:\d{2})\s*([A-Z]{3})\s*([A-Z\s\.]+)\s*penalty\s*[:\-]?\s*([^,\n]+)',
            # Time + Penalty + Team
            r'(\d{1,2}:\d{2})\s*penalty\s*([^,\n]+)\s*([A-Z]{3})',
            # Time + Team + Penalty
            r'(\d{1,2}:\d{2})\s*([A-Z]{3})\s*penalty\s*[:\-]?\s*([^,\n]+)',
            # Bench penalty patterns
            r'(\d{1,2}:\d{2})\s*([A-Z]{3})\s*bench\s*penalty\s*[:\-]?\s*([^,\n]+)',
        ]
        
        for pattern in penalty_patterns:
            matches = re.finditer(pattern, content, re.IGNORECASE)
            for match in matches:
                try:
                    penalty_data = self.parse_penalty_match(match, pattern)
                    if penalty_data:
                        penalties.append(penalty_data)
                except Exception as e:
                    self.logger.debug(f"Error parsing penalty match: {e}")
                    continue
        
        return penalties
    
    def parse_penalty_match(self, match, pattern: str) -> Optional[Dict[str, Any]]:
        """Parse a regex match into penalty data."""
        try:
            groups = match.groups()
            
            if len(groups) < 2:
                return None
            
            time = groups[0]
            
            # Determine which groups contain what information based on pattern
            if 'bench' in pattern.lower():
                team = groups[1] if len(groups) > 1 else ""
                description = groups[2] if len(groups) > 2 else ""
                player = ""
            elif len(groups) == 3:
                if len(groups[1]) == 3:  # Team abbreviation
                    team = groups[1]
                    player = groups[2]
                    description = groups[2]
                else:
                    team = groups[2] if len(groups) > 2 else ""
                    player = groups[1]
                    description = groups[1]
            elif len(groups) == 4:
                team = groups[1]
                player = groups[2]
                description = groups[3]
            else:
                team = ""
                player = ""
                description = groups[1]
            
            # Parse penalty details
            penalty_info = self.parse_penalty_description(description)
            if not penalty_info:
                return None
            
            return {
                'time': time,
                'team': team.strip(),
                'player': player.strip(),
                'description': penalty_info['description'],
                'duration': penalty_info['duration'],
                'penalty_type': penalty_info['type'],
                'penalty_minutes_served': {
                    'player_name': player.strip(),
                    'player_id': '',
                    'minutes_served': penalty_info['duration'],
                    'is_team_penalty': penalty_info['type'] == 'BEN',
                    'serving_player_identified': bool(player.strip())
                },
                'source': 'text_pattern',
                'raw_text': match.group(0)
            }
            
        except Exception as e:
            self.logger.debug(f"Error parsing penalty match: {e}")
            return None
    
    def parse_penalty_description(self, description: str) -> Optional[Dict[str, Any]]:
        """
        Parse penalty description to extract type, duration, and clean description.
        
        Args:
            description: Raw penalty description text
            
        Returns:
            Dictionary with parsed penalty information
        """
        if not description or not description.strip():
            return None
        
        desc_lower = description.lower().strip()
        
        # Determine penalty type
        penalty_type = 'MIN'  # Default to minor
        if any(major in desc_lower for major in ['major', '5 min', '5 minute']):
            penalty_type = 'MAJ'
        elif any(bench in desc_lower for bench in ['bench', 'team', 'too many men']):
            penalty_type = 'BEN'
        elif any(mis in desc_lower for mis in ['misconduct', '10 min', '10 minute']):
            penalty_type = 'MIS'
        elif any(match in desc_lower for match in ['match penalty', 'ejection']):
            penalty_type = 'MAT'
        
        # Extract duration
        duration = 2  # Default to 2 minutes
        for pattern in self.duration_patterns:
            duration_match = re.search(pattern, desc_lower)
            if duration_match:
                duration = int(duration_match.group(1))
                break
        
        # Clean description
        clean_desc = self.clean_penalty_description(description)
        
        return {
            'type': penalty_type,
            'duration': duration,
            'description': clean_desc,
            'is_power_play': self.is_power_play_penalty(clean_desc)
        }
    
    def clean_penalty_description(self, description: str) -> str:
        """Clean and standardize penalty description."""
        if not description:
            return ""
        
        # Remove common prefixes and suffixes
        cleaned = description.strip()
        cleaned = re.sub(r'^penalty\s*[:\-]?\s*', '', cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r'\s*penalty\s*$', '', cleaned, flags=re.IGNORECASE)
        
        # Standardize common variations
        variations = {
            'roughing-removing-opponents-helmet': 'roughing-removing-helmet',
            'ps-slash-on-breakaway': 'penalty-shot-slash',
            'too-many-men-on-the-ice': 'too-many-men',
            'delaying-game-puck-over-glass': 'delay-of-game-puck-over-glass',
            'interference-goalkeeper': 'goalie-interference'
        }
        
        for variant, standard in variations.items():
            if variant in cleaned.lower():
                cleaned = standard
                break
        
        return cleaned.strip()
    
    def is_power_play_penalty(self, description: str) -> bool:
        """Determine if a penalty leads to a power play."""
        if not description:
            return True  # Default to power play
        
        desc_lower = description.lower()
        return not any(non_pp in desc_lower for non_pp in self.non_power_play_penalties)
    
    def consolidate_penalties(self, source_penalties: Dict[str, List]) -> List[Dict[str, Any]]:
        """
        Consolidate penalties from multiple sources, removing duplicates.
        
        Args:
            source_penalties: Dictionary of penalties by source
            
        Returns:
            Consolidated list of penalties
        """
        consolidated = []
        seen_penalties = set()
        
        for source, penalties in source_penalties.items():
            for penalty in penalties:
                # Create unique identifier for this penalty
                penalty_key = self.create_penalty_key(penalty)
                
                if penalty_key not in seen_penalties:
                    seen_penalties.add(penalty_key)
                    consolidated.append(penalty)
                else:
                    # Merge additional information from this source
                    self.merge_penalty_info(consolidated, penalty)
        
        # Sort by time
        consolidated.sort(key=lambda x: self.parse_time(x.get('time', '00:00')))
        
        return consolidated
    
    def create_penalty_key(self, penalty: Dict[str, Any]) -> str:
        """Create a unique key for penalty deduplication."""
        time = penalty.get('time', '')
        team = penalty.get('team', '')
        description = penalty.get('description', '')
        
        return f"{time}_{team}_{description}".lower()
    
    def merge_penalty_info(self, consolidated: List[Dict], new_penalty: Dict):
        """Merge additional information from a new penalty source."""
        # Find matching penalty in consolidated list
        for existing in consolidated:
            if self.create_penalty_key(existing) == self.create_penalty_key(new_penalty):
                # Merge missing information
                if not existing.get('player') and new_penalty.get('player'):
                    existing['player'] = new_penalty['player']
                if not existing.get('duration') and new_penalty.get('duration'):
                    existing['duration'] = new_penalty['duration']
                if not existing.get('penalty_type') and new_penalty.get('penalty_type'):
                    existing['penalty_type'] = new_penalty['penalty_type']
                break
    
    def parse_time(self, time_str: str) -> int:
        """Parse time string to seconds for sorting."""
        try:
            if ':' in time_str:
                minutes, seconds = map(int, time_str.split(':'))
                return minutes * 60 + seconds
            return 0
        except:
            return 0
    
    def detect_complex_scenarios(self, penalties: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Detect complex penalty scenarios that require special handling.
        
        Args:
            penalties: List of consolidated penalties
            
        Returns:
            List of detected complex scenarios
        """
        scenarios = []
        
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
        
        return scenarios
    
    def generate_penalty_report(self, game_penalties: Dict[str, Any]) -> str:
        """
        Generate a comprehensive penalty report for a game.
        
        Args:
            game_penalties: Parsed penalty data for a game
            
        Returns:
            Formatted penalty report
        """
        report = []
        report.append(f"# Penalty Report - Game {game_penalties['game_id']}")
        report.append(f"## Season: {game_penalties['season']}")
        report.append("")
        
        # Summary
        report.append("## Summary")
        report.append(f"- **Total Penalties**: {len(game_penalties['consolidated_penalties'])}")
        report.append(f"- **Sources Parsed**: {', '.join(game_penalties['parsing_metadata']['reports_parsed'])}")
        report.append(f"- **Complex Scenarios**: {len(game_penalties['complex_scenarios'])}")
        report.append("")
        
        # Penalties by period
        report.append("## Penalties by Period")
        penalties_by_period = {}
        for penalty in game_penalties['consolidated_penalties']:
            time = penalty.get('time', '')
            period = self.determine_period(time)
            if period not in penalties_by_period:
                penalties_by_period[period] = []
            penalties_by_period[period].append(penalty)
        
        for period in sorted(penalties_by_period.keys()):
            report.append(f"### Period {period}")
            for penalty in penalties_by_period[period]:
                penalty_line = f"- **{penalty['time']}** - {penalty['team']} - {penalty['description']} ({penalty['duration']} min)"
                
                # Add penalty minutes served information
                if penalty.get('penalty_minutes_served'):
                    pms = penalty['penalty_minutes_served']
                    if pms.get('is_team_penalty') and pms.get('serving_player_identified'):
                        penalty_line += f" - Served by: {pms['player_name']}"
                    elif pms.get('serving_player_identified'):
                        penalty_line += f" - Player: {pms['player_name']}"
                
                report.append(penalty_line)
            report.append("")
        
        # Complex scenarios
        if game_penalties['complex_scenarios']:
            report.append("## Complex Scenarios")
            for scenario in game_penalties['complex_scenarios']:
                report.append(f"### {scenario['type'].replace('_', ' ').title()}")
                report.append(f"- **Description**: {scenario['description']}")
                report.append(f"- **Impact**: {scenario['impact']}")
                report.append(f"- **Penalties**: {len(scenario['penalties'])}")
                report.append("")
        
        # Parsing metadata
        report.append("## Parsing Metadata")
        report.append(f"- **Reports Parsed**: {', '.join(game_penalties['parsing_metadata']['reports_parsed'])}")
        report.append(f"- **Total Penalties Found**: {game_penalties['parsing_metadata']['total_penalties_found']}")
        if game_penalties['parsing_metadata']['parsing_errors']:
            report.append("### Parsing Errors")
            for error in game_penalties['parsing_metadata']['parsing_errors']:
                report.append(f"- {error}")
        
        return "\n".join(report)
    
    def determine_period(self, time: str) -> int:
        """Determine period from time string (simplified logic)."""
        try:
            if ':' in time:
                minutes = int(time.split(':')[0])
                if minutes <= 20:
                    return 1
                elif minutes <= 40:
                    return 2
                else:
                    return 3
        except:
            pass
        return 1  # Default to period 1
    
    # Comprehensive data extraction methods using BeautifulSoup
    
    def extract_game_metadata(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Extract game metadata from HTML."""
        metadata = {}
        
        try:
            # Look for game title/header
            title_elements = soup.find_all(['h1', 'h2', 'h3', 'title'])
            for element in title_elements:
                text = element.get_text(strip=True)
                if 'game' in text.lower() or 'summary' in text.lower():
                    metadata['title'] = text
                    break
            
            # Look for game date
            date_patterns = [
                r'(\d{1,2}/\d{1,2}/\d{4})',
                r'(\d{4}-\d{2}-\d{2})',
                r'(\w+ \d{1,2},? \d{4})'
            ]
            
            for pattern in date_patterns:
                matches = re.findall(pattern, soup.get_text())
                if matches:
                    metadata['date'] = matches[0]
                    break
            
            # Look for venue information
            venue_elements = soup.find_all(text=re.compile(r'venue|arena|stadium', re.IGNORECASE))
            if venue_elements:
                metadata['venue'] = venue_elements[0].strip()
            
        except Exception as e:
            self.logger.debug(f"Error extracting game metadata: {e}")
        
        return metadata
    
    def extract_team_info(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Extract team information from HTML."""
        team_info = {'home': {}, 'away': {}}
        
        try:
            # Look for team names and scores
            team_elements = soup.find_all(text=re.compile(r'[A-Z]{3}|[A-Z][a-z]+ [A-Z][a-z]+'))
            
            for element in team_elements:
                text = element.strip()
                if len(text) == 3:  # Team abbreviation
                    if 'home' in element.parent.get_text().lower():
                        team_info['home']['abbreviation'] = text
                    elif 'away' in element.parent.get_text().lower():
                        team_info['away']['abbreviation'] = text
                elif len(text.split()) >= 2:  # Full team name
                    if 'home' in element.parent.get_text().lower():
                        team_info['home']['name'] = text
                    elif 'away' in element.parent.get_text().lower():
                        team_info['away']['name'] = text
            
            # Look for scores
            score_elements = soup.find_all(text=re.compile(r'\d+'))
            for element in score_elements:
                parent_text = element.parent.get_text().lower()
                if 'home' in parent_text and 'score' in parent_text:
                    team_info['home']['score'] = int(element.strip())
                elif 'away' in parent_text and 'score' in parent_text:
                    team_info['away']['score'] = int(element.strip())
                    
        except Exception as e:
            self.logger.debug(f"Error extracting team info: {e}")
        
        return team_info
    
    def extract_scoring_summary(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Extract scoring summary by period from HTML."""
        scoring = {}
        
        try:
            # Look for period headers
            period_headers = soup.find_all(text=re.compile(r'period|1st|2nd|3rd|ot|shootout', re.IGNORECASE))
            
            for header in period_headers:
                period_text = header.strip().lower()
                if '1st' in period_text or 'first' in period_text:
                    scoring['period_1'] = self.extract_period_scoring(header.parent)
                elif '2nd' in period_text or 'second' in period_text:
                    scoring['period_2'] = self.extract_period_scoring(header.parent)
                elif '3rd' in period_text or 'third' in period_text:
                    scoring['period_3'] = self.extract_period_scoring(header.parent)
                elif 'ot' in period_text or 'overtime' in period_text:
                    scoring['overtime'] = self.extract_period_scoring(header.parent)
                elif 'shootout' in period_text:
                    scoring['shootout'] = self.extract_period_scoring(header.parent)
                    
        except Exception as e:
            self.logger.debug(f"Error extracting scoring summary: {e}")
        
        return scoring
    
    def extract_period_scoring(self, period_element) -> List[Dict[str, Any]]:
        """Extract scoring events for a specific period."""
        scoring_events = []
        
        try:
            # Look for scoring rows in tables
            tables = period_element.find_all('table')
            for table in tables:
                rows = table.find_all('tr')
                for row in rows:
                    cells = row.find_all('td')
                    if len(cells) >= 3:
                        event = self.extract_scoring_event(cells)
                        if event:
                            scoring_events.append(event)
                            
        except Exception as e:
            self.logger.debug(f"Error extracting period scoring: {e}")
        
        return scoring_events
    
    def extract_scoring_event(self, cells) -> Optional[Dict[str, Any]]:
        """Extract individual scoring event from table cells."""
        try:
            if len(cells) < 3:
                return None
            
            time_cell = cells[0].get_text(strip=True)
            team_cell = cells[1].get_text(strip=True) if len(cells) > 1 else ""
            desc_cell = cells[2].get_text(strip=True) if len(cells) > 2 else ""
            
            # Parse time
            time_match = re.search(r'(\d{1,2}:\d{2})', time_cell)
            if not time_match:
                return None
            
            time = time_match.group(1)
            
            # Parse goal scorer
            scorer_match = re.search(r'([A-Z][a-z]+ [A-Z][a-z]+)', desc_cell)
            scorer = scorer_match.group(1) if scorer_match else ""
            
            # Parse assists
            assists = []
            assist_matches = re.findall(r'([A-Z][a-z]+ [A-Z][a-z]+)', desc_cell)
            for assist in assist_matches:
                if assist != scorer:
                    assists.append(assist)
            
            return {
                'time': time,
                'team': team_cell.strip(),
                'scorer': scorer,
                'assists': assists,
                'description': desc_cell,
                'type': 'goal'
            }
            
        except Exception as e:
            self.logger.debug(f"Error extracting scoring event: {e}")
            return None
    
    def extract_penalties_by_period(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Extract penalties organized by period from HTML."""
        penalties = {}
        
        try:
            # Look for penalty sections
            penalty_sections = soup.find_all('table', class_='border')
            
            for section in penalty_sections:
                # Try to determine period from context
                period = self.determine_period_from_context(section)
                
                if period not in penalties:
                    penalties[period] = []
                
                rows = section.find_all('tr')
                for row in rows:
                    cells = row.find_all('td')
                    if len(cells) >= 3:
                        penalty_data = self.extract_penalty_from_gs_row(cells)
                        if penalty_data:
                            penalties[period].append(penalty_data)
                            
        except Exception as e:
            self.logger.debug(f"Error extracting penalties by period: {e}")
        
        return penalties
    
    def determine_period_from_context(self, section) -> str:
        """Determine which period a section belongs to based on context."""
        try:
            # Look for period indicators in nearby text
            context_text = section.parent.get_text().lower()
            
            if '1st' in context_text or 'first' in context_text:
                return 'period_1'
            elif '2nd' in context_text or 'second' in context_text:
                return 'period_2'
            elif '3rd' in context_text or 'third' in context_text:
                return 'period_3'
            elif 'ot' in context_text or 'overtime' in context_text:
                return 'overtime'
            else:
                return 'unknown'
                
        except Exception as e:
            self.logger.debug(f"Error determining period from context: {e}")
            return 'unknown'
    
    def extract_goalie_stats(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Extract goalie statistics from HTML."""
        goalie_stats = {'home': {}, 'away': {}}
        
        try:
            # Look for goalie statistics tables
            goalie_tables = soup.find_all('table')
            
            for table in goalie_tables:
                rows = table.find_all('tr')
                for row in rows:
                    cells = row.find_all('td')
                    if len(cells) >= 4:
                        goalie_data = self.extract_goalie_row(cells)
                        if goalie_data:
                            if goalie_data.get('team') == goalie_stats['home'].get('abbreviation'):
                                goalie_stats['home'][goalie_data['name']] = goalie_data
                            else:
                                goalie_stats['away'][goalie_data['name']] = goalie_data
                                
        except Exception as e:
            self.logger.debug(f"Error extracting goalie stats: {e}")
        
        return goalie_stats
    
    def extract_goalie_row(self, cells) -> Optional[Dict[str, Any]]:
        """Extract goalie statistics from table row."""
        try:
            if len(cells) < 4:
                return None
            
            name_cell = cells[0].get_text(strip=True)
            team_cell = cells[1].get_text(strip=True) if len(cells) > 1 else ""
            
            # Parse name
            name_match = re.search(r'([A-Z][a-z]+ [A-Z][a-z]+)', name_cell)
            if not name_match:
                return None
            
            name = name_match.group(1)
            
            # Extract statistics
            stats = {}
            for i, cell in enumerate(cells[2:], 2):
                cell_text = cell.get_text(strip=True)
                if cell_text.isdigit():
                    stats[f'stat_{i-2}'] = int(cell_text)
                else:
                    stats[f'stat_{i-2}'] = cell_text
            
            return {
                'name': name,
                'team': team_cell.strip(),
                'stats': stats
            }
            
        except Exception as e:
            self.logger.debug(f"Error extracting goalie row: {e}")
            return None
    
    def extract_team_stats(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Extract team statistics from HTML."""
        team_stats = {'home': {}, 'away': {}}
        
        try:
            # Look for team statistics tables
            stat_tables = soup.find_all('table')
            
            for table in stat_tables:
                rows = table.find_all('tr')
                for row in rows:
                    cells = row.find_all('td')
                    if len(cells) >= 2:
                        stat_data = self.extract_team_stat_row(cells)
                        if stat_data:
                            if stat_data.get('team') == team_stats['home'].get('abbreviation'):
                                team_stats['home'].update(stat_data['stats'])
                            else:
                                team_stats['away'].update(stat_data['stats'])
                                
        except Exception as e:
            self.logger.debug(f"Error extracting team stats: {e}")
        
        return team_stats
    
    def extract_team_stat_row(self, cells) -> Optional[Dict[str, Any]]:
        """Extract team statistics from table row."""
        try:
            if len(cells) < 2:
                return None
            
            stat_name = cells[0].get_text(strip=True)
            home_value = cells[1].get_text(strip=True) if len(cells) > 1 else ""
            away_value = cells[2].get_text(strip=True) if len(cells) > 2 else ""
            
            # Convert to appropriate type
            try:
                home_value = int(home_value) if home_value.isdigit() else home_value
                away_value = int(away_value) if away_value.isdigit() else away_value
            except:
                pass
            
            return {
                'stat_name': stat_name,
                'home_value': home_value,
                'away_value': away_value
            }
            
        except Exception as e:
            self.logger.debug(f"Error extracting team stat row: {e}")
            return None
    
    def extract_power_play_info(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Extract power play information from HTML."""
        power_plays = {'home': {}, 'away': {}}
        
        try:
            # Look for power play statistics
            pp_elements = soup.find_all(text=re.compile(r'power\s*play|pp', re.IGNORECASE))
            
            for element in pp_elements:
                parent = element.parent
                if parent:
                    # Look for power play numbers
                    numbers = re.findall(r'\d+/\d+', parent.get_text())
                    if numbers:
                        pp_string = numbers[0]
                        goals, attempts = map(int, pp_string.split('/'))
                        
                        # Determine team from context
                        if 'home' in parent.get_text().lower():
                            power_plays['home'] = {'goals': goals, 'attempts': attempts}
                        elif 'away' in parent.get_text().lower():
                            power_plays['away'] = {'goals': goals, 'attempts': attempts}
                            
        except Exception as e:
            self.logger.debug(f"Error extracting power play info: {e}")
        
        return power_plays
    
    def extract_faceoff_info(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Extract faceoff information from HTML."""
        faceoffs = {'home': {}, 'away': {}}
        
        try:
            # Look for faceoff statistics
            fo_elements = soup.find_all(text=re.compile(r'faceoff|face\s*off', re.IGNORECASE))
            
            for element in fo_elements:
                parent = element.parent
                if parent:
                    # Look for faceoff numbers
                    numbers = re.findall(r'\d+/\d+', parent.get_text())
                    if numbers:
                        fo_string = numbers[0]
                        won, total = map(int, fo_string.split('/'))
                        
                        # Determine team from context
                        if 'home' in parent.get_text().lower():
                            faceoffs['home'] = {'won': won, 'total': total}
                        elif 'away' in parent.get_text().lower():
                            faceoffs['away'] = {'won': won, 'total': total}
                            
        except Exception as e:
            self.logger.debug(f"Error extracting faceoff info: {e}")
        
        return faceoffs
    
    def extract_shot_info(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Extract shot information from HTML."""
        shots = {'home': {}, 'away': {}}
        
        try:
            # Look for shot statistics
            shot_elements = soup.find_all(text=re.compile(r'shot|sog|shoot', re.IGNORECASE))
            
            for element in shot_elements:
                parent = element.parent
                if parent:
                    # Look for shot numbers
                    numbers = re.findall(r'\d+', parent.get_text())
                    if len(numbers) >= 2:
                        # Assume first two numbers are shots
                        home_shots = int(numbers[0])
                        away_shots = int(numbers[1])
                        
                        shots['home'] = {'shots': home_shots}
                        shots['away'] = {'shots': away_shots}
                        break
                        
        except Exception as e:
            self.logger.debug(f"Error extracting shot info: {e}")
        
        return shots
    
    def parse_playbyplay_data(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Parse complete Play-by-Play (PL) data using BeautifulSoup."""
        data = {
            'report_type': 'PL',
            'periods': {},
            'events': [],
            'penalties': [],
            'goals': [],
            'faceoffs': [],
            'shots': []
        }
        
        try:
            # Extract events by period
            period_elements = soup.find_all(['h1', 'h2', 'h3', 'div'], class_=re.compile(r'period|page'))
            
            for period_element in period_elements:
                period_text = period_element.get_text().lower()
                period_num = self.determine_period_number(period_text)
                
                if period_num:
                    data['periods'][f'period_{period_num}'] = self.extract_period_events(period_element)
                    
        except Exception as e:
            self.logger.error(f"Error parsing play-by-play data: {e}")
            data['error'] = str(e)
        
        return data
    
    def parse_roster_data(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Parse complete Roster (RO) data using BeautifulSoup."""
        data = {
            'report_type': 'RO',
            'home_roster': [],
            'away_roster': []
        }
        
        try:
            # Extract roster information
            roster_tables = soup.find_all('table')
            
            for table in roster_tables:
                rows = table.find_all('tr')
                for row in rows:
                    cells = row.find_all('td')
                    if len(cells) >= 3:
                        player_data = self.extract_player_roster_data(cells)
                        if player_data:
                            if player_data.get('team') == 'home':
                                data['home_roster'].append(player_data)
                            else:
                                data['away_roster'].append(player_data)
                                
        except Exception as e:
            self.logger.error(f"Error parsing roster data: {e}")
            data['error'] = str(e)
        
        return data
    
    def parse_event_summary_data(self, soup: BeautifulSoup, file_path: Optional[str] = None) -> Dict[str, Any]:
        """
        Parse Event Summary (ES) data with detailed player statistics and penalty information.
        Enhanced version with improved BeautifulSoup parsing and comprehensive data extraction.
        
        Args:
            soup: BeautifulSoup object of the HTML content
            file_path: Optional file path for game ID extraction from filename
            
        Returns:
            Dictionary containing all parsed event summary data
        """
        data = {
            'report_type': 'ES',
            'game_header': {},
            'visitor_team_stats': {},
            'home_team_stats': {},
            'player_statistics': {
                'visitor': [],
                'home': []
            },
            'team_summaries': {
                'visitor': {},
                'home': {}
            },
            'faceoff_summaries': {
                'visitor': {},
                'home': {}
            },
            'parsing_metadata': {
                'timestamp': datetime.now().isoformat(),
                'file_path': str(file_path) if file_path else None,
                'parser_version': '2.0',
                'success': True,
                'errors': []
            }
        }
        
        try:
            # Parse game header (teams, score, date, venue)
            data['game_header'] = self._parse_game_header_enhanced(soup, file_path)
            
            # Store game data and ID for reference data lookup
            self._current_game_data = data['game_header']
            self._current_game_id = data['game_header'].get('game_info', {}).get('game_id')
            
            # Get team IDs from boxscore data
            if self._current_game_id and hasattr(self, 'reference_data'):
                game_id_int = int(self._current_game_id)
                boxscore_data = self.reference_data.get_boxscore_by_id(game_id_int)
                if boxscore_data:
                    # Set team IDs from boxscore data
                    away_team = boxscore_data.get('awayTeam', {})
                    home_team = boxscore_data.get('homeTeam', {})
                    
                    if away_team.get('id'):
                        data['game_header']['visitor_team']['id'] = away_team['id']
                    if home_team.get('id'):
                        data['game_header']['home_team']['id'] = home_team['id']
                    
                    # Update the stored game data with team IDs
                    self._current_game_data = data['game_header']
            
            # Parse visitor team player statistics with enhanced parsing
            data['player_statistics']['visitor'] = self._parse_team_player_stats_enhanced(soup, 'visitor')
            
            # Parse home team player statistics with enhanced parsing
            data['player_statistics']['home'] = self._parse_team_player_stats_enhanced(soup, 'home')
            
            # Parse team summary statistics
            data['visitor_team_stats'] = self._parse_team_summary_stats_enhanced(soup, 'visitor')
            data['home_team_stats'] = self._parse_team_summary_stats_enhanced(soup, 'home')
            
            # Parse faceoff summaries
            data['faceoff_summaries'] = self._parse_faceoff_summaries_enhanced(soup)
            
            # Parse team summaries (goals, shots, etc.)
            data['team_summaries'] = self._parse_team_summaries_enhanced(soup)
            
            # Validate parsed data
            self._validate_es_data(data)
            
        except Exception as e:
            self.logger.error(f"Error parsing event summary data: {e}")
            data['error'] = str(e)
            data['parsing_metadata']['success'] = False
            data['parsing_metadata']['errors'].append(str(e))
        
        return data
    
    def _parse_game_header_enhanced(self, soup: BeautifulSoup, file_path: Optional[str] = None) -> Dict[str, Any]:
        """
        Enhanced game header parsing with better BeautifulSoup usage.
        
        Args:
            soup: BeautifulSoup object of the HTML content
            file_path: Optional file path for game ID extraction
            
        Returns:
            Dictionary containing game header information
        """
        header_data = {
            'game_info': {},
            'visitor_team': {},
            'home_team': {},
            'venue_info': {},
            'game_details': {}
        }
        
        try:
            # Extract game ID from filename if provided
            if file_path:
                file_path_str = str(file_path)
                game_id_match = re.search(r'ES(\d{6})\.HTM', file_path_str)
                if game_id_match:
                    header_data['game_info']['game_id'] = f"2024{game_id_match.group(1)}"
            
            # Parse visitor team information
            visitor_table = soup.find('table', {'id': 'Visitor'})
            if visitor_table:
                # Extract team name and score
                team_name_tds = visitor_table.find_all('td')
                for td in team_name_tds:
                    td_text = td.get_text(strip=True)
                    if re.search(r'GAME \d+ AWAY GAME \d+', td_text):
                        team_name_match = re.search(r'^([^\n]+)', td_text)
                        if team_name_match:
                            header_data['visitor_team']['name'] = team_name_match.group(1).strip()
                        break
                
                # Extract score
                score_elem = visitor_table.find('td', style=re.compile(r'font-size: 40px'))
                if score_elem:
                    header_data['visitor_team']['score'] = int(score_elem.get_text(strip=True))
                
                # Extract team logo/abbreviation from image src
                logo_img = visitor_table.find('img', src=re.compile(r'logoc'))
                if logo_img:
                    logo_src = logo_img.get('src', '')
                    abbrev_match = re.search(r'logoc([a-z]+)', logo_src)
                    if abbrev_match:
                        header_data['visitor_team']['abbreviation'] = abbrev_match.group(1).upper()
            
            # Parse home team information (similar structure)
            home_table = soup.find('table', {'id': 'Home'})
            if home_table:
                # Extract team name and score
                team_name_tds = home_table.find_all('td')
                for td in team_name_tds:
                    td_text = td.get_text(strip=True)
                    if re.search(r'GAME \d+ HOME GAME \d+', td_text):
                        team_name_match = re.search(r'^([^\n]+)', td_text)
                        if team_name_match:
                            header_data['home_team']['name'] = team_name_match.group(1).strip()
                        break
                
                # Extract score
                score_elem = home_table.find('td', style=re.compile(r'font-size: 40px'))
                if score_elem:
                    header_data['home_team']['score'] = int(score_elem.get_text(strip=True))
                
                # Extract team logo/abbreviation from image src
                logo_img = home_table.find('img', src=re.compile(r'logoc'))
                if logo_img:
                    logo_src = logo_img.get('src', '')
                    abbrev_match = re.search(r'logoc([a-z]+)', logo_src)
                    if abbrev_match:
                        header_data['home_team']['abbreviation'] = abbrev_match.group(1).upper()
            
            # Parse game info table
            game_info_table = soup.find('table', {'id': 'GameInfo'})
            if game_info_table:
                # Extract date and venue information
                rows = game_info_table.find_all('tr')
                for row in rows:
                    cells = row.find_all('td')
                    if len(cells) >= 2:
                        key = cells[0].get_text(strip=True)
                        value = cells[1].get_text(strip=True)
                        
                        if 'Date' in key:
                            header_data['game_info']['date'] = value
                        elif 'Venue' in key:
                            header_data['venue_info']['name'] = value
                        elif 'Attendance' in key:
                            header_data['venue_info']['attendance'] = value
            
        except Exception as e:
            self.logger.error(f"Error parsing enhanced game header: {e}")
            header_data['error'] = str(e)
        
        return header_data
    
    def _parse_team_player_stats_enhanced(self, soup: BeautifulSoup, team_type: str) -> List[Dict[str, Any]]:
        """
        Enhanced player statistics parsing with improved BeautifulSoup usage and better data validation.
        
        Args:
            soup: BeautifulSoup object of the HTML content
            team_type: 'visitor' or 'home'
            
        Returns:
            List of player statistics dictionaries
        """
        players = []
        
        try:
            # Find the team-specific section using BeautifulSoup selectors
            # Look for the specific team section heading
            if team_type == 'visitor':
                team_section = soup.find('td', class_='visitorsectionheading')
            else:
                team_section = soup.find('td', class_='homesectionheading')
            
            if not team_section:
                self.logger.warning(f"Could not find {team_type} team section")
                return players
            
            # Find the parent table containing player data
            team_table = team_section.find_parent('table')
            if not team_table:
                self.logger.warning(f"Could not find {team_type} team table")
                return players
            
            # Find all rows in the table
            all_rows = team_table.find_all('tr')
            
            # Find the index of the current team's section heading
            team_section_row = None
            for i, row in enumerate(all_rows):
                if team_section in row.find_all('td'):
                    team_section_row = i
                    break
            
            if team_section_row is None:
                self.logger.warning(f"Could not find {team_type} team section row")
                return players
            
            # Find the next team's section heading to know where to stop
            next_team_section_row = None
            for i in range(team_section_row + 1, len(all_rows)):
                row = all_rows[i]
                # Look for the other team's section heading
                other_team_class = 'homesectionheading' if team_type == 'visitor' else 'visitorsectionheading'
                if row.find('td', class_=other_team_class):
                    next_team_section_row = i
                    break
            
            # If no next team found, use the end of the table
            if next_team_section_row is None:
                next_team_section_row = len(all_rows)
            
            # Extract player rows only from this team's section
            player_rows = []
            for i in range(team_section_row + 1, next_team_section_row):
                row = all_rows[i]
                cells = row.find_all('td')
                if len(cells) >= 25:  # Player stats tables have 25+ columns
                    first_cell_text = cells[0].get_text(strip=True)
                    if re.match(r'^\d+$', first_cell_text):  # First cell is a sweater number
                        player_rows.append(cells)
            
            # Process each player row
            for cells in player_rows:
                player_stats = self._extract_player_stats_from_row_enhanced(cells, team_type)
                if player_stats:
                    # Check for duplicates
                    is_duplicate = any(
                        existing.get('sweater_number') == player_stats.get('sweater_number') and
                        existing.get('name') == player_stats.get('name')
                        for existing in players
                    )
                    
                    if not is_duplicate:
                        players.append(player_stats)
            
        except Exception as e:
            self.logger.error(f"Error parsing enhanced {team_type} team player stats: {e}")
        
        return players
    
    def _extract_player_stats_from_row_enhanced(self, cells: List, team_type: str) -> Dict[str, Any]:
        """
        Enhanced player statistics extraction with better BeautifulSoup usage and comprehensive data validation.
        
        Args:
            cells: List of BeautifulSoup td elements
            team_type: 'visitor' or 'home'
            
        Returns:
            Dictionary with player statistics
        """
        try:
            # Validate input
            if not isinstance(cells, list) or len(cells) < 25:
                return None
            
            # Extract basic player info with validation
            sweater_text = cells[0].get_text(strip=True)
            if not re.match(r'^\d+$', sweater_text):
                return None
            
            sweater_number = int(sweater_text)
            position = cells[1].get_text(strip=True)
            player_name = cells[2].get_text(strip=True)
            
            # Get team ID from game header data
            team_id = None
            if hasattr(self, '_current_game_data') and self._current_game_data:
                if team_type == 'visitor':
                    team_id = self._current_game_data.get('visitor_team', {}).get('id')
                else:
                    team_id = self._current_game_data.get('home_team', {}).get('id')
            
            # Use reference data to get player ID and full name
            player_id = None
            resolved_name = player_name
            if team_id:
                resolved_name = self._resolve_player_name(team_id, sweater_number, player_name)
                
                # Get player ID from reference data
                game_id_int = int(self._current_game_id) if hasattr(self, '_current_game_id') and self._current_game_id else None
                boxscore_data = self.reference_data.get_boxscore_by_id(game_id_int) if game_id_int else None
                if boxscore_data:
                    team_key = 'awayTeam' if team_type == 'visitor' else 'homeTeam'
                    player_stats = boxscore_data.get('playerByGameStats', {}).get(team_key, {})
                    for player_type in ['forwards', 'defense', 'goalies']:
                        for player in player_stats.get(player_type, []):
                            if player.get('sweaterNumber') == sweater_number:
                                player_id = player.get('playerId')
                                break
                        if player_id:
                            break
            
            # Extract statistics with enhanced validation
            goals = self._safe_int_enhanced(cells[3].get_text(strip=True))
            assists = self._safe_int_enhanced(cells[4].get_text(strip=True))
            points = self._safe_int_enhanced(cells[5].get_text(strip=True))
            plus_minus = self._safe_int_enhanced(cells[6].get_text(strip=True))
            penalty_number = self._safe_int_enhanced(cells[7].get_text(strip=True))
            penalty_minutes = self._safe_int_enhanced(cells[8].get_text(strip=True))
            
            # Time on Ice data with better parsing
            toi_total = self._parse_time_string(cells[9].get_text(strip=True))
            shifts = self._safe_int_enhanced(cells[10].get_text(strip=True))
            avg_shift = self._parse_time_string(cells[11].get_text(strip=True))
            toi_pp = self._parse_time_string(cells[12].get_text(strip=True))
            toi_sh = self._parse_time_string(cells[13].get_text(strip=True))
            toi_ev = self._parse_time_string(cells[14].get_text(strip=True))
            
            # Additional stats with validation
            shots = self._safe_int_enhanced(cells[15].get_text(strip=True))
            attempts_blocked = self._safe_int_enhanced(cells[16].get_text(strip=True))
            missed_shots = self._safe_int_enhanced(cells[17].get_text(strip=True))
            hits = self._safe_int_enhanced(cells[18].get_text(strip=True))
            giveaways = self._safe_int_enhanced(cells[19].get_text(strip=True))
            takeaways = self._safe_int_enhanced(cells[20].get_text(strip=True))
            blocked_shots = self._safe_int_enhanced(cells[21].get_text(strip=True))
            faceoffs_won = self._safe_int_enhanced(cells[22].get_text(strip=True))
            faceoffs_lost = self._safe_int_enhanced(cells[23].get_text(strip=True))
            faceoff_percentage = self._safe_float_enhanced(cells[24].get_text(strip=True))
            
            return {
                'player_id': player_id,
                'sweater_number': sweater_number,
                'position': position,
                'name': resolved_name,
                'original_name': player_name,
                'team_id': team_id,
                'team_type': team_type,
                'goals': goals,
                'assists': assists,
                'points': points,
                'plus_minus': plus_minus,
                'penalty_number': penalty_number,
                'penalty_minutes': penalty_minutes,
                'time_on_ice': {
                    'total': toi_total,
                    'shifts': shifts,
                    'avg_shift': avg_shift,
                    'power_play': toi_pp,
                    'short_handed': toi_sh,
                    'even_strength': toi_ev
                },
                'shots': shots,
                'attempts_blocked': attempts_blocked,
                'missed_shots': missed_shots,
                'hits': hits,
                'giveaways': giveaways,
                'takeaways': takeaways,
                'blocked_shots': blocked_shots,
                'faceoffs_won': faceoffs_won,
                'faceoffs_lost': faceoffs_lost,
                'faceoff_percentage': faceoff_percentage,
                'parsing_metadata': {
                    'extracted_at': datetime.now().isoformat(),
                    'data_quality': self._assess_player_data_quality(goals, assists, points, plus_minus)
                }
            }
            
        except Exception as e:
            self.logger.error(f"Error extracting enhanced player stats from row: {e}")
            return None
    
    def _safe_int_enhanced(self, text: str) -> int:
        """Enhanced integer parsing with better error handling."""
        if not text or text.strip() == '' or text.strip() == '&nbsp;':
            return 0
        
        # Clean the text
        cleaned_text = text.strip().replace('&nbsp;', '').replace('+', '')
        
        # Use regex to extract numeric value
        match = re.search(r'-?\d+', cleaned_text)
        if match:
            return int(match.group())
        return 0
    
    def _safe_float_enhanced(self, text: str) -> float:
        """Enhanced float parsing with better error handling."""
        if not text or text.strip() == '' or text.strip() == '&nbsp;':
            return 0.0
        
        # Clean the text
        cleaned_text = text.strip().replace('&nbsp;', '').replace('%', '')
        
        # Use regex to extract numeric value
        match = re.search(r'-?\d+\.?\d*', cleaned_text)
        if match:
            return float(match.group())
        return 0.0
    
    def _parse_time_string(self, time_str: str) -> str:
        """Parse time string and return in consistent format."""
        if not time_str or time_str.strip() == '' or time_str.strip() == '&nbsp;':
            return '00:00'
        
        # Clean the time string
        cleaned_time = time_str.strip().replace('&nbsp;', '')
        
        # Validate time format (MM:SS)
        if re.match(r'^\d{1,2}:\d{2}$', cleaned_time):
            return cleaned_time
        
        return '00:00'
    
    def _assess_player_data_quality(self, goals: int, assists: int, points: int, plus_minus: int) -> str:
        """Assess the quality of player data based on logical consistency."""
        if points == goals + assists:
            return 'high'
        elif abs(points - (goals + assists)) <= 1:
            return 'medium'
        else:
            return 'low'
    
    def _parse_team_summary_stats_enhanced(self, soup: BeautifulSoup, team_type: str) -> Dict[str, Any]:
        """
        Enhanced team summary statistics parsing with better BeautifulSoup usage.
        
        Args:
            soup: BeautifulSoup object of the HTML content
            team_type: 'visitor' or 'home'
            
        Returns:
            Dictionary containing team summary statistics
        """
        team_stats = {}
        
        try:
            # Find team summary section
            team_section = soup.find('td', class_=re.compile(r'visitorsectionheading|homesectionheading'))
            if not team_section:
                return team_stats
            
            # Find the parent table containing team summary data
            team_table = team_section.find_parent('table')
            if not team_table:
                return team_stats
            
            # Look for team summary rows (typically have fewer columns than player rows)
            for row in team_table.find_all('tr'):
                cells = row.find_all('td')
                if len(cells) >= 10 and len(cells) < 25:  # Team summary rows have fewer columns
                    # Check if this is a team summary row
                    first_cell_text = cells[0].get_text(strip=True)
                    if not re.match(r'^\d+$', first_cell_text):  # Not a player row
                        # This might be a team summary row
                        team_stats = self._extract_team_summary_from_row(cells, team_type)
                        break
            
        except Exception as e:
            self.logger.error(f"Error parsing enhanced team summary stats for {team_type}: {e}")
        
        return team_stats
    
    def _extract_team_summary_from_row(self, cells: List, team_type: str) -> Dict[str, Any]:
        """Extract team summary statistics from a table row."""
        try:
            if len(cells) < 10:
                return {}
            
            return {
                'team_type': team_type,
                'goals': self._safe_int_enhanced(cells[3].get_text(strip=True)) if len(cells) > 3 else 0,
                'assists': self._safe_int_enhanced(cells[4].get_text(strip=True)) if len(cells) > 4 else 0,
                'points': self._safe_int_enhanced(cells[5].get_text(strip=True)) if len(cells) > 5 else 0,
                'plus_minus': self._safe_int_enhanced(cells[6].get_text(strip=True)) if len(cells) > 6 else 0,
                'penalty_minutes': self._safe_int_enhanced(cells[8].get_text(strip=True)) if len(cells) > 8 else 0,
                'shots': self._safe_int_enhanced(cells[15].get_text(strip=True)) if len(cells) > 15 else 0,
                'hits': self._safe_int_enhanced(cells[18].get_text(strip=True)) if len(cells) > 18 else 0,
                'blocked_shots': self._safe_int_enhanced(cells[21].get_text(strip=True)) if len(cells) > 21 else 0,
                'faceoffs_won': self._safe_int_enhanced(cells[22].get_text(strip=True)) if len(cells) > 22 else 0,
                'faceoffs_lost': self._safe_int_enhanced(cells[23].get_text(strip=True)) if len(cells) > 23 else 0,
                'faceoff_percentage': self._safe_float_enhanced(cells[24].get_text(strip=True)) if len(cells) > 24 else 0.0
            }
        except Exception as e:
            self.logger.error(f"Error extracting team summary from row: {e}")
            return {}
    
    def _parse_faceoff_summaries_enhanced(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """
        Enhanced faceoff summaries parsing with better BeautifulSoup usage.
        
        Args:
            soup: BeautifulSoup object of the HTML content
            
        Returns:
            Dictionary containing faceoff summaries for both teams
        """
        faceoff_data = {
            'visitor': {},
            'home': {}
        }
        
        try:
            # Find faceoff summary section
            faceoff_section = None
            all_tds = soup.find_all('td')
            for td in all_tds:
                if re.search(r'FACE-OFF SUMMARY', td.get_text(strip=True)):
                    faceoff_section = td
                    break
            
            if not faceoff_section:
                return faceoff_data
            
            # Find the parent table containing faceoff data
            faceoff_table = faceoff_section.find_parent('table')
            if not faceoff_table:
                return faceoff_data
            
            # Parse faceoff data for both teams
            faceoff_rows = faceoff_table.find_all('tr', class_=re.compile(r'oddColor|evenColor'))
            
            for i, row in enumerate(faceoff_rows):
                cells = row.find_all('td')
                if len(cells) >= 4:
                    team_type = 'visitor' if i == 0 else 'home'
                    
                    faceoff_data[team_type] = {
                        'even_strength': self._parse_faceoff_string(cells[0].get_text(strip=True)),
                        'power_play': self._parse_faceoff_string(cells[1].get_text(strip=True)),
                        'short_handed': self._parse_faceoff_string(cells[2].get_text(strip=True)),
                        'total': self._parse_faceoff_string(cells[3].get_text(strip=True))
                    }
            
        except Exception as e:
            self.logger.error(f"Error parsing enhanced faceoff summaries: {e}")
        
        return faceoff_data
    
    def _parse_faceoff_string(self, faceoff_str: str) -> Dict[str, Any]:
        """Parse faceoff string like '16-46/35%' into structured data."""
        try:
            if not faceoff_str or faceoff_str.strip() == '':
                return {'won': 0, 'total': 0, 'percentage': 0.0}
            
            # Parse format like "16-46/35%"
            match = re.match(r'(\d+)-(\d+)/(\d+)%', faceoff_str.strip())
            if match:
                won = int(match.group(1))
                total = int(match.group(2))
                percentage = float(match.group(3))
                
                return {
                    'won': won,
                    'total': total,
                    'percentage': percentage
                }
            
            return {'won': 0, 'total': 0, 'percentage': 0.0}
        except Exception as e:
            self.logger.error(f"Error parsing faceoff string '{faceoff_str}': {e}")
            return {'won': 0, 'total': 0, 'percentage': 0.0}
    
    def _parse_team_summaries_enhanced(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """
        Enhanced team summaries parsing (goals, shots, etc.) with better BeautifulSoup usage.
        
        Args:
            soup: BeautifulSoup object of the HTML content
            
        Returns:
            Dictionary containing team summaries for both teams
        """
        team_summaries = {
            'visitor': {},
            'home': {}
        }
        
        try:
            # Find team summary section
            team_summary_section = None
            all_tds = soup.find_all('td')
            for td in all_tds:
                if re.search(r'TEAM SUMMARY', td.get_text(strip=True)):
                    team_summary_section = td
                    break
            
            if not team_summary_section:
                return team_summaries
            
            # Find the parent table containing team summary data
            team_summary_table = team_summary_section.find_parent('table')
            if not team_summary_table:
                return team_summaries
            
            # Parse team summary data
            # Look for rows with team statistics
            for row in team_summary_table.find_all('tr'):
                cells = row.find_all('td')
                if len(cells) >= 10:
                    # Check if this is a team summary row
                    first_cell_text = cells[0].get_text(strip=True)
                    if not re.match(r'^\d+$', first_cell_text):  # Not a player row
                        # This might be a team summary row
                        team_summary = self._extract_team_summary_from_row(cells, 'unknown')
                        if team_summary:
                            # Determine team type based on context
                            team_type = 'visitor' if len(team_summaries['visitor']) == 0 else 'home'
                            team_summaries[team_type] = team_summary
            
        except Exception as e:
            self.logger.error(f"Error parsing enhanced team summaries: {e}")
        
        return team_summaries
    
    def _validate_es_data(self, data: Dict[str, Any]) -> None:
        """
        Validate parsed ES data for consistency and quality.
        
        Args:
            data: Parsed ES data dictionary
        """
        try:
            validation_errors = []
            
            # Validate game header
            if not data.get('game_header', {}).get('game_info', {}).get('game_id'):
                validation_errors.append("Missing game ID")
            
            # Validate team data
            visitor_team = data.get('game_header', {}).get('visitor_team', {})
            home_team = data.get('game_header', {}).get('home_team', {})
            
            if not visitor_team.get('name'):
                validation_errors.append("Missing visitor team name")
            
            if not home_team.get('name'):
                validation_errors.append("Missing home team name")
            
            # Validate player statistics
            visitor_players = data.get('player_statistics', {}).get('visitor', [])
            home_players = data.get('player_statistics', {}).get('home', [])
            
            if len(visitor_players) == 0:
                validation_errors.append("No visitor team players found")
            
            if len(home_players) == 0:
                validation_errors.append("No home team players found")
            
            # Validate player data quality
            for player in visitor_players + home_players:
                if player.get('goals', 0) + player.get('assists', 0) != player.get('points', 0):
                    validation_errors.append(f"Points mismatch for player {player.get('name', 'Unknown')}")
            
            # Add validation results to metadata
            if validation_errors:
                data['parsing_metadata']['validation_errors'] = validation_errors
                data['parsing_metadata']['data_quality'] = 'low'
            else:
                data['parsing_metadata']['data_quality'] = 'high'
            
        except Exception as e:
            self.logger.error(f"Error validating ES data: {e}")
            data['parsing_metadata']['validation_errors'] = [f"Validation error: {str(e)}"]
            data['parsing_metadata']['data_quality'] = 'unknown'
    
    def _parse_team_player_stats(self, soup: BeautifulSoup, team_type: str) -> List[Dict[str, Any]]:
        """
        Parse player statistics for a specific team from Event Summary using BeautifulSoup.
        
        Args:
            soup: BeautifulSoup object of the HTML content
            team_type: 'visitor' or 'home'
            
        Returns:
            List of player statistics dictionaries
        """
        players = []
        
        try:
            # Find the team-specific table using the ID attribute to determine team context
            team_table_id = 'Visitor' if team_type == 'visitor' else 'Home'
            team_table = soup.find('table', {'id': team_table_id})
            
            if not team_table:
                self.logger.warning(f"Could not find table with id='{team_table_id}' for {team_type} team")
                return players
            
            # Find all tables in the document that contain player data
            # Look for tables with many columns (25+ columns indicate player stats tables)
            all_tables = soup.find_all('table')
            
            for table in all_tables:
                # Check if this table has the structure of a player stats table
                rows = table.find_all('tr')
                if len(rows) < 5:  # Skip small tables
                    continue
                    
                # Check if any row has 25+ cells (player stats table structure)
                has_player_rows = False
                for row in rows:
                    cells = row.find_all('td')
                    if len(cells) >= 25:  # Player stats tables have 25+ columns
                        first_cell_text = cells[0].get_text(strip=True)
                        if re.match(r'^\d+$', first_cell_text):  # First cell is a sweater number
                            has_player_rows = True
                            break
                
                if has_player_rows:
                    # This is a player stats table, process it
                    for row in rows:
                        cells = row.find_all('td')
                        if len(cells) >= 25:  # Ensure we have enough columns for player stats
                            # Check if first cell contains a sweater number (digit)
                            first_cell_text = cells[0].get_text(strip=True)
                            if re.match(r'^\d+$', first_cell_text):  # Regex to match only digits
                                player_stats = self._extract_player_stats_from_row_bs4(cells, team_type)
                                if player_stats:
                                    # Check for duplicates based on player_id and sweater_number
                                    is_duplicate = False
                                    for existing_player in players:
                                        if (existing_player.get('player_id') == player_stats.get('player_id') and 
                                            existing_player.get('sweater_number') == player_stats.get('sweater_number')):
                                            is_duplicate = True
                                            break
                                    
                                    if not is_duplicate:
                                        players.append(player_stats)
                                
        except Exception as e:
            self.logger.error(f"Error parsing {team_type} team player stats: {e}")
        
        return players
    
    def _extract_player_stats_from_row_bs4(self, cells: List, team_type: str) -> Dict[str, Any]:
        """
        Extract player statistics from a table row using BeautifulSoup and regex.
        
        Args:
            cells: List of BeautifulSoup td elements
            team_type: 'visitor' or 'home'
            
        Returns:
            Dictionary with player statistics
        """
        try:
            # Ensure cells is a list and has enough elements
            if not isinstance(cells, list) or len(cells) < 25:
                return None
                
            # Extract basic player info using regex for validation
            sweater_text = cells[0].get_text(strip=True)
            if not re.match(r'^\d+$', sweater_text):
                return None
                
            sweater_number = int(sweater_text)
            position = cells[1].get_text(strip=True)
            player_name = cells[2].get_text(strip=True)
            
            # Get team ID from game header data
            team_id = None
            if hasattr(self, '_current_game_data') and self._current_game_data:
                if team_type == 'visitor':
                    team_id = self._current_game_data.get('visitor_team', {}).get('id')
                else:
                    team_id = self._current_game_data.get('home_team', {}).get('id')
            
            # Use reference data to get player ID and full name
            player_id = None
            resolved_name = player_name
            if team_id:
                # Try to resolve player name using reference data
                resolved_name = self._resolve_player_name(team_id, sweater_number, player_name)
                
                # Get player ID from reference data
                boxscore_data = self.reference_data.get_boxscore_by_id(self._current_game_id) if hasattr(self, '_current_game_id') else None
                if boxscore_data:
                    team_key = 'awayTeam' if team_type == 'visitor' else 'homeTeam'
                    player_stats = boxscore_data.get('playerByGameStats', {}).get(team_key, {})
                    for player_type in ['forwards', 'defense', 'goalies']:
                        for player in player_stats.get(player_type, []):
                            if player.get('sweaterNumber') == sweater_number:
                                player_id = player.get('playerId')
                                break
                        if player_id:
                            break
            
            # Extract statistics using regex for numeric validation
            goals = self._safe_int_regex(cells[3].get_text(strip=True))
            assists = self._safe_int_regex(cells[4].get_text(strip=True))
            points = self._safe_int_regex(cells[5].get_text(strip=True))
            plus_minus = self._safe_int_regex(cells[6].get_text(strip=True))
            penalty_number = self._safe_int_regex(cells[7].get_text(strip=True))  # PN
            penalty_minutes = self._safe_int_regex(cells[8].get_text(strip=True))  # PIM
            
            # Time on Ice data
            toi_total = cells[9].get_text(strip=True)
            shifts = self._safe_int_regex(cells[10].get_text(strip=True))
            avg_shift = cells[11].get_text(strip=True)
            toi_pp = cells[12].get_text(strip=True)
            toi_sh = cells[13].get_text(strip=True)
            toi_ev = cells[14].get_text(strip=True)
            
            # Additional stats
            shots = self._safe_int_regex(cells[15].get_text(strip=True))
            attempts_blocked = self._safe_int_regex(cells[16].get_text(strip=True))
            missed_shots = self._safe_int_regex(cells[17].get_text(strip=True))
            hits = self._safe_int_regex(cells[18].get_text(strip=True))
            giveaways = self._safe_int_regex(cells[19].get_text(strip=True))
            takeaways = self._safe_int_regex(cells[20].get_text(strip=True))
            blocked_shots = self._safe_int_regex(cells[21].get_text(strip=True))
            faceoffs_won = self._safe_int_regex(cells[22].get_text(strip=True))
            faceoffs_lost = self._safe_int_regex(cells[23].get_text(strip=True))
            faceoff_percentage = self._safe_float_regex(cells[24].get_text(strip=True))
            
            return {
                'player_id': player_id,
                'sweater_number': sweater_number,
                'position': position,
                'name': resolved_name,
                'original_name': player_name,
                'team_id': team_id,
                'team_type': team_type,
                'goals': goals,
                'assists': assists,
                'points': points,
                'plus_minus': plus_minus,
                'penalty_number': penalty_number,
                'penalty_minutes': penalty_minutes,
                'time_on_ice': {
                    'total': toi_total,
                    'shifts': shifts,
                    'avg_shift': avg_shift,
                    'power_play': toi_pp,
                    'short_handed': toi_sh,
                    'even_strength': toi_ev
                },
                'shots': shots,
                'attempts_blocked': attempts_blocked,
                'missed_shots': missed_shots,
                'hits': hits,
                'giveaways': giveaways,
                'takeaways': takeaways,
                'blocked_shots': blocked_shots,
                'faceoffs_won': faceoffs_won,
                'faceoffs_lost': faceoffs_lost,
                'faceoff_percentage': faceoff_percentage
            }
            
        except Exception as e:
            self.logger.error(f"Error extracting player stats from row: {e}")
            import traceback
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            return None
    
    def _safe_int_regex(self, text: str) -> int:
        """Safely convert text to int using regex to extract numeric values."""
        if not text or text.strip() == '' or text.strip() == '&nbsp;':
            return 0
        
        # Use regex to extract numeric value (including negative numbers)
        match = re.search(r'-?\d+', text.strip())
        if match:
            return int(match.group())
        return 0
    
    def _safe_float_regex(self, text: str) -> float:
        """Safely convert text to float using regex to extract numeric values."""
        if not text or text.strip() == '' or text.strip() == '&nbsp;':
            return 0.0
        
        # Use regex to extract numeric value (including decimals and negative numbers)
        match = re.search(r'-?\d+\.?\d*', text.strip())
        if match:
            return float(match.group())
        return 0.0
    
    def _extract_player_stats_from_row(self, cells: List, team_type: str) -> Dict[str, Any]:
        """
        Extract player statistics from a table row.
        
        Args:
            cells: List of table cells
            team_type: 'visitor' or 'home'
            
        Returns:
            Dictionary with player statistics
        """
        try:
            # Extract basic player info
            sweater_number = int(cells[0].get_text(strip=True))
            position = cells[1].get_text(strip=True)
            player_name = cells[2].get_text(strip=True)
            
            # Get team ID from game header data
            team_id = None
            if hasattr(self, '_current_game_data') and self._current_game_data:
                if team_type == 'visitor':
                    team_id = self._current_game_data.get('visitor_team', {}).get('id')
                else:
                    team_id = self._current_game_data.get('home_team', {}).get('id')
            
            # Use reference data to get player ID and full name
            player_id = None
            resolved_name = player_name
            if team_id:
                # Try to resolve player name using reference data
                resolved_name = self._resolve_player_name(team_id, sweater_number, player_name)
                
                # Get player ID from reference data
                boxscore_data = self.reference_data.get_boxscore_by_id(self._current_game_id) if hasattr(self, '_current_game_id') else None
                if boxscore_data:
                    team_key = 'awayTeam' if team_type == 'visitor' else 'homeTeam'
                    player_stats = boxscore_data.get('playerByGameStats', {}).get(team_key, {})
                    for player_type in ['forwards', 'defense', 'goalies']:
                        for player in player_stats.get(player_type, []):
                            if player.get('sweaterNumber') == sweater_number:
                                player_id = player.get('playerId')
                                break
                        if player_id:
                            break
            
            # Extract statistics (based on ES file structure)
            goals = self._safe_int(cells[3].get_text(strip=True))
            assists = self._safe_int(cells[4].get_text(strip=True))
            points = self._safe_int(cells[5].get_text(strip=True))
            plus_minus = self._safe_int(cells[6].get_text(strip=True))
            penalty_number = self._safe_int(cells[7].get_text(strip=True))  # PN
            penalty_minutes = self._safe_int(cells[8].get_text(strip=True))  # PIM
            
            # Time on Ice data
            toi_total = cells[9].get_text(strip=True)
            shifts = self._safe_int(cells[10].get_text(strip=True))
            avg_shift = cells[11].get_text(strip=True)
            toi_pp = cells[12].get_text(strip=True)
            toi_sh = cells[13].get_text(strip=True)
            toi_ev = cells[14].get_text(strip=True)
            
            # Additional stats
            shots = self._safe_int(cells[15].get_text(strip=True))
            attempts_blocked = self._safe_int(cells[16].get_text(strip=True))
            missed_shots = self._safe_int(cells[17].get_text(strip=True))
            hits = self._safe_int(cells[18].get_text(strip=True))
            giveaways = self._safe_int(cells[19].get_text(strip=True))
            takeaways = self._safe_int(cells[20].get_text(strip=True))
            blocked_shots = self._safe_int(cells[21].get_text(strip=True))
            faceoffs_won = self._safe_int(cells[22].get_text(strip=True))
            faceoffs_lost = self._safe_int(cells[23].get_text(strip=True))
            faceoff_percentage = self._safe_float(cells[24].get_text(strip=True))
            
            return {
                'player_id': player_id,
                'sweater_number': sweater_number,
                'position': position,
                'name': resolved_name,
                'original_name': player_name,
                'team_id': team_id,
                'team_type': team_type,
                'goals': goals,
                'assists': assists,
                'points': points,
                'plus_minus': plus_minus,
                'penalty_number': penalty_number,
                'penalty_minutes': penalty_minutes,
                'time_on_ice': {
                    'total': toi_total,
                    'shifts': shifts,
                    'avg_shift': avg_shift,
                    'power_play': toi_pp,
                    'short_handed': toi_sh,
                    'even_strength': toi_ev
                },
                'shots': shots,
                'attempts_blocked': attempts_blocked,
                'missed_shots': missed_shots,
                'hits': hits,
                'giveaways': giveaways,
                'takeaways': takeaways,
                'blocked_shots': blocked_shots,
                'faceoffs': {
                    'won': faceoffs_won,
                    'lost': faceoffs_lost,
                    'percentage': faceoff_percentage
                }
            }
            
        except Exception as e:
            self.logger.error(f"Error extracting player stats from row: {e}")
            return None
    
    def _parse_team_summary_stats(self, soup: BeautifulSoup, team_type: str) -> Dict[str, Any]:
        """
        Parse team summary statistics from Event Summary.
        
        Args:
            soup: BeautifulSoup object of the HTML content
            team_type: 'visitor' or 'home'
            
        Returns:
            Dictionary with team summary statistics
        """
        # This would parse team totals, power play stats, etc.
        # For now, return placeholder
        return {
            'team_type': team_type,
            'message': 'Team summary stats parsing not yet implemented'
        }
    
    def _safe_int(self, value: str) -> int:
        """Safely convert string to integer, returning 0 for empty/invalid values."""
        if not value or value.strip() == '' or value.strip() == '&nbsp;':
            return 0
        try:
            return int(value.strip())
        except ValueError:
            return 0
    
    def _safe_float(self, value: str) -> float:
        """Safely convert string to float, returning 0.0 for empty/invalid values."""
        if not value or value.strip() == '' or value.strip() == '&nbsp;':
            return 0.0
        try:
            return float(value.strip())
        except ValueError:
            return 0.0
    
    def parse_shot_summary_data(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Parse complete Shot Summary (SS) data using BeautifulSoup."""
        data = {
            'report_type': 'SS',
            'shots_by_period': {},
            'shot_types': {},
            'shot_locations': {}
        }
        
        try:
            # Extract shot information by period
            shot_tables = soup.find_all('table')
            
            for table in shot_tables:
                rows = table.find_all('tr')
                for row in rows:
                    cells = row.find_all('td')
                    if len(cells) >= 4:
                        shot_data = self.extract_shot_data(cells)
                        if shot_data:
                            period = shot_data.get('period', 'unknown')
                            if period not in data['shots_by_period']:
                                data['shots_by_period'][period] = []
                            data['shots_by_period'][period].append(shot_data)
                            
        except Exception as e:
            self.logger.error(f"Error parsing shot summary data: {e}")
            data['error'] = str(e)
        
        return data
    
    def parse_faceoff_summary_data(self, soup: BeautifulSoup, file_path: Optional[str] = None) -> Dict[str, Any]:
        """
        Parse complete Faceoff Summary (FS) data using BeautifulSoup.
        
        Args:
            soup: BeautifulSoup object of the HTML content
            file_path: Optional file path for game ID extraction from filename
            
        Returns:
            Dictionary containing all parsed faceoff summary data
        """
        data = {
            'report_type': 'FS',
            'game_header': {},
            'faceoffs_by_period': {},
            'player_faceoffs': {
                'visitor': [],
                'home': []
            },
            'team_totals': {
                'visitor': {},
                'home': {}
            }
        }
        
        try:
            # Parse game header (teams, score, date, venue)
            data['game_header'] = self._parse_game_header(soup, file_path)
            
            # Store game data and ID for reference data lookup
            self._current_game_data = data['game_header']
            self._current_game_id = data['game_header'].get('game_info', {}).get('game_id')
            
            # Parse faceoff data by period and player
            data['faceoffs_by_period'] = self._parse_faceoffs_by_period(soup)
            
            # Parse player faceoff statistics
            data['player_faceoffs']['visitor'] = self._parse_team_faceoff_stats(soup, 'visitor')
            data['player_faceoffs']['home'] = self._parse_team_faceoff_stats(soup, 'home')
            
            # Parse team totals
            data['team_totals'] = self._parse_faceoff_team_totals(soup)
            
        except Exception as e:
            self.logger.error(f"Error parsing faceoff summary data: {e}")
            data['error'] = str(e)
        
        return data
    
    def _parse_faceoffs_by_period(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """
        Parse faceoff data organized by period.
        
        Args:
            soup: BeautifulSoup object of the HTML content
            
        Returns:
            Dictionary with faceoff data by period
        """
        faceoffs_by_period = {}
        
        try:
            # Look for tables with faceoff data
            tables = soup.find_all('table')
            
            for table in tables:
                rows = table.find_all('tr')
                for row in rows:
                    cells = row.find_all('td')
                    if len(cells) >= 3:
                        # Check if this row contains faceoff data
                        faceoff_data = self._extract_faceoff_data_from_row(cells)
                        if faceoff_data:
                            period = faceoff_data.get('period', 'unknown')
                            if period not in faceoffs_by_period:
                                faceoffs_by_period[period] = []
                            faceoffs_by_period[period].append(faceoff_data)
                            
        except Exception as e:
            self.logger.error(f"Error parsing faceoffs by period: {e}")
        
        return faceoffs_by_period
    
    def _parse_team_faceoff_stats(self, soup: BeautifulSoup, team_type: str) -> List[Dict[str, Any]]:
        """
        Parse faceoff statistics for a specific team.
        
        Args:
            soup: BeautifulSoup object of the HTML content
            team_type: 'visitor' or 'home'
            
        Returns:
            List of player faceoff statistics
        """
        players = []
        
        try:
            # Find the PlayerTable which contains detailed faceoff data
            player_table = soup.find('table', id='PlayerTable')
            if not player_table:
                return players
            
            # Look for player headers and their associated faceoff data
            rows = player_table.find_all('tr')
            current_player = None
            current_team = None
            team_header_count = 0
            
            for row in rows:
                cells = row.find_all('td')
                if len(cells) >= 1:
                    # Prefer detecting team headers via class
                    cell_classes = cells[0].get('class') or []
                    if any('teamHeading' in cls for cls in cell_classes):
                        current_team = 'visitor' if team_header_count == 0 else 'home'
                        team_header_count += 1
                        continue
                    
                    cell_text = cells[0].get_text(strip=True)
                    
                    # Skip the first row which contains concatenated data (has many cells)
                    if len(cells) > 100:
                        continue
                    
                    # Only process players for the requested team
                    if current_team != team_type:
                        continue
                    
                    # Check if this is a player header (e.g., "13 C HISCHIER, NICO")
                    # Look for pattern like "13 C HISCHIER, NICO" (sweater number, position, name)
                    player_match = re.search(r'(\d+)\s+([A-Z])\s+([A-Z\s,]+)', cell_text)
                    if player_match and len(cells) == 1:  # Player headers are typically single-cell rows
                        sweater_number = int(player_match.group(1))
                        position = player_match.group(2)
                        player_name = player_match.group(3).strip()
                        
                        # Get team ID from game header data
                        team_id = None
                        if hasattr(self, '_current_game_data') and self._current_game_data:
                            if team_type == 'visitor':
                                team_id = self._current_game_data.get('visitor_team', {}).get('id')
                            else:
                                team_id = self._current_game_data.get('home_team', {}).get('id')
                        
                        # Use reference data to get player ID and full name
                        player_id = None
                        resolved_name = player_name
                        if team_id:
                            # Try to resolve player name using reference data
                            resolved_name = self._resolve_player_name(team_id, sweater_number, player_name)
                            
                            # Get player ID from reference data
                            boxscore_data = self.reference_data.get_boxscore_by_id(self._current_game_id) if hasattr(self, '_current_game_id') else None
                            if boxscore_data:
                                team_key = 'awayTeam' if team_type == 'visitor' else 'homeTeam'
                                player_stats = boxscore_data.get('playerByGameStats', {}).get(team_key, {})
                                for player_type in ['forwards', 'defense', 'goalies']:
                                    for player in player_stats.get(player_type, []):
                                        if player.get('sweaterNumber') == sweater_number:
                                            player_id = player.get('playerId')
                                            break
                                    if player_id:
                                        break
                        
                        current_player = {
                            'player_id': player_id,
                            'sweater_number': sweater_number,
                            'position': position,
                            'name': resolved_name,
                            'original_name': player_name,
                            'team_id': team_id,
                            'team_type': team_type,
                            'faceoff_details': []
                        }
                        players.append(current_player)
                    
                    # If we have a current player and this row contains faceoff data
                    elif current_player and len(cells) >= 5:
                        # Dynamically detect any strength label like "NvM" (e.g., 6v5, 5v3) or known tokens like TOT
                        strength = cells[0].get_text(strip=True)
                        is_strength = bool(re.match(r"^\d+v\d+$", strength, flags=re.IGNORECASE)) or strength.upper() == 'TOT'
                        if is_strength:
                            # Extract faceoff data from cells 1-4 (Off, Def, Neu, TOT)
                            faceoff_data = []
                            for cell in cells[1:5]:  # Skip first cell (strength info)
                                cell_text = cell.get_text(strip=True)
                                if cell_text and '/' in cell_text and '%' in cell_text:
                                    faceoff_match = re.search(r'(\d+)-(\d+)/(\d+)%', cell_text)
                                    if faceoff_match:
                                        won = int(faceoff_match.group(1))
                                        total = int(faceoff_match.group(2))
                                        percentage = int(faceoff_match.group(3))
                                        lost = total - won
                                        
                                        faceoff_data.append({
                                            'won': won,
                                            'lost': lost,
                                            'total': total,
                                            'percentage': percentage,
                                            'raw_text': cell_text
                                        })
                                    else:
                                        faceoff_data.append(None)
                                else:
                                    faceoff_data.append(None)
                            
                            # Add faceoff detail if we have any data
                            if any(faceoff_data):
                                current_player['faceoff_details'].append({
                                    'strength': strength,
                                    'offensive_zone': faceoff_data[0],
                                    'defensive_zone': faceoff_data[1],
                                    'neutral_zone': faceoff_data[2],
                                    'total': faceoff_data[3] if len(faceoff_data) > 3 else None
                                })
                                
        except Exception as e:
            self.logger.error(f"Error parsing {team_type} team faceoff stats: {e}")
        
        return players
    
    def _extract_faceoff_data_from_row(self, cells: List) -> Optional[Dict[str, Any]]:
        """
        Extract faceoff data from a table row.
        
        Args:
            cells: List of table cells
            
        Returns:
            Dictionary with faceoff data or None
        """
        try:
            # Look for faceoff data pattern (e.g., "4-14/29%")
            for cell in cells:
                cell_text = cell.get_text(strip=True)
                if '/' in cell_text and '%' in cell_text:
                    # Parse faceoff data: "4-14/29%"
                    faceoff_match = re.search(r'(\d+)-(\d+)/(\d+)%', cell_text)
                    if faceoff_match:
                        won = int(faceoff_match.group(1))
                        total = int(faceoff_match.group(2))
                        percentage = int(faceoff_match.group(3))
                        lost = total - won
                        
                        return {
                            'won': won,
                            'lost': lost,
                            'total': total,
                            'percentage': percentage,
                            'raw_text': cell_text
                        }
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error extracting faceoff data from row: {e}")
            return None
    
    def _extract_player_faceoff_stats(self, cells: List, team_type: str) -> Optional[Dict[str, Any]]:
        """
        Extract player faceoff statistics from a table row.
        
        Args:
            cells: List of table cells
            team_type: 'visitor' or 'home'
            
        Returns:
            Dictionary with player faceoff statistics
        """
        try:
            # Extract basic player info
            sweater_number = int(cells[0].get_text(strip=True))
            player_name = cells[1].get_text(strip=True) if len(cells) > 1 else ""
            
            # Get team ID from game header data
            team_id = None
            if hasattr(self, '_current_game_data') and self._current_game_data:
                if team_type == 'visitor':
                    team_id = self._current_game_data.get('visitor_team', {}).get('id')
                else:
                    team_id = self._current_game_data.get('home_team', {}).get('id')
            
            # Use reference data to get player ID and full name
            player_id = None
            resolved_name = player_name
            if team_id:
                # Try to resolve player name using reference data
                resolved_name = self._resolve_player_name(team_id, sweater_number, player_name)
                
                # Get player ID from reference data
                boxscore_data = self.reference_data.get_boxscore_by_id(self._current_game_id) if hasattr(self, '_current_game_id') else None
                if boxscore_data:
                    team_key = 'awayTeam' if team_type == 'visitor' else 'homeTeam'
                    player_stats = boxscore_data.get('playerByGameStats', {}).get(team_key, {})
                    for player_type in ['forwards', 'defense', 'goalies']:
                        for player in player_stats.get(player_type, []):
                            if player.get('sweaterNumber') == sweater_number:
                                player_id = player.get('playerId')
                                break
                        if player_id:
                            break
            
            # Extract faceoff data from cells
            faceoff_data = None
            for cell in cells:
                cell_text = cell.get_text(strip=True)
                if '/' in cell_text and '%' in cell_text:
                    faceoff_match = re.search(r'(\d+)-(\d+)/(\d+)%', cell_text)
                    if faceoff_match:
                        won = int(faceoff_match.group(1))
                        total = int(faceoff_match.group(2))
                        percentage = int(faceoff_match.group(3))
                        lost = total - won
                        
                        faceoff_data = {
                            'won': won,
                            'lost': lost,
                            'total': total,
                            'percentage': percentage,
                            'raw_text': cell_text
                        }
                        break
            
            if faceoff_data:
                return {
                    'player_id': player_id,
                    'sweater_number': sweater_number,
                    'name': resolved_name,
                    'original_name': player_name,
                    'team_id': team_id,
                    'team_type': team_type,
                    'faceoffs': faceoff_data
                }
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error extracting player faceoff stats from row: {e}")
            return None
    
    def _parse_faceoff_team_totals(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """
        Parse team faceoff totals from Faceoff Summary.
        
        Args:
            soup: BeautifulSoup object of the HTML content
            
        Returns:
            Dictionary with team totals
        """
        team_totals = {
            'visitor': {},
            'home': {}
        }
        
        try:
            # Find the team summary table
            tables = soup.find_all('table')
            team_summary_table = None
            
            for table in tables:
                rows = table.find_all('tr')
                if rows:
                    first_row_text = rows[0].get_text(strip=True)
                    if 'TEAM SUMMARY' in first_row_text:
                        team_summary_table = table
                        break
            
            if not team_summary_table:
                return team_totals
            
            rows = team_summary_table.find_all('tr')
            current_team = None
            team_header_count = 0
            
            for row in rows:
                cells = row.find_all('td')
                if not cells:
                    continue
                
                cell_texts = [c.get_text(strip=True) for c in cells]
                
                # Check for team headers dynamically via class
                first_cell_classes = cells[0].get('class') or []
                if any('teamHeading' in cls for cls in first_cell_classes):
                    current_team = 'visitor' if team_header_count == 0 else 'home'
                    team_header_count += 1
                    continue
                
                # Skip header rows
                if any(text in ['Per', 'EV', 'PP', 'SH', 'TOT', 'Zone', 'Strength', 'Off.', 'Def.', 'Neu.'] for text in cell_texts):
                    continue
                
                # Parse period data (rows with period numbers or OT variants)
                if len(cell_texts) >= 5 and (re.match(r'^\d+$', cell_texts[0]) or re.match(r'^OT\d*$', cell_texts[0], flags=re.IGNORECASE)):
                    if current_team:
                        period_token = cell_texts[0]
                        if period_token.isdigit():
                            period_key = f'period_{int(period_token)}'
                        else:
                            # Handle OT, OT2, etc.
                            ot_suffix = period_token[2:]
                            period_key = 'period_ot' if ot_suffix == '' else f'period_ot{ot_suffix}'
                        
                        if period_key not in team_totals[current_team]:
                            team_totals[current_team][period_key] = {}
                        
                        # Parse faceoff data: EV, PP, SH, TOT
                        team_totals[current_team][period_key]['even_strength'] = self._parse_faceoff_stat(cell_texts[1])
                        team_totals[current_team][period_key]['power_play'] = self._parse_faceoff_stat(cell_texts[2])
                        team_totals[current_team][period_key]['penalty_kill'] = self._parse_faceoff_stat(cell_texts[3])
                        team_totals[current_team][period_key]['total'] = self._parse_faceoff_stat(cell_texts[4])
                
                # Parse strength data (rows with NvM like 5v5, 5v4, 4v5, 3v5, 6v5, etc., or TOT)
                elif len(cell_texts) >= 5 and (re.match(r"^\d+v\d+$", cell_texts[0], flags=re.IGNORECASE) or cell_texts[0].upper() == 'TOT'):
                    if current_team:
                        strength = cell_texts[0]
                        strength_key = f'strength_{strength}'
                        
                        if strength_key not in team_totals[current_team]:
                            team_totals[current_team][strength_key] = {}
                        
                        # Parse zone data: Off, Def, Neu, TOT
                        team_totals[current_team][strength_key]['offensive_zone'] = self._parse_faceoff_stat(cell_texts[1])
                        team_totals[current_team][strength_key]['defensive_zone'] = self._parse_faceoff_stat(cell_texts[2])
                        team_totals[current_team][strength_key]['neutral_zone'] = self._parse_faceoff_stat(cell_texts[3])
                        team_totals[current_team][strength_key]['total'] = self._parse_faceoff_stat(cell_texts[4])
                        
                        
        except Exception as e:
            self.logger.error(f"Error parsing faceoff team totals: {e}")
        
        return team_totals
    
    def _parse_faceoff_stat(self, stat_text: str) -> Dict[str, Any]:
        """
        Parse a faceoff statistic string like "16-46/35%".
        
        Args:
            stat_text: Faceoff statistic string
            
        Returns:
            Dictionary with won, lost, total, percentage
        """
        if not stat_text or stat_text == '':
            return None
        
        try:
            # Parse format like "16-46/35%"
            match = re.search(r'(\d+)-(\d+)/(\d+)%', stat_text)
            if match:
                won = int(match.group(1))
                total = int(match.group(2))
                percentage = int(match.group(3))
                lost = total - won
                
                return {
                    'won': won,
                    'lost': lost,
                    'total': total,
                    'percentage': percentage,
                    'raw_text': stat_text
                }
        except Exception as e:
            self.logger.error(f"Error parsing faceoff stat '{stat_text}': {e}")
        
        return None
    
    def _normalize_strength_label(self, strength: str, team_type: Optional[str] = None) -> Dict[str, Any]:
        """Normalize a strength label like '5v4', '4v5', '6v5', '4v4', or 'TOT'.

        Returns a metadata dictionary including skater counts, man advantage,
        and standardized situation labels.
        """
        try:
            raw = (strength or '').strip()
            if not raw:
                return {'raw': strength}

            if raw.upper() == 'TOT':
                return {
                    'raw': raw,
                    'label': 'TOT',
                    'situation': 'total',
                }

            match = re.match(r'^(\d+)v(\d+)$', raw, flags=re.IGNORECASE)
            if not match:
                return {'raw': raw, 'situation': 'unknown'}

            skaters_for = int(match.group(1))
            skaters_against = int(match.group(2))
            man_advantage = skaters_for - skaters_against

            pulled_for = skaters_for > 5
            pulled_against = skaters_against > 5

            if pulled_for or pulled_against:
                label = 'EN'
                situation = 'empty_net'
            elif man_advantage > 0:
                label = 'PP'
                situation = 'power_play'
            elif man_advantage < 0:
                label = 'SH'
                situation = 'penalty_kill'
            else:
                label = 'EV'
                situation = 'even_strength'

            return {
                'raw': raw,
                'skaters_for': skaters_for,
                'skaters_against': skaters_against,
                'man_advantage': man_advantage,
                'label': label,
                'situation': situation,
                'pulled_goalie_for': pulled_for,
                'pulled_goalie_against': pulled_against,
                'both_goalies_pulled': pulled_for and pulled_against,
                'three_on_three': skaters_for == 3 and skaters_against == 3,
                'four_on_four': skaters_for == 4 and skaters_against == 4,
                'team_type': team_type,
            }
        except Exception:
            return {'raw': strength, 'situation': 'unknown'}

    def parse_faceoff_comparison_data(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Parse complete Faceoff Comparison (FC) data using BeautifulSoup."""
        data = {
            'report_type': 'FC',
            'faceoff_comparison': {},
            'zone_faceoffs': {}
        }
        
        try:
            # Extract faceoff comparison data
            comparison_tables = soup.find_all('table')
            
            for table in comparison_tables:
                rows = table.find_all('tr')
                for row in rows:
                    cells = row.find_all('td')
                    if len(cells) >= 3:
                        comparison_data = self.extract_faceoff_comparison_data(cells)
                        if comparison_data:
                            data['faceoff_comparison'][comparison_data['zone']] = comparison_data
                            
        except Exception as e:
            self.logger.error(f"Error parsing faceoff comparison data: {e}")
            data['error'] = str(e)
        
        return data
    
    def parse_time_on_ice_data(self, soup: BeautifulSoup, report_type: str, file_path: Optional[str] = None) -> Dict[str, Any]:
        """Parse Time on Ice (TH/TV) data: per-player totals and special-teams TOI.

        Extracts for each player:
        - sweater_number, name (resolved via reference where possible), player_id
        - shifts (SHF), total TOI, PP total, SH total
        """
        data = {
            'report_type': report_type,
            'game_header': {},
            'player_time_on_ice': {
                'visitor': [],
                'home': []
            }
        }

        try:
            # Parse header for game/team context
            data['game_header'] = self._parse_game_header(soup, file_path)
            self._current_game_data = data['game_header']
            self._current_game_id = data['game_header'].get('game_info', {}).get('game_id')

            # Iterate each player block: identified by a td with class containing 'playerHeading'
            player_heading_cells = [td for td in soup.find_all('td') if 'playerHeading' in (td.get('class') or [])]
            player_headers = player_heading_cells  # Make this available in the loop scope
            for heading_cell in player_heading_cells:
                heading_text = heading_cell.get_text(strip=True)
                # Typical formats: "4 BYRAM, BOWEN" or "1 LUUKKONEN, UKKO-PEKKA"
                parts = heading_text.split(' ', 1)
                if len(parts) < 2 or not parts[0].isdigit():
                    continue
                sweater_number = int(parts[0])
                player_name_raw = parts[1].strip()

                # Determine team type by searching ancestor tables with id Visitor/Home
                team_type = None
                parent = heading_cell.parent
                while parent is not None and parent.name != 'body':
                    if parent.name == 'table' and parent.has_attr('id'):
                        tid = parent.get('id')
                        if isinstance(tid, str):
                            if tid.lower().startswith('visitor'):
                                team_type = 'visitor'
                                break
                            if tid.lower().startswith('home'):
                                team_type = 'home'
                                break
                    parent = parent.parent
                if team_type is None:
                    # Fallback: default using report type (TH->home, TV->visitor)
                    team_type = 'home' if report_type == 'TH' else 'visitor'

                # Resolve team_id and player_id using reference data
                team_id = None
                if self._current_game_data:
                    team_id = self._current_game_data.get('visitor_team', {}).get('id') if team_type == 'visitor' else self._current_game_data.get('home_team', {}).get('id')

                resolved_name = player_name_raw
                player_id = None
                if team_id:
                    resolved_name = self._resolve_player_name(team_id, sweater_number, player_name_raw)
                    box = self.reference_data.get_boxscore_by_id(self._current_game_id)
                    if box:
                        team_key = 'awayTeam' if team_type == 'visitor' else 'homeTeam'
                        for group in ['forwards', 'defensemen', 'goalies']:
                            for pl in (box.get('playerByGameStats', {}).get(team_key, {}).get(group, []) or []):
                                if pl.get('sweaterNumber') == sweater_number:
                                    player_id = pl.get('playerId')
                                    break
                            if player_id:
                                break

                # Initialize entry
                entry = {
                    'player_id': player_id,
                    'sweater_number': sweater_number,
                    'name': resolved_name,
                    'original_name': player_name_raw,
                    'team_id': team_id,
                    'team_type': team_type,
                    'shifts': [],
                    'totals': {
                        'shifts': None,
                        'toi': None,
                        'pp_toi': None,
                        'sh_toi': None
                    }
                }

                # Scan rows after the player heading:
                # 1) Shift table header row (contains 'Shift #' and 'Start of Shift')
                # 2) Multiple shift rows until totals header (contains 'SHF' and 'TOI') or next player heading
                # 3) Totals values row (immediately after totals header), and nested per-period totals table
                row = heading_cell.parent
                player_table = heading_cell.find_parent('table')  # Find the main table containing this player
                found_shift_header = False
                found_totals_header = False
                # Determine shift column indexes from header row once found
                idx_shift = idx_per = idx_start = idx_end = idx_duration = idx_event = None

                # Find the shift section for THIS specific player
                # We need to find the shift header that comes after this player's heading
                # but before the next player's heading
                probe = row
                next_player_found = False
                for _ in range(1000):
                    probe = probe.find_next_sibling('tr')
                    if not probe:
                        break
                    cells = probe.find_all('td')
                    if not cells:
                        continue
                    
                    # Check if we hit the next player heading
                    if any('playerHeading' in (td.get('class') or []) for td in cells):
                        next_player_found = True
                        break
                    
                    texts_upper = [c.get_text(strip=True).upper() for c in cells]
                    if any('SHIFT #' in t for t in texts_upper) and any('START OF SHIFT' in t for t in texts_upper):
                        found_shift_header = True
                        # Map column indexes
                        for i, t in enumerate(texts_upper):
                            if 'SHIFT #' in t:
                                idx_shift = i
                            elif t == 'PER':
                                idx_per = i
                            elif 'START OF SHIFT' in t:
                                idx_start = i
                            elif 'END OF SHIFT' in t:
                                idx_end = i
                            elif 'DURATION' in t:
                                idx_duration = i
                            elif 'EVENT' in t:
                                idx_event = i
                        row = probe
                        break
                
                # If we found the next player before finding shift header, this player has no shifts
                if next_player_found and not found_shift_header:
                    continue

                # Collect shift rows until we encounter totals header
                if found_shift_header:
                    for _ in range(5000):
                        row = row.find_next_sibling('tr')
                        if not row:
                            break
                        cells = row.find_all('td')
                        if not cells:
                            continue
                        # Stop at next player heading
                        if any('playerHeading' in (td.get('class') or []) for td in cells):
                            break
                        texts_upper = [c.get_text(strip=True).upper() for c in cells]
                        # Totals header?
                        if ('SHF' in texts_upper) and ('TOI' in texts_upper):
                            found_totals_header = True
                            totals_header_cells = cells
                            break
                        # Likely a shift row: expect at least 4 tds (shift #, start, end, event)
                        cell_texts = [c.get_text(strip=True) for c in cells]
                        # Validate using mapped indexes
                        if (
                            idx_shift is not None and idx_per is not None and idx_start is not None and idx_end is not None
                            and idx_event is not None and idx_shift < len(cell_texts) and idx_per < len(cell_texts)
                        ):
                            shift_no_txt = cell_texts[idx_shift]
                            if not shift_no_txt or not shift_no_txt.isdigit():
                                continue
                            per_txt = cell_texts[idx_per]
                            if not per_txt or not per_txt.isdigit():
                                continue
                            shift_number = int(shift_no_txt)
                            # Start/End columns include 'elapsed / game' values separated by '/'
                            def split_elapsed_game(val: str):
                                parts = [p.strip() for p in val.split('/')]
                                if len(parts) == 2:
                                    return {'elapsed': parts[0] or None, 'game': parts[1] or None}
                                return {'elapsed': val or None, 'game': None}
                            start_info = split_elapsed_game(cell_texts[idx_start] if idx_start < len(cell_texts) else '')
                            end_info = split_elapsed_game(cell_texts[idx_end] if idx_end < len(cell_texts) else '')
                            event_mark = cell_texts[idx_event] if idx_event < len(cell_texts) else None
                            duration_val = cell_texts[idx_duration] if (idx_duration is not None and idx_duration < len(cell_texts)) else None
                            # Require duration to look like a time value (contains ':') to treat as a valid shift row
                            if not duration_val or ':' not in duration_val:
                                continue
                            entry['shifts'].append({
                                'shift_number': shift_number,
                                'period': int(per_txt),
                                'start': start_info,
                                'end': end_info,
                                'duration': duration_val,
                                'event': event_mark
                            })

                # Parse per-period totals table following shift section; capture the 'TOT' row values
                if found_shift_header:
                    # The summary tables are nested within the main player table
                    # We need to find the summary table that belongs to THIS player
                    probe_table = None
                    
                    # Look for nested tables in the player's section
                    # We need to find the table that comes after this player's shift data
                    # but before the next player's data
                    nested_tables = player_table.find_all('table')
                    
                    # Find the summary table for this player
                    # We need to find the table that matches this player's data
                    # The tables are in order, so we can use the player's position to find the right table
                    player_index = None
                    for i, header in enumerate(player_headers):
                        if header == heading_cell:
                            player_index = i
                            break
                    
                    if player_index is not None:
                        # Find the summary table for this player
                        # The tables should be in the same order as the players
                        summary_table_count = 0
                        for nested in nested_tables:
                            # Check headers
                            header_tr = nested.find('tr')
                            if not header_tr:
                                continue
                            headers = [td.get_text(strip=True).upper().replace('\xa0', ' ') for td in header_tr.find_all('td')]
                            if headers and 'PER' in headers and 'SHF' in headers and 'TOI' in headers:
                                # This is a summary table - check if it has data rows
                                data_rows = nested.find_all('tr')[1:]  # Skip header row
                                if data_rows and any(row.find_all('td') for row in data_rows):
                                    if summary_table_count == player_index:
                                        probe_table = nested
                                        break
                                    summary_table_count += 1
                    if probe_table:
                        # Map column indexes by header names
                        header_tr = probe_table.find('tr')
                        hdrs = [td.get_text(strip=True).upper().replace('\xa0', ' ') for td in header_tr.find_all('td')]
                        def idx(name):
                            try:
                                return hdrs.index(name)
                            except ValueError:
                                return None
                        idx_shf = idx('SHF')
                        idx_toi = idx('TOI')
                        idx_ev = idx('EV TOT')
                        idx_pp = idx('PP TOT')
                        idx_sh = idx('SH TOT')
                        def val(i):
                            if i is None or i >= len(tds):
                                return None
                            return tds[i].get_text(strip=True) or None
                        
                        # Extract per-period and total data from the summary table using BeautifulSoup
                        period_totals = {}
                        
                        # Find the SHF column index
                        header_tr = probe_table.find('tr')
                        headers = [td.get_text(strip=True).upper().replace('\xa0', ' ') for td in header_tr.find_all('td')]
                        shf_idx = headers.index('SHF') if 'SHF' in headers else None
                        toi_idx = headers.index('TOI') if 'TOI' in headers else None
                        ev_idx = headers.index('EV TOT') if 'EV TOT' in headers else None
                        pp_idx = headers.index('PP TOT') if 'PP TOT' in headers else None
                        sh_idx = headers.index('SH TOT') if 'SH TOT' in headers else None
                        
                        if shf_idx is not None:
                            # Extract data from all rows
                            for tr in probe_table.find_all('tr')[1:]:  # Skip header row
                                tds = tr.find_all('td')
                                if not tds:
                                    continue
                                
                                first_cell = tds[0].get_text(strip=True)
                                
                                # Check for period rows (first cell is a digit)
                                if first_cell.isdigit() and int(first_cell) in [1, 2, 3]:
                                    period = int(first_cell)
                                    if shf_idx < len(tds):
                                        shf_val = tds[shf_idx].get_text(strip=True)
                                        if shf_val.isdigit():
                                            period_totals[f'period_{period}'] = int(shf_val)
                                
                                # Check for total row (first cell is 'TOT')
                                elif first_cell.upper() == 'TOT':
                                    if shf_idx < len(tds):
                                        total_shf = tds[shf_idx].get_text(strip=True)
                                        if total_shf.isdigit():
                                            entry['totals']['shifts'] = int(total_shf)
                                    
                                    # Also extract other totals
                                    if toi_idx and toi_idx < len(tds):
                                        entry['totals']['toi'] = tds[toi_idx].get_text(strip=True)
                                    if ev_idx and ev_idx < len(tds):
                                        entry['totals']['ev_toi'] = tds[ev_idx].get_text(strip=True)
                                    if pp_idx and pp_idx < len(tds):
                                        entry['totals']['pp_toi'] = tds[pp_idx].get_text(strip=True)
                                    if sh_idx and sh_idx < len(tds):
                                        entry['totals']['sh_toi'] = tds[sh_idx].get_text(strip=True)
                        
                        # Add period totals to entry
                        if period_totals:
                            entry['period_totals'] = period_totals

                data['player_time_on_ice'][team_type].append(entry)

        except Exception as e:
            self.logger.error(f"Error parsing time on ice data: {e}")
            data['error'] = str(e)

        return data
    
    def consolidate_game_data(self, source_data: Dict[str, Any]) -> Dict[str, Any]:
        """Consolidate data from multiple report sources."""
        consolidated = {
            'game_summary': {},
            'penalties': [],
            'roster': {},
            'statistics': {},
            'events': []
        }
        
        try:
            # Consolidate game summary data
            if 'GS' in source_data:
                consolidated['game_summary'] = source_data['GS']
            
            # Consolidate penalty data
            penalties = []
            for report_type, data in source_data.items():
                if 'penalties' in data:
                    if isinstance(data['penalties'], dict):
                        for period, period_penalties in data['penalties'].items():
                            penalties.extend(period_penalties)
                    elif isinstance(data['penalties'], list):
                        penalties.extend(data['penalties'])
            
            consolidated['penalties'] = penalties
            
            # Consolidate roster data
            if 'RO' in source_data:
                consolidated['roster'] = source_data['RO']
            
            # Consolidate statistics
            if 'GS' in source_data:
                consolidated['statistics'] = {
                    'team_stats': source_data['GS'].get('team_stats', {}),
                    'goalie_stats': source_data['GS'].get('goalie_stats', {}),
                    'power_plays': source_data['GS'].get('power_plays', {}),
                    'faceoffs': source_data['GS'].get('faceoffs', {}),
                    'shots': source_data['GS'].get('shots', {})
                }
            
            # Consolidate events
            if 'PL' in source_data:
                consolidated['events'] = source_data['PL'].get('events', [])
                
        except Exception as e:
            self.logger.error(f"Error consolidating game data: {e}")
            consolidated['error'] = str(e)
        
        return consolidated
    
    def count_records(self, data: Dict[str, Any]) -> int:
        """Count total records in parsed data."""
        count = 0
        
        try:
            if isinstance(data, dict):
                for key, value in data.items():
                    if isinstance(value, list):
                        count += len(value)
                    elif isinstance(value, dict):
                        count += self.count_records(value)
                    elif isinstance(value, (int, float)):
                        count += 1
            elif isinstance(data, list):
                count = len(data)
                
        except Exception as e:
            self.logger.debug(f"Error counting records: {e}")
        
        return count
    
    # Helper methods for data extraction
    def extract_player_roster_data(self, cells) -> Optional[Dict[str, Any]]:
        """Extract player roster data from table cells."""
        try:
            if len(cells) < 3:
                return None
            
            name_cell = cells[0].get_text(strip=True)
            number_cell = cells[1].get_text(strip=True) if len(cells) > 1 else ""
            position_cell = cells[2].get_text(strip=True) if len(cells) > 2 else ""
            
            # Parse name
            name_match = re.search(r'([A-Z][a-z]+ [A-Z][a-z]+)', name_cell)
            if not name_match:
                return None
            
            name = name_match.group(1)
            
            return {
                'name': name,
                'number': number_cell.strip(),
                'position': position_cell.strip(),
                'team': 'home' if 'home' in name_cell.lower() else 'away'
            }
            
        except Exception as e:
            self.logger.debug(f"Error extracting player roster data: {e}")
            return None
    
    def extract_shot_data(self, cells) -> Optional[Dict[str, Any]]:
        """Extract shot data from table cells."""
        try:
            if len(cells) < 4:
                return None
            
            time_cell = cells[0].get_text(strip=True)
            team_cell = cells[1].get_text(strip=True) if len(cells) > 1 else ""
            player_cell = cells[2].get_text(strip=True) if len(cells) > 2 else ""
            type_cell = cells[3].get_text(strip=True) if len(cells) > 3 else ""
            
            # Parse time
            time_match = re.search(r'(\d{1,2}:\d{2})', time_cell)
            if not time_match:
                return None
            
            time = time_match.group(1)
            
            return {
                'time': time,
                'team': team_cell.strip(),
                'player': player_cell.strip(),
                'shot_type': type_cell.strip(),
                'period': self.determine_period(time)
            }
            
        except Exception as e:
            self.logger.debug(f"Error extracting shot data: {e}")
            return None
    
    def extract_faceoff_data(self, cells) -> Optional[Dict[str, Any]]:
        """Extract faceoff data from table cells."""
        try:
            if len(cells) < 3:
                return None
            
            time_cell = cells[0].get_text(strip=True)
            team_cell = cells[1].get_text(strip=True) if len(cells) > 1 else ""
            player_cell = cells[2].get_text(strip=True) if len(cells) > 2 else ""
            
            # Parse time
            time_match = re.search(r'(\d{1,2}:\d{2})', time_cell)
            if not time_match:
                return None
            
            time = time_match.group(1)
            
            return {
                'time': time,
                'team': team_cell.strip(),
                'player': player_cell.strip(),
                'period': self.determine_period(time)
            }
            
        except Exception as e:
            self.logger.debug(f"Error extracting faceoff data: {e}")
            return None
    
    def extract_faceoff_comparison_data(self, cells) -> Optional[Dict[str, Any]]:
        """Extract faceoff comparison data from table cells."""
        try:
            if len(cells) < 3:
                return None
            
            zone_cell = cells[0].get_text(strip=True)
            home_cell = cells[1].get_text(strip=True) if len(cells) > 1 else ""
            away_cell = cells[2].get_text(strip=True) if len(cells) > 2 else ""
            
            return {
                'zone': zone_cell.strip(),
                'home_faceoffs': home_cell.strip(),
                'away_faceoffs': away_cell.strip()
            }
            
        except Exception as e:
            self.logger.debug(f"Error extracting faceoff comparison data: {e}")
            return None
    
    def extract_time_on_ice_data(self, cells) -> Optional[Dict[str, Any]]:
        """Extract time on ice data from table cells."""
        try:
            if len(cells) < 4:
                return None
            
            name_cell = cells[0].get_text(strip=True)
            position_cell = cells[1].get_text(strip=True) if len(cells) > 1 else ""
            shifts_cell = cells[2].get_text(strip=True) if len(cells) > 2 else ""
            time_cell = cells[3].get_text(strip=True) if len(cells) > 3 else ""
            
            # Parse name
            name_match = re.search(r'([A-Z][a-z]+ [A-Z][a-z]+)', name_cell)
            if not name_match:
                return None
            
            name = name_match.group(1)
            
            return {
                'player_name': name,
                'position': position_cell.strip(),
                'shifts': shifts_cell.strip(),
                'total_time': time_cell.strip()
            }
            
        except Exception as e:
            self.logger.debug(f"Error extracting time on ice data: {e}")
            return None
    
    def extract_period_events(self, period_element) -> List[Dict[str, Any]]:
        """Extract events for a specific period."""
        events = []
        
        try:
            # Look for event tables
            event_tables = period_element.find_all('table')
            
            for table in event_tables:
                rows = table.find_all('tr')
                for row in rows:
                    cells = row.find_all('td')
                    if len(cells) >= 3:
                        event = self.extract_event_data(cells)
                        if event:
                            events.append(event)
                            
        except Exception as e:
            self.logger.debug(f"Error extracting period events: {e}")
        
        return events
    
    def extract_event_data(self, cells) -> Optional[Dict[str, Any]]:
        """Extract event data from table cells."""
        try:
            if len(cells) < 3:
                return None
            
            time_cell = cells[0].get_text(strip=True)
            team_cell = cells[1].get_text(strip=True) if len(cells) > 1 else ""
            desc_cell = cells[2].get_text(strip=True) if len(cells) > 2 else ""
            
            # Parse time
            time_match = re.search(r'(\d{1,2}:\d{2})', time_cell)
            if not time_match:
                return None
            
            time = time_match.group(1)
            
            # Determine event type
            event_type = 'unknown'
            desc_lower = desc_cell.lower()
            
            if any(word in desc_lower for word in ['goal', 'score']):
                event_type = 'goal'
            elif any(word in desc_lower for word in ['penalty', 'penalized']):
                event_type = 'penalty'
            elif any(word in desc_lower for word in ['faceoff', 'face-off']):
                event_type = 'faceoff'
            elif any(word in desc_lower for word in ['shot', 'missed']):
                event_type = 'shot'
            elif any(word in desc_lower for word in ['hit', 'check']):
                event_type = 'hit'
            
            return {
                'time': time,
                'team': team_cell.strip(),
                'description': desc_cell,
                'event_type': event_type
            }
            
        except Exception as e:
            self.logger.debug(f"Error extracting event data: {e}")
            return None
    
    def determine_period_number(self, period_text: str) -> Optional[int]:
        """Determine period number from text."""
        try:
            if '1st' in period_text or 'first' in period_text:
                return 1
            elif '2nd' in period_text or 'second' in period_text:
                return 2
            elif '3rd' in period_text or 'third' in period_text:
                return 3
            elif 'ot' in period_text or 'overtime' in period_text:
                return 4
            elif 'shootout' in period_text:
                return 5
            else:
                return None
                
        except Exception as e:
            self.logger.debug(f"Error determining period number: {e}")
            return None
    
    # DUPLICATE METHOD - REMOVED TO USE THE ONE WITH REFERENCE DATA INTEGRATION
    def _parse_game_header_duplicate(self, soup: BeautifulSoup, file_path: Optional[str] = None) -> Dict[str, Any]:
        """Parse game header section with teams, score, date, and venue."""
        header = {
            'title': '',
            'date': '',
            'venue': '',
            'game_number': '',
            'final_score': '',
            'teams': {
                'home': {'name': '', 'score': 0, 'game_info': ''},
                'away': {'name': '', 'score': 0, 'game_info': ''}
            }
        }
        
        try:
            # Find the main table
            main_table = soup.find('table', id='MainTable')
            if not main_table:
                return header
            
            # Extract title
            title_elem = main_table.find('td', style=re.compile(r'font-size: 14px.*font-weight:bold'))
            if title_elem:
                header['title'] = title_elem.get_text(strip=True)
            
            # Extract game info (date, venue, etc.)
            game_info_table = main_table.find('table', id='GameInfo')
            if game_info_table:
                rows = game_info_table.find_all('tr')
                for row in rows:
                    cells = row.find_all('td')
                    for cell in cells:
                        text = cell.get_text(strip=True)
                        if 'Wednesday' in text or 'Thursday' in text or 'Friday' in text or 'Saturday' in text or 'Sunday' in text or 'Monday' in text or 'Tuesday' in text:
                            header['date'] = text
                        elif 'Attendance' in text:
                            header['venue'] = text
                        elif 'Game' in text and text.replace('Game', '').strip().isdigit():
                            header['game_number'] = text
            
            # Extract team scores and names
            visitor_table = main_table.find('table', id='Visitor')
            if visitor_table:
                # Away team score
                score_elem = visitor_table.find('td', style=re.compile(r'font-size: 40px'))
                if score_elem:
                    header['teams']['away']['score'] = int(score_elem.get_text(strip=True))
                
                # Away team name
                team_elem = visitor_table.find('td', style=re.compile(r'font-size: 10px.*font-weight:bold'))
                if team_elem:
                    team_text = team_elem.get_text(strip=True)
                    lines = team_text.split('\n')
                    if lines:
                        header['teams']['away']['name'] = lines[0].strip()
                        if len(lines) > 1:
                            header['teams']['away']['game_info'] = lines[1].strip()
            
            home_table = main_table.find('table', id='Home')
            if home_table:
                # Home team score
                score_elem = home_table.find('td', style=re.compile(r'font-size: 40px'))
                if score_elem:
                    header['teams']['home']['score'] = int(score_elem.get_text(strip=True))
                
                # Home team name
                team_elem = home_table.find('td', style=re.compile(r'font-size: 10px.*font-weight:bold'))
                if team_elem:
                    team_text = team_elem.get_text(strip=True)
                    lines = team_text.split('\n')
                    if lines:
                        header['teams']['home']['name'] = lines[0].strip()
                        if len(lines) > 1:
                            header['teams']['home']['game_info'] = lines[1].strip()
            
            # Set final score
            header['final_score'] = f"{header['teams']['away']['score']}-{header['teams']['home']['score']}"
            
        except Exception as e:
            self.logger.error(f"Error parsing game header: {e}")
        
        return header
    
    def _parse_scoring_summary(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Parse scoring summary section with proper goal structure."""
        scoring = {
            'goals': [],
            'periods': {}
        }
        
        try:
            # Find all tables with border attribute
            tables = soup.find_all('table', border='0')
            
            for table in tables:
                rows = table.find_all('tr')
                if not rows:
                    continue
                
                # Check if this is a scoring table by looking at header
                header_row = rows[0]
                header_cells = header_row.find_all('td')
                if len(header_cells) >= 5:
                    header_text = ' '.join([cell.get_text(strip=True) for cell in header_cells])
                    if 'Goal Scorer' in header_text or 'Assist' in header_text:
                        # This is a scoring table
                        for row in rows[1:]:  # Skip header
                            cells = row.find_all('td')
                            if len(cells) >= 8:
                                goal_data = self._extract_goal_from_row(cells)
                                if goal_data:
                                    scoring['goals'].append(goal_data)
                                    
                                    # Group by period
                                    period = goal_data.get('period', 'unknown')
                                    if period not in scoring['periods']:
                                        scoring['periods'][period] = []
                                    scoring['periods'][period].append(goal_data)
        
        except Exception as e:
            self.logger.error(f"Error parsing scoring summary: {e}")
        
        return scoring
    
    def _extract_goal_from_row(self, cells) -> Dict[str, Any]:
        """Extract goal data from a table row."""
        try:
            if len(cells) < 8:
                return None
            
            # Convert goal number and period to integers
            goal_number = None
            period = None
            if cells[0].get_text(strip=True).isdigit():
                goal_number = int(cells[0].get_text(strip=True))
            if cells[1].get_text(strip=True).isdigit():
                period = int(cells[1].get_text(strip=True))
            
            # Extract scorer info (format: "72 T.THOMPSON(34)")
            scorer_text = cells[5].get_text(strip=True)
            scorer_info = self._parse_player_info(scorer_text)
            
            # Extract assist info
            assist1_text = cells[6].get_text(strip=True) if len(cells) > 6 else ''
            assist1_info = self._parse_player_info(assist1_text) if assist1_text else None
            
            assist2_text = cells[7].get_text(strip=True) if len(cells) > 7 else ''
            assist2_info = self._parse_player_info(assist2_text) if assist2_text else None
            
            # Parse players on ice (format: "1,4,9,19,25,72")
            away_players = self._parse_players_on_ice(cells[8].get_text(strip=True) if len(cells) > 8 else '')
            home_players = self._parse_players_on_ice(cells[9].get_text(strip=True) if len(cells) > 9 else '')
            
            goal_data = {
                'goal_number': goal_number,
                'period': period,
                'time': cells[2].get_text(strip=True),
                'strength': cells[3].get_text(strip=True),
                'team': cells[4].get_text(strip=True),
                'scorer': scorer_info,
                'assist1': assist1_info,
                'assist2': assist2_info,
                'players_on_ice': {
                    'away': away_players,
                    'home': home_players
                }
            }
            
            return goal_data
            
        except Exception as e:
            self.logger.debug(f"Error extracting goal from row: {e}")
            return None
    
    def _parse_player_info(self, player_text: str) -> Dict[str, Any]:
        """Parse player information from text like '72 T.THOMPSON(34)'."""
        try:
            if not player_text:
                return None
            
            # Pattern: "72 T.THOMPSON(34)" or "T.THOMPSON(34)" or "T.THOMPSON"
            import re
            pattern = r'(\d+)?\s*([A-Z\.\s]+)(?:\((\d+)\))?'
            match = re.match(pattern, player_text.strip())
            
            if match:
                sweater_number = int(match.group(1)) if match.group(1) else None
                name = match.group(2).strip()
                season_goals = int(match.group(3)) if match.group(3) else None
                
                # Parse name into first initial and last name
                name_parts = self._parse_name_parts(name)
                
                # Look up playerId using sweater number
                player_id = None
                if sweater_number and hasattr(self, 'reference_data') and self.reference_data:
                    player_id = self._lookup_player_id_by_sweater(sweater_number, name)
                
                return {
                    'name': name,
                    'first_initial': name_parts['first_initial'],
                    'last_name': name_parts['last_name'],
                    'sweater_number': sweater_number,
                    'player_id': player_id,
                    'season_goals': season_goals
                }
            
            # Fallback for simple names
            name_parts = self._parse_name_parts(player_text.strip())
            return {
                'name': player_text.strip(), 
                'first_initial': name_parts['first_initial'],
                'last_name': name_parts['last_name'],
                'sweater_number': None,
                'player_id': None,
                'season_goals': None
            }
            
        except Exception as e:
            self.logger.debug(f"Error parsing player info '{player_text}': {e}")
            name_parts = self._parse_name_parts(player_text.strip())
            return {
                'name': player_text.strip(), 
                'first_initial': name_parts['first_initial'],
                'last_name': name_parts['last_name'],
                'sweater_number': None,
                'player_id': None,
                'season_goals': None
            }
    
    def _parse_name_parts(self, name: str) -> Dict[str, str]:
        """Parse name into first initial and last name."""
        try:
            if not name:
                return {'first_initial': None, 'last_name': None}
            
            # Handle special cases
            if name.upper() == 'TEAM':
                return {'first_initial': None, 'last_name': 'TEAM'}
            
            import re
            name_upper = name.upper().strip()
            
            # Check for periods first - these indicate initials
            if '.' in name_upper:
                # Pattern 1: Multiple initials with space: "J.J. SMITH" or "A.B.C. JOHNSON"
                multi_initials_space = re.match(r'([A-Z]\.?[A-Z]?\.?[A-Z]?\.?)\s+([A-Z]+)', name_upper)
                if multi_initials_space and '.' in multi_initials_space.group(1):
                    return {
                        'first_initial': multi_initials_space.group(1),
                        'last_name': multi_initials_space.group(2)
                    }
                
                # Pattern 2: Single initial with period: "T.THOMPSON" or "T. THOMPSON"
                single_initial_period = re.match(r'([A-Z])\.\s*([A-Z]+)', name_upper)
                if single_initial_period:
                    return {
                        'first_initial': single_initial_period.group(1),
                        'last_name': single_initial_period.group(2)
                    }
                
                # Pattern 3: Multiple initials no space: "J.J.SMITH"
                multi_initials_no_space = re.match(r'([A-Z]\.?[A-Z]?\.?)([A-Z]+)', name_upper)
                if multi_initials_no_space and '.' in multi_initials_no_space.group(1):
                    return {
                        'first_initial': multi_initials_no_space.group(1),
                        'last_name': multi_initials_no_space.group(2)
                    }
                
                # Pattern 3b: Multiple initials no space without periods: "JJSMITH" -> treat as single initial
                multi_initials_no_space_no_periods = re.match(r'([A-Z]{2,})([A-Z]+)', name_upper)
                if multi_initials_no_space_no_periods:
                    first_group = multi_initials_no_space_no_periods.group(1)
                    last_group = multi_initials_no_space_no_periods.group(2)
                    return {
                        'first_initial': first_group[0],  # First letter only
                        'last_name': first_group[1:] + last_group
                    }
            
            # No periods - check for spaces (full names)
            if ' ' in name_upper:
                parts = name_upper.split()
                if len(parts) == 2:
                    # Two words - likely first name and last name
                    first_name = parts[0]
                    last_name = parts[1]
                    return {
                        'first_initial': first_name[0],  # First letter of first name
                        'last_name': last_name
                    }
                elif len(parts) > 2:
                    # Multiple words - first word is initial, rest is last name
                    return {
                        'first_initial': parts[0],
                        'last_name': ' '.join(parts[1:])
                    }
            
            # No spaces - single word
            if len(name_upper.split()) == 1:
                return {
                    'first_initial': None,
                    'last_name': name_upper
                }
            
            # Final fallback
            return {
                'first_initial': None,
                'last_name': name_upper
            }
            
        except Exception as e:
            self.logger.debug(f"Error parsing name parts '{name}': {e}")
            return {'first_initial': None, 'last_name': name.upper() if name else None}
    
    def _parse_players_on_ice(self, players_text: str) -> List[int]:
        """Parse players on ice from text like '1,4,9,19,25,72'."""
        try:
            if not players_text:
                return []
            
            # Split by comma and convert to integers
            players = []
            for player in players_text.split(','):
                player = player.strip()
                if player.isdigit():
                    players.append(int(player))
            
            return players
            
        except Exception as e:
            self.logger.debug(f"Error parsing players on ice '{players_text}': {e}")
            return []
    
    def _parse_penalties_section(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Parse penalties section with proper penalty structure."""
        penalties = {
            'by_period': {},
            'all_penalties': [],
            'power_plays': {}
        }
        
        try:
            # Look for penalty summary tables specifically (exclude VPenaltySummary which is just a header)
            penalty_tables = soup.find_all('table', id='PenaltySummary')
            
            # Track processed penalties to avoid duplicates
            processed_penalties = set()
            
            for table in penalty_tables:
                # Find all nested tables within the penalty summary
                nested_tables = table.find_all('table', border='0')
                
                for nested_table in nested_tables:
                    rows = nested_table.find_all('tr')
                    if not rows:
                        continue
                    
                    # Check if this is a penalty table by looking for the header pattern
                    header_row = rows[0]
                    header_cells = header_row.find_all('td')
                    if len(header_cells) >= 6:
                        header_text = ' '.join([cell.get_text(strip=True) for cell in header_cells])
                        if '#' in header_text and 'Per' in header_text and 'Time' in header_text and 'Player' in header_text and 'PIM' in header_text and 'Penalty' in header_text:
                            # This is a penalty table
                            for row in rows[1:]:  # Skip header
                                cells = row.find_all('td')
                                if len(cells) >= 6:
                                    penalty_data = self._extract_penalty_from_row(cells)
                                    if penalty_data:
                                        # Create a unique key for this penalty to avoid duplicates
                                        penalty_key = (
                                            penalty_data.get('penalty_number'),
                                            penalty_data.get('period'),
                                            penalty_data.get('time'),
                                            penalty_data.get('player', {}).get('name'),
                                            penalty_data.get('penalty_type')
                                        )
                                        
                                        if penalty_key not in processed_penalties:
                                            processed_penalties.add(penalty_key)
                                            penalties['all_penalties'].append(penalty_data)
                                            
                                            # Group by period
                                            period = penalty_data.get('period', 'unknown')
                                            if period not in penalties['by_period']:
                                                penalties['by_period'][period] = []
                                            penalties['by_period'][period].append(penalty_data)
        
        except Exception as e:
            self.logger.error(f"Error parsing penalties section: {e}")
        
        return penalties
    
    def _extract_penalty_from_row(self, cells) -> Dict[str, Any]:
        """Extract penalty data from a table row."""
        try:
            if len(cells) < 6:
                return None
            
            # Skip header rows
            first_cell_text = cells[0].get_text(strip=True)
            if first_cell_text in ['#', 'Per', 'Time', 'Player', 'PIM', 'Penalty']:
                return None
            
            # Skip rows that look like headers or totals
            if any(header_word in first_cell_text for header_word in ['TOT', 'Power Plays', 'Goals-Opp']):
                return None
            
            # Extract player info from nested table
            player_cell = cells[3]
            player_name = ''
            sweater_number = None
            
            # Check if there's a nested table in the player cell
            nested_table = player_cell.find('table')
            if nested_table:
                nested_cells = nested_table.find_all('td')
                if len(nested_cells) >= 4:
                    # First cell is sweater number, last cell is player name
                    sweater_text = nested_cells[0].get_text(strip=True)
                    if sweater_text and sweater_text.isdigit():
                        sweater_number = int(sweater_text)
                    player_name = nested_cells[-1].get_text(strip=True)
            else:
                player_name = player_cell.get_text(strip=True)
            
            # Extract PIM and convert to integer (cell 8 based on debug output)
            pim_text = cells[8].get_text(strip=True) if len(cells) > 8 else ''
            pim = None
            if pim_text and pim_text.isdigit():
                pim = int(pim_text)
            
            # Extract penalty number and period, convert to integers
            penalty_number = None
            period = None
            if cells[0].get_text(strip=True).isdigit():
                penalty_number = int(cells[0].get_text(strip=True))
            if cells[1].get_text(strip=True).isdigit():
                period = int(cells[1].get_text(strip=True))
            
            # Parse player name into first initial and last name
            name_parts = self._parse_name_parts(player_name)
            
            # Look up playerId using sweater number and team context
            player_id = None
            if sweater_number and hasattr(self, 'reference_data') and self.reference_data:
                player_id = self._lookup_player_id_by_sweater(sweater_number, player_name)
            
            penalty_data = {
                'penalty_number': penalty_number,
                'period': period,
                'time': cells[2].get_text(strip=True),
                'player': {
                    'name': player_name,
                    'first_initial': name_parts['first_initial'],
                    'last_name': name_parts['last_name'],
                    'sweater_number': sweater_number,
                    'player_id': player_id
                },
                'pim': pim,
                'penalty_type': cells[9].get_text(strip=True) if len(cells) > 9 else None
            }
            
            # Clean up empty values
            for key in ['time', 'penalty_type']:
                if not penalty_data[key]:
                    penalty_data[key] = None
            
            # Only return if we have meaningful data
            if penalty_data['penalty_number'] and penalty_data['period'] and penalty_data['time'] and penalty_data['player']['name']:
                return penalty_data
            
            return None
            
        except Exception as e:
            self.logger.debug(f"Error extracting penalty from row: {e}")
            return None
    
    def _parse_team_stats(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Parse team statistics section."""
        team_stats = {
            'home': {},
            'away': {}
        }
        
        try:
            # Look for power play information
            power_play_tables = soup.find_all('table', border='0')
            for table in power_play_tables:
                rows = table.find_all('tr')
                for row in rows:
                    cells = row.find_all('td')
                    if len(cells) >= 2:
                        text = ' '.join([cell.get_text(strip=True) for cell in cells])
                        if 'Power Plays' in text:
                            # Extract power play data
                            pp_data = self._extract_power_play_data(cells)
                            if pp_data:
                                team_stats.update(pp_data)
        
        except Exception as e:
            self.logger.error(f"Error parsing team stats: {e}")
        
        return team_stats
    
    def _extract_power_play_data(self, cells) -> Dict[str, Any]:
        """Extract power play data from cells."""
        try:
            power_plays = {}
            for i, cell in enumerate(cells):
                text = cell.get_text(strip=True)
                if 'Power Plays' in text and i + 1 < len(cells):
                    next_cell = cells[i + 1]
                    pp_text = next_cell.get_text(strip=True)
                    # Parse format like "3-6/07:55"
                    if '/' in pp_text:
                        parts = pp_text.split('/')
                        if len(parts) == 2:
                            goals_opp = parts[0]
                            time = parts[1]
                            power_plays['power_plays'] = {
                                'goals_opportunities': goals_opp,
                                'time': time
                            }
            return power_plays
        except Exception as e:
            self.logger.debug(f"Error extracting power play data: {e}")
            return {}
    
    def _parse_officials(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Parse officials section."""
        officials = {
            'referees': [],
            'linesmen': []
        }
        
        try:
            # Look for officials information
            tables = soup.find_all('table', border='0')
            for table in tables:
                rows = table.find_all('tr')
                for row in rows:
                    cells = row.find_all('td')
                    for cell in cells:
                        text = cell.get_text(strip=True)
                        if 'Referee' in text or 'Linesperson' in text:
                            # Extract official names
                            official_data = self._extract_official_data(cell)
                            if official_data:
                                if 'referee' in official_data.get('type', '').lower():
                                    officials['referees'].append(official_data)
                                elif 'linesperson' in official_data.get('type', '').lower():
                                    officials['linesmen'].append(official_data)
        
        except Exception as e:
            self.logger.error(f"Error parsing officials: {e}")
        
        return officials
    
    def _extract_official_data(self, cell) -> Dict[str, Any]:
        """Extract official data from a cell."""
        try:
            text = cell.get_text(strip=True)
            # Look for patterns like "#34 Brandon Schrader"
            official_match = re.search(r'#(\d+)\s+([^#]+)', text)
            if official_match:
                return {
                    'number': official_match.group(1),
                    'name': official_match.group(2).strip(),
                    'type': 'referee' if 'referee' in text.lower() else 'linesperson'
                }
        except Exception as e:
            self.logger.debug(f"Error extracting official data: {e}")
        return None
    
    def _parse_three_stars(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Parse three stars section."""
        three_stars = {
            'stars': []
        }
        
        try:
            # Look for three stars information
            tables = soup.find_all('table', border='0')
            for table in tables:
                rows = table.find_all('tr')
                for row in rows:
                    cells = row.find_all('td')
                    for cell in cells:
                        text = cell.get_text(strip=True)
                        if 'STARS' in text.upper():
                            # Extract star information
                            star_data = self._extract_star_data(cell)
                            if star_data:
                                three_stars['stars'].extend(star_data)
        
        except Exception as e:
            self.logger.error(f"Error parsing three stars: {e}")
        
        return three_stars
    
    def _extract_star_data(self, cell) -> List[Dict[str, Any]]:
        """Extract three stars data from a cell."""
        try:
            text = cell.get_text(strip=True)
            stars = []
            
            # Look for patterns like "1.DETR88 P.KANE"
            star_pattern = r'(\d+)\.([A-Z]{3})([A-Z])(\d+)\s+([A-Z\.\s]+)'
            matches = re.findall(star_pattern, text)
            
            for match in matches:
                stars.append({
                    'rank': int(match[0]),
                    'team': match[1],
                    'position': match[2],
                    'number': match[3],
                    'name': match[4].strip()
                })
            
            return stars
        except Exception as e:
            self.logger.debug(f"Error extracting star data: {e}")
            return []
    
    def _parse_roster_data(self, soup: BeautifulSoup, file_path: Optional[str] = None) -> Dict[str, Any]:
        """Parse roster data from RO HTML report with proper structure."""
        roster_data = {
            'report_type': 'RO',
            'game_header': {},
            'teams': {
                'away': {
                    'name': '',
                    'active_roster': {
                        'players': [],
                        'goalies': []
                    },
                    'scratches': []
                },
                'home': {
                    'name': '',
                    'active_roster': {
                        'players': [],
                        'goalies': []
                    },
                    'scratches': []
                }
            }
        }
        
        try:
            # Parse game header (RO-specific structure)
            roster_data['game_header'] = self._parse_roster_game_header(soup)
            
            # Find roster tables by looking for the specific header pattern
            roster_headers = soup.find_all('td', class_='heading + bborder')
            
            roster_count = 0
            for header in roster_headers:
                header_text = header.get_text(strip=True)
                if header_text == '#':
                    # Found a roster table header
                    table = header.find_parent('table')
                    if table:
                        rows = table.find_all('tr')
                        if len(rows) > 1:
                            # Skip header row, process player rows
                            team_players = []
                            team_goalies = []
                            
                            for row in rows[1:]:
                                cells = row.find_all('td')
                                if len(cells) >= 3:
                                    player_data = self._extract_roster_player_from_row(cells)
                                    if player_data:
                                        if player_data['position'] == 'G':
                                            team_goalies.append(player_data)
                                        else:
                                            team_players.append(player_data)
                            
                            # Determine which team and section this is
                            if roster_count == 0:
                                # First roster table - Away team active roster
                                roster_data['teams']['away']['active_roster']['players'] = team_players
                                roster_data['teams']['away']['active_roster']['goalies'] = team_goalies
                            elif roster_count == 1:
                                # Second roster table - Home team active roster
                                roster_data['teams']['home']['active_roster']['players'] = team_players
                                roster_data['teams']['home']['active_roster']['goalies'] = team_goalies
                            elif roster_count == 2:
                                # Third roster table - Away team scratches
                                roster_data['teams']['away']['scratches'] = team_players + team_goalies
                            elif roster_count == 3:
                                # Fourth roster table - Home team scratches
                                roster_data['teams']['home']['scratches'] = team_players + team_goalies
                            
                            roster_count += 1
            
            # Set team names from game header
            if roster_data['game_header']['teams']:
                roster_data['teams']['away']['name'] = roster_data['game_header']['teams']['away']['name']
                roster_data['teams']['home']['name'] = roster_data['game_header']['teams']['home']['name']
        
        except Exception as e:
            self.logger.error(f"Error parsing roster data: {e}")
            roster_data['error'] = str(e)
        
        return roster_data
    
    def _extract_roster_player_from_row(self, cells) -> Dict[str, Any]:
        """Extract player data from a roster table row."""
        try:
            if len(cells) < 3:
                return None
            
            # Extract sweater number
            sweater_number = None
            sweater_text = cells[0].get_text(strip=True)
            if sweater_text.isdigit():
                sweater_number = int(sweater_text)
            
            # Extract position
            position = cells[1].get_text(strip=True)
            
            # Extract name and parse it
            name_text = cells[2].get_text(strip=True)
            name_parts = self._parse_name_parts(name_text)
            
            # Extract captaincy information (A), (C), etc.
            captaincy = None
            if '(A)' in name_text:
                captaincy = 'A'
            elif '(C)' in name_text:
                captaincy = 'C'
            
            # Clean up name (remove captaincy markers)
            clean_name = name_text.replace(' (A)', '').replace(' (C)', '').strip()
            clean_name_parts = self._parse_name_parts(clean_name)
            
            # Check for special formatting (bold, italic)
            is_bold = 'bold' in str(cells[0].get('class', []))
            is_italic = 'italic' in str(cells[0].get('class', []))
            
            player_data = {
                'sweater_number': sweater_number,
                'position': position,
                'name': clean_name,
                'first_initial': clean_name_parts['first_initial'],
                'last_name': clean_name_parts['last_name'],
                'captaincy': captaincy,
                'is_bold': is_bold,
                'is_italic': is_italic
            }
            
            return player_data
            
        except Exception as e:
            self.logger.debug(f"Error extracting roster player from row: {e}")
            return None
    
    def _parse_roster_game_header(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Parse game header from RO HTML report with RO-specific structure."""
        header = {
            'title': '',
            'date': '',
            'venue': '',
            'game_number': '',
            'final_score': '',
            'teams': {
                'home': {'name': '', 'score': 0, 'game_info': ''},
                'away': {'name': '', 'score': 0, 'game_info': ''}
            }
        }
        
        try:
            # Parse away team info from Visitor table
            visitor_table = soup.find('table', id='Visitor')
            if visitor_table:
                # Get away team name from image alt text or team info cell
                team_img = visitor_table.find('img', alt=lambda x: x and 'SABRES' in x.upper())
                if team_img:
                    header['teams']['away']['name'] = team_img.get('alt', '')
                
                # Get team info (Game 64 Away Game 31)
                team_info_cell = visitor_table.find('td', string=lambda text: text and 'Game' in text and 'Away' in text)
                if team_info_cell:
                    team_text = team_info_cell.get_text(strip=True)
                    # Extract team name and game info
                    lines = team_text.split('<br>')
                    if len(lines) >= 2:
                        header['teams']['away']['name'] = lines[0].strip()
                        header['teams']['away']['game_info'] = lines[1].strip()
                
                # Get away team score
                score_cell = visitor_table.find('td', style=lambda x: x and 'font-size: 40px' in x)
                if score_cell:
                    score_text = score_cell.get_text(strip=True)
                    if score_text.isdigit():
                        header['teams']['away']['score'] = int(score_text)
            
            # Parse home team info from Home table
            home_table = soup.find('table', id='Home')
            if home_table:
                # Get home team name from image alt text or team info cell
                team_img = home_table.find('img', alt=lambda x: x and 'WINGS' in x.upper())
                if team_img:
                    header['teams']['home']['name'] = team_img.get('alt', '')
                
                # Get team info (Game 65 Home Game 35)
                team_info_cell = home_table.find('td', string=lambda text: text and 'Game' in text and 'Home' in text)
                if team_info_cell:
                    team_text = team_info_cell.get_text(strip=True)
                    # Extract team name and game info
                    lines = team_text.split('<br>')
                    if len(lines) >= 2:
                        header['teams']['home']['name'] = lines[0].strip()
                        header['teams']['home']['game_info'] = lines[1].strip()
                
                # Get home team score
                score_cell = home_table.find('td', style=lambda x: x and 'font-size: 40px' in x)
                if score_cell:
                    score_text = score_cell.get_text(strip=True)
                    if score_text.isdigit():
                        header['teams']['home']['score'] = int(score_text)
            
            # Parse game info from GameInfo table
            game_info_table = soup.find('table', id='GameInfo')
            if game_info_table:
                rows = game_info_table.find_all('tr')
                for row in rows:
                    cell = row.find('td')
                    if cell:
                        text = cell.get_text(strip=True)
                        if 'Wednesday' in text or 'Thursday' in text or 'Friday' in text or 'Saturday' in text or 'Sunday' in text or 'Monday' in text or 'Tuesday' in text:
                            header['date'] = text
                        elif 'Attendance' in text and 'at' in text:
                            # Extract venue from "Attendance 18,885 at Little Caesars Arena"
                            venue_part = text.split('at ')[-1] if 'at ' in text else ''
                            header['venue'] = venue_part
                        elif 'Start' in text and 'End' in text:
                            # Game time info
                            pass
                        elif text.startswith('Game '):
                            header['game_number'] = text
                        elif text == 'Final':
                            header['title'] = 'Final'
            
            # Create final score string
            if header['teams']['away']['score'] > 0 or header['teams']['home']['score'] > 0:
                header['final_score'] = f"{header['teams']['away']['score']}-{header['teams']['home']['score']}"
        
        except Exception as e:
            self.logger.error(f"Error parsing roster game header: {e}")
        
        return header
