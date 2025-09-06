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
    
    def __init__(self, config=None):
        """Initialize the HTML penalty parser."""
        self.config = config
        self.logger = logging.getLogger('HTMLPenaltyParser')
        
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
            html_file = html_dir / f"{report_type}{game_id}.HTM"
            if html_file.exists():
                try:
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
            game_data['complex_scenarios'] = self.detect_complex_scenarios(game_penalties['consolidated_penalties'])
        
        return game_penalties
    
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
                return self.parse_game_summary_data(soup)
            elif report_type == 'PL':
                return self.parse_playbyplay_data(soup)
            elif report_type == 'ES':
                return self.parse_event_summary_data(soup)
            elif report_type == 'RO':
                return self._parse_roster_data(soup)
            elif report_type == 'SS':
                return self.parse_shot_summary_data(soup)
            elif report_type == 'FS':
                return self.parse_faceoff_summary_data(soup)
            elif report_type == 'FC':
                return self.parse_faceoff_comparison_data(soup)
            elif report_type in ['TH', 'TV']:
                return self.parse_time_on_ice_data(soup, report_type)
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
    
    def parse_game_summary_data(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """
        Parse complete Game Summary (GS) data using BeautifulSoup with proper section structure.
        
        Args:
            soup: BeautifulSoup object of the HTML content
            
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
            data['game_header'] = self._parse_game_header(soup)
            
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
    
    def parse_faceoff_summary_data(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Parse complete Faceoff Summary (FS) data using BeautifulSoup."""
        data = {
            'report_type': 'FS',
            'faceoffs_by_period': {},
            'player_faceoffs': {}
        }
        
        try:
            # Extract faceoff information
            faceoff_tables = soup.find_all('table')
            
            for table in faceoff_tables:
                rows = table.find_all('tr')
                for row in rows:
                    cells = row.find_all('td')
                    if len(cells) >= 3:
                        faceoff_data = self.extract_faceoff_data(cells)
                        if faceoff_data:
                            period = faceoff_data.get('period', 'unknown')
                            if period not in data['faceoffs_by_period']:
                                data['faceoffs_by_period'][period] = []
                            data['faceoffs_by_period'][period].append(faceoff_data)
                            
        except Exception as e:
            self.logger.error(f"Error parsing faceoff summary data: {e}")
            data['error'] = str(e)
        
        return data
    
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
    
    def parse_time_on_ice_data(self, soup: BeautifulSoup, report_type: str) -> Dict[str, Any]:
        """Parse complete Time on Ice (TH/TV) data using BeautifulSoup."""
        data = {
            'report_type': report_type,
            'player_time_on_ice': {},
            'line_combinations': {},
            'defense_pairs': {}
        }
        
        try:
            # Extract time on ice information
            time_tables = soup.find_all('table')
            
            for table in time_tables:
                rows = table.find_all('tr')
                for row in rows:
                    cells = row.find_all('td')
                    if len(cells) >= 4:
                        time_data = self.extract_time_on_ice_data(cells)
                        if time_data:
                            player_name = time_data.get('player_name', 'unknown')
                            data['player_time_on_ice'][player_name] = time_data
                            
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
    
    def _parse_game_header(self, soup: BeautifulSoup) -> Dict[str, Any]:
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
                
                return {
                    'name': name,
                    'first_initial': name_parts['first_initial'],
                    'last_name': name_parts['last_name'],
                    'sweater_number': sweater_number,
                    'season_goals': season_goals
                }
            
            # Fallback for simple names
            name_parts = self._parse_name_parts(player_text.strip())
            return {
                'name': player_text.strip(), 
                'first_initial': name_parts['first_initial'],
                'last_name': name_parts['last_name'],
                'sweater_number': None, 
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
            # Look for penalty summary tables specifically
            penalty_tables = soup.find_all('table', id='PenaltySummary')
            
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
            
            penalty_data = {
                'penalty_number': penalty_number,
                'period': period,
                'time': cells[2].get_text(strip=True),
                'player': {
                    'name': player_name,
                    'first_initial': name_parts['first_initial'],
                    'last_name': name_parts['last_name'],
                    'sweater_number': sweater_number
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
    
    def _parse_roster_data(self, soup: BeautifulSoup) -> Dict[str, Any]:
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
