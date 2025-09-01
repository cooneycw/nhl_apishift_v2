#!/usr/bin/env python3
"""
HTML Report Collector for NHL Data Retrieval System
==================================================

This module provides comprehensive HTML report collection for all NHL datasets,
implementing the HTML report integration described in the API documentation.
HTML reports contain valuable data that can be extracted for analysis using BeautifulSoup.
"""

import requests
from bs4 import BeautifulSoup
import logging
import time
from typing import Dict, List, Any, Optional
from datetime import datetime
import re


class HTMLReportCollector:
    """
    Collects and processes HTML reports from NHL.com.
    
    The NHL provides comprehensive HTML reports for each game that contain detailed
    statistics and information. These reports are accessible via direct URLs and
    provide granular data not available through the JSON API.
    """
    
    def __init__(self, config):
        """Initialize the HTML report collector."""
        self.config = config
        self.logger = logging.getLogger('HTMLReportCollector')
        
        # Base URL for HTML reports
        self.base_url = "https://www.nhl.com/scores/htmlreports"
        self.shift_charts_url = "https://www.nhl.com/stats/shiftcharts"
        
        # Headers for requests
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Safari/537.36"
        }
        
        # Available report types
        self.report_types = {
            'GS': 'Game Summary',
            'ES': 'Event Summary', 
            'PL': 'Play-by-Play',
            'FS': 'Faceoff Summary',
            'FC': 'Faceoff Comparison',
            'RO': 'Rosters',
            'SS': 'Shot Summary',
            'TV': 'Time on Ice Away',
            'TH': 'Time on Ice Home'
        }
    
    def fetch_html_report(self, season: str, report_type: str, game_id: str) -> Optional[str]:
        """
        Fetch and return HTML report content.
        
        Args:
            season: Season identifier (e.g., '20242025')
            report_type: Report type code (e.g., 'GS', 'ES', 'PL')
            game_id: Game ID (e.g., '020489')
            
        Returns:
            HTML content as string, or None if failed
        """
        try:
            # Construct URL
            url = f"{self.base_url}/{season}/{report_type}{game_id}.HTM"
            
            self.logger.debug(f"Fetching HTML report: {url}")
            
            # Make request
            response = requests.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()
            
            # Check if content is valid HTML
            if not response.text.strip():
                self.logger.warning(f"Empty response for {url}")
                return None
            
            # Verify it's HTML content
            if not response.text.lower().startswith('<html'):
                self.logger.warning(f"Non-HTML response for {url}")
                return None
            
            self.logger.debug(f"Successfully fetched {report_type} report for game {game_id}")
            return response.text
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Request error fetching {report_type} report for game {game_id}: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Unexpected error fetching {report_type} report for game {game_id}: {e}")
            return None
    
    def fetch_shift_chart(self, game_id: str) -> Optional[str]:
        """
        Fetch shift chart data.
        
        Args:
            game_id: Game ID (e.g., '2024020489')
            
        Returns:
            Shift chart content as string, or None if failed
        """
        try:
            url = f"{self.shift_charts_url}?id={game_id}"
            
            self.logger.debug(f"Fetching shift chart: {url}")
            
            response = requests.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()
            
            self.logger.debug(f"Successfully fetched shift chart for game {game_id}")
            return response.text
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Request error fetching shift chart for game {game_id}: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Unexpected error fetching shift chart for game {game_id}: {e}")
            return None
    
    def extract_game_summary_data(self, html_content: str) -> Dict[str, Any]:
        """
        Extract data from Game Summary (GS) HTML report.
        
        Args:
            html_content: HTML content as string
            
        Returns:
            Dictionary containing extracted game summary data
        """
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            data = {}
            
            # Find game info table
            game_info_table = soup.find('table', {'id': 'GameInfo'})
            if game_info_table:
                # Extract game details
                rows = game_info_table.find_all('tr')
                for row in rows:
                    cells = row.find_all('td')
                    if len(cells) >= 2:
                        key = cells[0].get_text(strip=True)
                        value = cells[1].get_text(strip=True)
                        data[key] = value
            
            # Find team stats tables
            team_stats_tables = soup.find_all('table', {'class': 'teamStats'})
            for table in team_stats_tables:
                # Extract team statistics
                team_name = table.find_previous('h3')
                team_name = team_name.get_text(strip=True) if team_name else 'Unknown'
                team_data = {}
                
                rows = table.find_all('tr')
                for row in rows[1:]:  # Skip header row
                    cells = row.find_all('td')
                    if len(cells) >= 2:
                        stat_name = cells[0].get_text(strip=True)
                        stat_value = cells[1].get_text(strip=True)
                        team_data[stat_name] = stat_value
                
                data[f'{team_name}_stats'] = team_data
            
            return data
            
        except Exception as e:
            self.logger.error(f"Error extracting game summary data: {e}")
            return {}
    
    def extract_event_summary_data(self, html_content: str) -> List[Dict[str, Any]]:
        """
        Extract data from Event Summary (ES) HTML report.
        
        Args:
            html_content: HTML content as string
            
        Returns:
            List of dictionaries containing event data
        """
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            events = []
            
            # Find event table
            event_table = soup.find('table', {'id': 'EventTable'})
            if event_table:
                rows = event_table.find_all('tr')
                for row in rows[1:]:  # Skip header row
                    cells = row.find_all('td')
                    if len(cells) >= 6:
                        event = {
                            'period': cells[0].get_text(strip=True),
                            'time': cells[1].get_text(strip=True),
                            'event_type': cells[2].get_text(strip=True),
                            'description': cells[3].get_text(strip=True),
                            'team': cells[4].get_text(strip=True),
                            'player': cells[5].get_text(strip=True)
                        }
                        events.append(event)
            
            return events
            
        except Exception as e:
            self.logger.error(f"Error extracting event summary data: {e}")
            return []
    
    def extract_faceoff_data(self, html_content: str) -> Dict[str, List[Dict[str, Any]]]:
        """
        Extract data from Faceoff Summary (FS) HTML report.
        
        Args:
            html_content: HTML content as string
            
        Returns:
            Dictionary containing faceoff data by team
        """
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            faceoff_data = {}
            
            # Find faceoff tables for each team
            faceoff_tables = soup.find_all('table', {'class': 'faceoffTable'})
            for table in faceoff_tables:
                team_name = table.find_previous('h3')
                team_name = team_name.get_text(strip=True) if team_name else 'Unknown'
                team_faceoffs = []
                
                rows = table.find_all('tr')
                for row in rows[1:]:  # Skip header row
                    cells = row.find_all('td')
                    if len(cells) >= 4:
                        faceoff = {
                            'player': cells[0].get_text(strip=True),
                            'wins': int(cells[1].get_text(strip=True) or 0),
                            'losses': int(cells[2].get_text(strip=True) or 0),
                            'percentage': float(cells[3].get_text(strip=True).replace('%', '') or 0)
                        }
                        team_faceoffs.append(faceoff)
                
                faceoff_data[team_name] = team_faceoffs
            
            return faceoff_data
            
        except Exception as e:
            self.logger.error(f"Error extracting faceoff data: {e}")
            return {}
    
    def extract_time_on_ice_data(self, html_content: str) -> Dict[str, List[Dict[str, Any]]]:
        """
        Extract data from Time on Ice (TO) HTML report.
        
        Args:
            html_content: HTML content as string
            
        Returns:
            Dictionary containing TOI data by team
        """
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            toi_data = {}
            
            # Find TOI tables for each team
            toi_tables = soup.find_all('table', {'class': 'toiTable'})
            for table in toi_tables:
                team_name = table.find_previous('h3')
                team_name = team_name.get_text(strip=True) if team_name else 'Unknown'
                team_toi = []
                
                rows = table.find_all('tr')
                for row in rows[1:]:  # Skip header row
                    cells = row.find_all('td')
                    if len(cells) >= 6:
                        toi = {
                            'player': cells[0].get_text(strip=True),
                            'position': cells[1].get_text(strip=True),
                            'total_toi': cells[2].get_text(strip=True),
                            'even_toi': cells[3].get_text(strip=True),
                            'pp_toi': cells[4].get_text(strip=True),
                            'sh_toi': cells[5].get_text(strip=True)
                        }
                        team_toi.append(toi)
                
                toi_data[team_name] = team_toi
            
            return toi_data
            
        except Exception as e:
            self.logger.error(f"Error extracting time on ice data: {e}")
            return {}
    
    def extract_shot_data(self, html_content: str) -> Dict[str, Any]:
        """
        Extract data from Shot Summary (SS) HTML report.
        
        Args:
            html_content: HTML content as string
            
        Returns:
            Dictionary containing shot data
        """
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            shot_data = {}
            
            # Find shot summary tables
            shot_tables = soup.find_all('table', {'class': 'shotTable'})
            for table in shot_tables:
                team_name = table.find_previous('h3')
                team_name = team_name.get_text(strip=True) if team_name else 'Unknown'
                team_shots = []
                
                rows = table.find_all('tr')
                for row in rows[1:]:  # Skip header row
                    cells = row.find_all('td')
                    if len(cells) >= 4:
                        shot = {
                            'player': cells[0].get_text(strip=True),
                            'shots': int(cells[1].get_text(strip=True) or 0),
                            'goals': int(cells[2].get_text(strip=True) or 0),
                            'percentage': float(cells[3].get_text(strip=True).replace('%', '') or 0)
                        }
                        team_shots.append(shot)
                
                shot_data[f'{team_name}_shots'] = team_shots
            
            return shot_data
            
        except Exception as e:
            self.logger.error(f"Error extracting shot data: {e}")
            return {}
    
    def extract_roster_data(self, html_content: str) -> Dict[str, List[Dict[str, Any]]]:
        """
        Extract data from Rosters (RO) HTML report.
        
        Args:
            html_content: HTML content as string
            
        Returns:
            Dictionary containing roster data by team
        """
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            roster_data = {}
            
            # Find roster tables for each team
            roster_tables = soup.find_all('table', {'class': 'rosterTable'})
            for table in roster_tables:
                team_name = table.find_previous('h3')
                team_name = team_name.get_text(strip=True) if team_name else 'Unknown'
                team_roster = []
                
                rows = table.find_all('tr')
                for row in rows[1:]:  # Skip header row
                    cells = row.find_all('td')
                    if len(cells) >= 6:
                        player = {
                            'number': cells[0].get_text(strip=True),
                            'name': cells[1].get_text(strip=True),
                            'position': cells[2].get_text(strip=True),
                            'height': cells[3].get_text(strip=True),
                            'weight': cells[4].get_text(strip=True),
                            'birth_date': cells[5].get_text(strip=True)
                        }
                        team_roster.append(player)
                
                roster_data[team_name] = team_roster
            
            return roster_data
            
        except Exception as e:
            self.logger.error(f"Error extracting roster data: {e}")
            return {}
    
    def extract_play_by_play_data(self, html_content: str) -> List[Dict[str, Any]]:
        """
        Extract data from Play-by-Play (PL) HTML report.
        
        Args:
            html_content: HTML content as string
            
        Returns:
            List of dictionaries containing play-by-play data
        """
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            plays = []
            
            # Find play-by-play table
            play_table = soup.find('table', {'id': 'PlayByPlayTable'})
            if play_table:
                rows = play_table.find_all('tr')
                for row in rows[1:]:  # Skip header row
                    cells = row.find_all('td')
                    if len(cells) >= 4:
                        play = {
                            'period': cells[0].get_text(strip=True),
                            'time': cells[1].get_text(strip=True),
                            'event': cells[2].get_text(strip=True),
                            'description': cells[3].get_text(strip=True)
                        }
                        plays.append(play)
            
            return plays
            
        except Exception as e:
            self.logger.error(f"Error extracting play-by-play data: {e}")
            return []
    
    def process_html_report(self, season: str, report_type: str, game_id: str) -> Dict[str, Any]:
        """
        Fetch and process HTML report, extracting structured data.
        
        Args:
            season: Season identifier
            report_type: Report type code
            game_id: Game ID
            
        Returns:
            Dictionary containing extracted data and metadata
        """
        # Fetch HTML content
        html_content = self.fetch_html_report(season, report_type, game_id)
        
        if not html_content:
            return {
                'success': False,
                'error': 'Failed to fetch HTML content',
                'data': None
            }
        
        # Extract data based on report type
        extracted_data = {}
        
        try:
            if report_type == 'GS':
                extracted_data = self.extract_game_summary_data(html_content)
            elif report_type == 'ES':
                extracted_data = self.extract_event_summary_data(html_content)
            elif report_type == 'FS':
                extracted_data = self.extract_faceoff_data(html_content)
            elif report_type == 'TO':
                extracted_data = self.extract_time_on_ice_data(html_content)
            elif report_type == 'SS':
                extracted_data = self.extract_shot_data(html_content)
            elif report_type == 'RO':
                extracted_data = self.extract_roster_data(html_content)
            elif report_type == 'PL':
                extracted_data = self.extract_play_by_play_data(html_content)
            else:
                # For other report types, return raw HTML for now
                extracted_data = {'raw_html': html_content[:1000] + '...'}  # Truncated for storage
            
            return {
                'success': True,
                'report_type': report_type,
                'season': season,
                'game_id': game_id,
                'timestamp': datetime.now().isoformat(),
                'data': extracted_data,
                'raw_html': html_content
            }
            
        except Exception as e:
            self.logger.error(f"Error processing {report_type} report for game {game_id}: {e}")
            return {
                'success': False,
                'error': str(e),
                'data': None,
                'raw_html': html_content
            }
    
    def batch_fetch_reports(self, season: str, game_ids: List[str], 
                           report_types: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Batch fetch multiple HTML reports for efficiency.
        
        Args:
            season: Season identifier
            game_ids: List of game IDs
            report_types: List of report types to fetch (default: all)
            
        Returns:
            Dictionary containing all fetched reports
        """
        if report_types is None:
            report_types = list(self.report_types.keys())
        
        results = {
            'season': season,
            'total_games': len(game_ids),
            'total_reports': len(game_ids) * len(report_types),
            'successful_fetches': 0,
            'failed_fetches': 0,
            'reports': {}
        }
        
        self.logger.info(f"Starting batch fetch for {len(game_ids)} games, {len(report_types)} report types each")
        
        for game_id in game_ids:
            results['reports'][game_id] = {}
            
            for report_type in report_types:
                try:
                    report_data = self.process_html_report(season, report_type, game_id)
                    results['reports'][game_id][report_type] = report_data
                    
                    if report_data['success']:
                        results['successful_fetches'] += 1
                    else:
                        results['failed_fetches'] += 1
                    
                    # Add small delay to be respectful to the server
                    time.sleep(0.1)
                    
                except Exception as e:
                    self.logger.error(f"Error in batch fetch for game {game_id}, report {report_type}: {e}")
                    results['reports'][game_id][report_type] = {
                        'success': False,
                        'error': str(e),
                        'data': None
                    }
                    results['failed_fetches'] += 1
        
        self.logger.info(f"Batch fetch completed: {results['successful_fetches']} successful, {results['failed_fetches']} failed")
        return results
    
    def validate_html_content(self, html_content: str) -> bool:
        """
        Validate that HTML content is properly formatted and contains expected data.
        
        Args:
            html_content: HTML content as string
            
        Returns:
            True if content is valid, False otherwise
        """
        if not html_content or not html_content.strip():
            return False
        
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Check for basic HTML structure
            if not soup.find('html'):
                return False
            
            # Check for NHL-specific content
            if not soup.find(text=re.compile(r'NHL|hockey|game', re.IGNORECASE)):
                return False
            
            # Check for tables (most reports contain tables)
            if not soup.find('table'):
                return False
            
            return True
            
        except Exception:
            return False
    
    def get_report_urls(self, season: str, game_id: str) -> Dict[str, str]:
        """
        Generate URLs for all available report types for a game.
        
        Args:
            season: Season identifier
            game_id: Game ID
            
        Returns:
            Dictionary mapping report types to URLs
        """
        urls = {}
        
        for report_type in self.report_types.keys():
            urls[report_type] = f"{self.base_url}/{season}/{report_type}{game_id}.HTM"
        
        # Add shift chart URL
        urls['SHIFT_CHART'] = f"{self.shift_charts_url}?id={game_id}"
        
        return urls
