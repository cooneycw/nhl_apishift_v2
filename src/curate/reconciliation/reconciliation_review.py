#!/usr/bin/env python3
"""
Reconciliation Review System
============================
Identifies and categorizes differences between NHL data sources for collaborative review.
"""

import json
import sys
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
import pandas as pd

# Add src to path
sys.path.append('src')

from parse.html_penalty_parser import HTMLPenaltyParser

class ReconciliationReviewer:
    """
    Identifies and categorizes reconciliation differences between data sources.
    """
    
    def __init__(self, storage_path: str = "storage"):
        self.storage_path = Path(storage_path)
        self.html_parser = HTMLPenaltyParser()
        
    def analyze_game_reconciliation(self, season: str, game_id: str) -> Dict[str, Any]:
        """
        Analyze reconciliation differences for a specific game.
        
        Args:
            season: Season identifier
            game_id: Game ID
            
        Returns:
            Dictionary containing reconciliation analysis
        """
        analysis = {
            'game_id': game_id,
            'season': season,
            'timestamp': datetime.now().isoformat(),
            'data_sources': {},
            'reconciliation_issues': [],
            'penalty_analysis': {},
            'recommendations': []
        }
        
        try:
            # Load data from different sources
            data_sources = self.load_game_data_sources(season, game_id)
            analysis['data_sources'] = data_sources
            
            # Analyze penalty reconciliation
            penalty_analysis = self.analyze_penalty_reconciliation(data_sources)
            analysis['penalty_analysis'] = penalty_analysis
            
            # Identify reconciliation issues
            issues = self.identify_reconciliation_issues(data_sources, penalty_analysis)
            analysis['reconciliation_issues'] = issues
            
            # Generate recommendations
            recommendations = self.generate_recommendations(issues, penalty_analysis)
            analysis['recommendations'] = recommendations
            
        except Exception as e:
            analysis['error'] = str(e)
            
        return analysis
    
    def load_game_data_sources(self, season: str, game_id: str) -> Dict[str, Any]:
        """Load data from all available sources for a game."""
        sources = {}
        
        # 1. Gamecenter Landing JSON
        gc_file = self.storage_path / season / "json" / "gamecenter_landing" / f"gamecenter_landing_{game_id}.json"
        if gc_file.exists():
            try:
                with open(gc_file, 'r') as f:
                    sources['gamecenter_landing'] = json.load(f)
            except Exception as e:
                sources['gamecenter_landing'] = {'error': str(e)}
        
        # 2. Boxscore JSON
        box_file = self.storage_path / season / "json" / "boxscores" / f"boxscore_{game_id}.json"
        if box_file.exists():
            try:
                with open(box_file, 'r') as f:
                    sources['boxscore'] = json.load(f)
            except Exception as e:
                sources['boxscore'] = {'error': str(e)}
        
        # 3. Play-by-Play JSON
        pbp_file = self.storage_path / season / "json" / "playbyplay" / f"playbyplay_{game_id}.json"
        if pbp_file.exists():
            try:
                with open(pbp_file, 'r') as f:
                    sources['playbyplay'] = json.load(f)
            except Exception as e:
                sources['playbyplay'] = {'error': str(e)}
        
        # 4. Parsed HTML Data
        html_file = self.storage_path / season / "csv" / "curate" / f"html_data_{game_id}.json"
        if html_file.exists():
            try:
                with open(html_file, 'r') as f:
                    sources['parsed_html'] = json.load(f)
            except Exception as e:
                sources['parsed_html'] = {'error': str(e)}
        
        # 5. Parsed Penalties
        penalty_file = self.storage_path / season / "json" / "parsed_penalties" / f"penalties_{game_id}.json"
        if penalty_file.exists():
            try:
                with open(penalty_file, 'r') as f:
                    sources['parsed_penalties'] = json.load(f)
            except Exception as e:
                sources['parsed_penalties'] = {'error': str(e)}
        
        return sources
    
    def analyze_penalty_reconciliation(self, data_sources: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze penalty data reconciliation between sources."""
        analysis = {
            'penalty_counts': {},
            'penalty_details': {},
            'discrepancies': [],
            'complex_scenarios': [],
            'data_quality': {}
        }
        
        try:
            # Extract penalty counts from each source
            penalty_counts = {}
            
            # Gamecenter Landing penalties
            if 'gamecenter_landing' in data_sources and 'error' not in data_sources['gamecenter_landing']:
                gc_penalties = self.extract_gamecenter_penalties(data_sources['gamecenter_landing'])
                penalty_counts['gamecenter_landing'] = len(gc_penalties)
                analysis['penalty_details']['gamecenter_landing'] = gc_penalties
            
            # Boxscore PIM data
            if 'boxscore' in data_sources and 'error' not in data_sources['boxscore']:
                box_penalties = self.extract_boxscore_penalties(data_sources['boxscore'])
                penalty_counts['boxscore'] = len(box_penalties)
                analysis['penalty_details']['boxscore'] = box_penalties
            
            # Play-by-Play penalties
            if 'playbyplay' in data_sources and 'error' not in data_sources['playbyplay']:
                pbp_penalties = self.extract_playbyplay_penalties(data_sources['playbyplay'])
                penalty_counts['playbyplay'] = len(pbp_penalties)
                analysis['penalty_details']['playbyplay'] = pbp_penalties
            
            # Parsed HTML penalties
            if 'parsed_html' in data_sources and 'error' not in data_sources['parsed_html']:
                html_penalties = data_sources['parsed_html'].get('consolidated_data', {}).get('penalties', [])
                penalty_counts['parsed_html'] = len(html_penalties)
                analysis['penalty_details']['parsed_html'] = html_penalties
            
            # Parsed penalties file
            if 'parsed_penalties' in data_sources and 'error' not in data_sources['parsed_penalties']:
                parsed_penalties = data_sources['parsed_penalties'].get('penalties', [])
                penalty_counts['parsed_penalties'] = len(parsed_penalties)
                analysis['penalty_details']['parsed_penalties'] = parsed_penalties
            
            analysis['penalty_counts'] = penalty_counts
            
            # Identify discrepancies
            discrepancies = self.identify_penalty_discrepancies(penalty_counts, analysis['penalty_details'])
            analysis['discrepancies'] = discrepancies
            
            # Extract complex scenarios
            if 'parsed_penalties' in data_sources and 'error' not in data_sources['parsed_penalties']:
                analysis['complex_scenarios'] = data_sources['parsed_penalties'].get('complex_scenarios', [])
            
            # Assess data quality
            analysis['data_quality'] = self.assess_penalty_data_quality(penalty_counts, analysis['penalty_details'])
            
        except Exception as e:
            analysis['error'] = str(e)
        
        return analysis
    
    def extract_gamecenter_penalties(self, gc_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract penalties from Gamecenter Landing data."""
        penalties = []
        
        try:
            if 'penalties' in gc_data:
                for penalty in gc_data['penalties']:
                    penalty_info = {
                        'time': penalty.get('timeInPeriod', ''),
                        'team': penalty.get('teamAbbrev', ''),
                        'player': penalty.get('playerName', ''),
                        'description': penalty.get('description', ''),
                        'penalty_minutes': penalty.get('penaltyMinutes', 0),
                        'penalty_type': penalty.get('penaltyType', ''),
                        'event_id': penalty.get('eventId', ''),
                        'situation_code': penalty.get('situationCode', ''),
                        'source': 'gamecenter_landing'
                    }
                    penalties.append(penalty_info)
        except Exception as e:
            print(f"Error extracting gamecenter penalties: {e}")
        
        return penalties
    
    def extract_boxscore_penalties(self, box_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract penalty information from boxscore data."""
        penalties = []
        
        try:
            if 'homeTeam' in box_data and 'awayTeam' in box_data:
                for team_data in [box_data['homeTeam'], box_data['awayTeam']]:
                    if 'skaters' in team_data:
                        for skater in team_data['skaters']:
                            if skater.get('penaltyMinutes', 0) > 0:
                                penalty_info = {
                                    'team': team_data.get('abbrev', ''),
                                    'player': skater.get('name', ''),
                                    'penalty_minutes': skater.get('penaltyMinutes', 0),
                                    'source': 'boxscore'
                                }
                                penalties.append(penalty_info)
        except Exception as e:
            print(f"Error extracting boxscore penalties: {e}")
        
        return penalties
    
    def extract_playbyplay_penalties(self, pbp_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract penalties from play-by-play data."""
        penalties = []
        
        try:
            if 'plays' in pbp_data:
                for play in pbp_data['plays']:
                    if play.get('result', {}).get('eventTypeId') == 'PENALTY':
                        penalty_info = {
                            'time': play.get('about', {}).get('periodTime', ''),
                            'team': play.get('team', {}).get('abbreviation', ''),
                            'player': play.get('players', [{}])[0].get('player', {}).get('fullName', ''),
                            'description': play.get('result', {}).get('description', ''),
                            'penalty_minutes': play.get('result', {}).get('penaltyMinutes', 0),
                            'penalty_type': play.get('result', {}).get('penaltyType', ''),
                            'source': 'playbyplay'
                        }
                        penalties.append(penalty_info)
        except Exception as e:
            print(f"Error extracting playbyplay penalties: {e}")
        
        return penalties
    
    def identify_penalty_discrepancies(self, penalty_counts: Dict[str, int], penalty_details: Dict[str, List]) -> List[Dict[str, Any]]:
        """Identify specific discrepancies in penalty data."""
        discrepancies = []
        
        try:
            # Check for count mismatches
            if len(penalty_counts) > 1:
                counts = list(penalty_counts.values())
                if len(set(counts)) > 1:
                    discrepancies.append({
                        'type': 'count_mismatch',
                        'description': f'Penalty count mismatch: {penalty_counts}',
                        'severity': 'high',
                        'sources': list(penalty_counts.keys())
                    })
            
            # Check for missing penalties in specific sources
            for source, penalties in penalty_details.items():
                if source != 'boxscore':  # Boxscore only has PIM totals
                    for penalty in penalties:
                        # Check if this penalty exists in other sources
                        found_in_other_sources = []
                        for other_source, other_penalties in penalty_details.items():
                            if other_source != source and other_source != 'boxscore':
                                if self.find_matching_penalty(penalty, other_penalties):
                                    found_in_other_sources.append(other_source)
                        
                        if not found_in_other_sources:
                            discrepancies.append({
                                'type': 'missing_penalty',
                                'description': f'Penalty missing from other sources: {penalty.get("description", "Unknown")}',
                                'severity': 'medium',
                                'source': source,
                                'penalty': penalty
                            })
            
            # Check for data inconsistencies
            for source, penalties in penalty_details.items():
                for penalty in penalties:
                    if penalty.get('penalty_minutes'):
                        # Check if penalty minutes match across sources
                        for other_source, other_penalties in penalty_details.items():
                            if other_source != source:
                                matching_penalty = self.find_matching_penalty(penalty, other_penalties)
                                if matching_penalty and matching_penalty.get('penalty_minutes'):
                                    if penalty['penalty_minutes'] != matching_penalty['penalty_minutes']:
                                        discrepancies.append({
                                            'type': 'penalty_minutes_mismatch',
                                            'description': f'Penalty minutes mismatch: {penalty["penalty_minutes"]} vs {matching_penalty["penalty_minutes"]}',
                                            'severity': 'medium',
                                            'sources': [source, other_source],
                                            'penalties': [penalty, matching_penalty]
                                        })
                                        
        except Exception as e:
            print(f"Error identifying penalty discrepancies: {e}")
        
        return discrepancies
    
    def find_matching_penalty(self, penalty: Dict[str, Any], penalty_list: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Find a matching penalty in a list based on key attributes."""
        try:
            for other_penalty in penalty_list:
                # Try to match based on time and description
                if (penalty.get('time') == other_penalty.get('time') and 
                    penalty.get('description') == other_penalty.get('description')):
                    return other_penalty
                
                # Try to match based on player and description
                if (penalty.get('player') == other_penalty.get('player') and 
                    penalty.get('description') == other_penalty.get('description')):
                    return other_penalty
                    
        except Exception as e:
            print(f"Error finding matching penalty: {e}")
        
        return None
    
    def assess_penalty_data_quality(self, penalty_counts: Dict[str, int], penalty_details: Dict[str, List]) -> Dict[str, Any]:
        """Assess the quality of penalty data from different sources."""
        quality = {
            'completeness': {},
            'consistency': {},
            'overall_score': 0
        }
        
        try:
            total_sources = len(penalty_counts)
            if total_sources == 0:
                return quality
            
            # Assess completeness
            for source, count in penalty_counts.items():
                if source == 'boxscore':
                    quality['completeness'][source] = 'partial'  # Only PIM totals
                elif count > 0:
                    quality['completeness'][source] = 'complete'
                else:
                    quality['completeness'][source] = 'missing'
            
            # Assess consistency
            counts = [count for source, count in penalty_counts.items() if source != 'boxscore']
            if len(set(counts)) == 1:
                quality['consistency']['counts'] = 'consistent'
            elif len(set(counts)) > 1:
                quality['consistency']['counts'] = 'inconsistent'
            
            # Calculate overall score
            complete_sources = sum(1 for q in quality['completeness'].values() if q == 'complete')
            quality['overall_score'] = (complete_sources / total_sources) * 100
            
        except Exception as e:
            print(f"Error assessing data quality: {e}")
        
        return quality
    
    def identify_reconciliation_issues(self, data_sources: Dict[str, Any], penalty_analysis: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Identify broader reconciliation issues beyond penalties."""
        issues = []
        
        try:
            # Check for missing data sources
            expected_sources = ['gamecenter_landing', 'boxscore', 'playbyplay', 'parsed_html']
            missing_sources = [source for source in expected_sources if source not in data_sources]
            
            if missing_sources:
                issues.append({
                    'type': 'missing_data_source',
                    'description': f'Missing data sources: {missing_sources}',
                    'severity': 'high',
                    'impact': 'Incomplete reconciliation analysis'
                })
            
            # Check for data source errors
            error_sources = [source for source, data in data_sources.items() if 'error' in data]
            if error_sources:
                issues.append({
                    'type': 'data_source_error',
                    'description': f'Data source errors: {error_sources}',
                    'severity': 'medium',
                    'impact': 'Data quality issues'
                })
            
            # Add penalty-specific issues
            issues.extend(penalty_analysis.get('discrepancies', []))
            
        except Exception as e:
            print(f"Error identifying reconciliation issues: {e}")
        
        return issues
    
    def generate_recommendations(self, issues: List[Dict[str, Any]], penalty_analysis: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate recommendations for resolving reconciliation issues."""
        recommendations = []
        
        try:
            # Recommendations for count mismatches
            count_issues = [issue for issue in issues if issue.get('type') == 'count_mismatch']
            if count_issues:
                recommendations.append({
                    'priority': 'high',
                    'action': 'Investigate penalty count discrepancies',
                    'description': 'Review penalty extraction logic and identify missing penalties',
                    'sources': count_issues[0].get('sources', [])
                })
            
            # Recommendations for missing penalties
            missing_penalty_issues = [issue for issue in issues if issue.get('type') == 'missing_penalty']
            if missing_penalty_issues:
                recommendations.append({
                    'priority': 'medium',
                    'action': 'Cross-reference penalty data',
                    'description': 'Ensure all penalties are captured across all sources',
                    'affected_sources': list(set(issue.get('source') for issue in missing_penalty_issues))
                })
            
            # Recommendations for data quality
            data_quality = penalty_analysis.get('data_quality', {})
            if data_quality.get('overall_score', 0) < 80:
                recommendations.append({
                    'priority': 'medium',
                    'action': 'Improve data extraction',
                    'description': 'Enhance HTML parsing to capture more complete penalty data',
                    'current_score': data_quality.get('overall_score', 0)
                })
            
            # General recommendations
            recommendations.append({
                'priority': 'low',
                'action': 'Establish data validation rules',
                'description': 'Create automated checks for penalty data consistency',
                'benefit': 'Prevent future reconciliation issues'
            })
            
        except Exception as e:
            print(f"Error generating recommendations: {e}")
        
        return recommendations
    
    def find_available_games(self, season: str) -> List[str]:
        """Find all available games for a season."""
        games = []
        
        # Look for gamecenter landing files
        gc_dir = self.storage_path / season / "json" / "gamecenter_landing"
        if gc_dir.exists():
            for file in gc_dir.glob("gamecenter_landing_*.json"):
                game_id = file.stem.replace("gamecenter_landing_", "")
                games.append(game_id)
        
        # Sort games by ID
        games.sort()
        
        return games
    
    def generate_review_report(self, analysis: Dict[str, Any], output_file: Optional[Path] = None) -> str:
        """Generate a human-readable review report."""
        report = []
        report.append("=" * 80)
        report.append("NHL DATA RECONCILIATION REVIEW REPORT")
        report.append("=" * 80)
        report.append(f"Game ID: {analysis.get('game_id', 'Unknown')}")
        report.append(f"Season: {analysis.get('season', 'Unknown')}")
        report.append(f"Analysis Date: {analysis.get('timestamp', 'Unknown')}")
        report.append("")
        
        # Data Sources Summary
        report.append("DATA SOURCES SUMMARY")
        report.append("-" * 40)
        for source, data in analysis.get('data_sources', {}).items():
            if 'error' in data:
                report.append(f"  {source}: ERROR - {data['error']}")
            else:
                report.append(f"  {source}: Available")
        report.append("")
        
        # Penalty Analysis
        penalty_analysis = analysis.get('penalty_analysis', {})
        if penalty_analysis:
            report.append("PENALTY RECONCILIATION ANALYSIS")
            report.append("-" * 40)
            
            # Penalty counts
            counts = penalty_analysis.get('penalty_counts', {})
            if counts:
                report.append("Penalty Counts by Source:")
                for source, count in counts.items():
                    report.append(f"  {source}: {count}")
                report.append("")
            
            # Discrepancies
            discrepancies = penalty_analysis.get('discrepancies', [])
            if discrepancies:
                report.append("Reconciliation Discrepancies:")
                for i, disc in enumerate(discrepancies, 1):
                    report.append(f"  {i}. {disc['type'].replace('_', ' ').title()}")
                    report.append(f"     Description: {disc['description']}")
                    report.append(f"     Severity: {disc['severity']}")
                    if 'sources' in disc:
                        report.append(f"     Sources: {', '.join(disc['sources'])}")
                    report.append("")
            else:
                report.append("No reconciliation discrepancies found.")
                report.append("")
            
            # Complex scenarios
            complex_scenarios = penalty_analysis.get('complex_scenarios', [])
            if complex_scenarios:
                report.append("Complex Penalty Scenarios:")
                for i, scenario in enumerate(complex_scenarios, 1):
                    report.append(f"  {i}. {scenario['type'].replace('_', ' ').title()}")
                    report.append(f"     Description: {scenario['description']}")
                    report.append(f"     Impact: {scenario['impact']}")
                    report.append("")
            
            # Data quality
            data_quality = penalty_analysis.get('data_quality', {})
            if data_quality:
                report.append("Data Quality Assessment:")
                report.append(f"  Overall Score: {data_quality.get('overall_score', 0):.1f}%")
                for source, completeness in data_quality.get('completeness', {}).items():
                    report.append(f"  {source}: {completeness}")
                report.append("")
        
        # Issues and Recommendations
        issues = analysis.get('reconciliation_issues', [])
        if issues:
            report.append("RECONCILIATION ISSUES")
            report.append("-" * 40)
            for i, issue in enumerate(issues, 1):
                report.append(f"  {i}. {issue['type'].replace('_', ' ').title()}")
                report.append(f"     Description: {issue['description']}")
                report.append(f"     Severity: {issue['severity']}")
                if 'impact' in issue:
                    report.append(f"     Impact: {issue['impact']}")
                report.append("")
        
        recommendations = analysis.get('recommendations', [])
        if recommendations:
            report.append("RECOMMENDATIONS")
            report.append("-" * 40)
            for i, rec in enumerate(recommendations, 1):
                report.append(f"  {i}. {rec['action']}")
                report.append(f"     Priority: {rec['priority']}")
                report.append(f"     Description: {rec['description']}")
                if 'benefit' in rec:
                    report.append(f"     Benefit: {rec['benefit']}")
                report.append("")
        
        report.append("=" * 80)
        report.append("END OF REPORT")
        report.append("=" * 80)
        
        report_text = "\n".join(report)
        
        # Save to file if specified
        if output_file:
            output_file.parent.mkdir(parents=True, exist_ok=True)
            with open(output_file, 'w') as f:
                f.write(report_text)
        
        return report_text
    
    def generate_interactive_review_data(self, analysis: Dict[str, Any]) -> Dict[str, Any]:
        """Generate data for interactive review interface."""
        review_data = {
            'game_info': {
                'game_id': analysis.get('game_id'),
                'season': analysis.get('season'),
                'timestamp': analysis.get('timestamp')
            },
            'penalty_comparison': self.create_penalty_comparison_table(analysis),
            'discrepancies': analysis.get('reconciliation_issues', []),
            'complex_scenarios': analysis.get('penalty_analysis', {}).get('complex_scenarios', []),
            'data_quality': analysis.get('penalty_analysis', {}).get('data_quality', {}),
            'recommendations': analysis.get('recommendations', [])
        }
        
        return review_data
    
    def create_penalty_comparison_table(self, analysis: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Create a comparison table for penalties across sources."""
        comparison = []
        
        try:
            penalty_details = analysis.get('penalty_analysis', {}).get('penalty_details', {})
            
            # Get all unique penalties
            all_penalties = set()
            for source, penalties in penalty_details.items():
                for penalty in penalties:
                    key = f"{penalty.get('time', '')}_{penalty.get('player', '')}_{penalty.get('description', '')}"
                    all_penalties.add(key)
            
            # Create comparison rows
            for penalty_key in sorted(all_penalties):
                time, player, description = penalty_key.split('_', 2)
                
                row = {
                    'time': time,
                    'player': player,
                    'description': description,
                    'sources': {}
                }
                
                # Check each source
                for source, penalties in penalty_details.items():
                    found = False
                    for penalty in penalties:
                        if (penalty.get('time') == time and 
                            penalty.get('player') == player and 
                            penalty.get('description') == description):
                            found = True
                            row['sources'][source] = {
                                'penalty_minutes': penalty.get('penalty_minutes'),
                                'penalty_type': penalty.get('penalty_type'),
                                'team': penalty.get('team')
                            }
                            break
                    
                    if not found:
                        row['sources'][source] = None
                
                comparison.append(row)
                
        except Exception as e:
            print(f"Error creating penalty comparison table: {e}")
        
        return comparison

def main():
    """Main function for testing the reconciliation reviewer."""
    reviewer = ReconciliationReviewer()
    
    # Test with a specific game
    season = "20242025"
    game_id = "2024021130"  # Use the game we analyzed earlier
    
    print(f"Analyzing reconciliation for game {game_id}...")
    
    # Perform analysis
    analysis = reviewer.analyze_game_reconciliation(season, game_id)
    
    # Generate report
    report = reviewer.generate_review_report(analysis)
    print(report)
    
    # Save detailed analysis
    output_dir = Path("reconciliation_reviews")
    output_dir.mkdir(exist_ok=True)
    
    analysis_file = output_dir / f"reconciliation_analysis_{game_id}.json"
    with open(analysis_file, 'w') as f:
        json.dump(analysis, f, indent=2)
    
    report_file = output_dir / f"reconciliation_report_{game_id}.txt"
    with open(report_file, 'w') as f:
        f.write(report)
    
    print(f"\nDetailed analysis saved to: {analysis_file}")
    print(f"Report saved to: {report_file}")
    
    # Generate interactive review data
    review_data = reviewer.generate_interactive_review_data(analysis)
    review_file = output_dir / f"interactive_review_{game_id}.json"
    with open(review_file, 'w') as f:
        json.dump(review_data, f, indent=2)
    
    print(f"Interactive review data saved to: {review_file}")

if __name__ == "__main__":
    main()
