#!/usr/bin/env python3
"""
Batch Reconciliation Review System
=================================
Processes multiple games to identify reconciliation patterns and trends.
"""

import json
import sys
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime
import pandas as pd

# Add src to path
sys.path.append('src')

from reconciliation_review import ReconciliationReviewer

class BatchReconciliationReviewer:
    """
    Processes multiple games to identify reconciliation patterns and trends.
    """
    
    def __init__(self, storage_path: str = "storage"):
        self.storage_path = Path(storage_path)
        self.reviewer = ReconciliationReviewer(storage_path)
        self.season_results = {}
        self.patterns = {}
        
    def review_season_games(self, season: str, max_games: Optional[int] = None) -> Dict[str, Any]:
        """
        Review reconciliation for multiple games in a season.
        
        Args:
            season: Season identifier
            max_games: Maximum number of games to process (None for all)
            
        Returns:
            Dictionary containing season-wide reconciliation analysis
        """
        print(f"Starting batch reconciliation review for season {season}...")
        
        # Find available games
        available_games = self.find_available_games(season)
        
        if not available_games:
            print(f"No games found for season {season}")
            return {}
        
        if max_games:
            available_games = available_games[:max_games]
        
        print(f"Found {len(available_games)} games to review")
        
        # Process each game
        season_summary = {
            'season': season,
            'total_games': len(available_games),
            'processed_games': 0,
            'games_with_issues': 0,
            'overall_data_quality': 0,
            'common_issues': [],
            'penalty_patterns': {},
            'data_source_reliability': {},
            'recommendations': []
        }
        
        for i, game_id in enumerate(available_games, 1):
            print(f"\nProcessing game {i}/{len(available_games)}: {game_id}")
            
            try:
                # Analyze game reconciliation
                game_analysis = self.reviewer.analyze_game_reconciliation(season, game_id)
                
                if 'error' not in game_analysis:
                    # Store results
                    self.season_results[game_id] = game_analysis
                    season_summary['processed_games'] += 1
                    
                    # Check for issues
                    if game_analysis.get('reconciliation_issues'):
                        season_summary['games_with_issues'] += 1
                    
                    # Accumulate data quality scores
                    penalty_analysis = game_analysis.get('penalty_analysis', {})
                    if penalty_analysis:
                        quality = penalty_analysis.get('data_quality', {})
                        if quality.get('overall_score'):
                            season_summary['overall_data_quality'] += quality['overall_score']
                    
                    print(f"  ✅ Processed successfully")
                else:
                    print(f"  ❌ Error: {game_analysis['error']}")
                    
            except Exception as e:
                print(f"  ❌ Exception: {e}")
        
        # Calculate averages and identify patterns
        if season_summary['processed_games'] > 0:
            season_summary['overall_data_quality'] /= season_summary['processed_games']
            self.analyze_season_patterns(season_summary)
        
        return season_summary
    
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
    
    def analyze_season_patterns(self, season_summary: Dict[str, Any]):
        """Analyze patterns across all processed games."""
        print(f"\nAnalyzing season-wide patterns...")
        
        # Analyze penalty patterns
        penalty_patterns = self.analyze_penalty_patterns()
        season_summary['penalty_patterns'] = penalty_patterns
        
        # Analyze data source reliability
        source_reliability = self.analyze_data_source_reliability()
        season_summary['data_source_reliability'] = source_reliability
        
        # Identify common issues
        common_issues = self.identify_common_issues()
        season_summary['common_issues'] = common_issues
        
        # Generate season-wide recommendations
        recommendations = self.generate_season_recommendations(season_summary)
        season_summary['recommendations'] = recommendations
    
    def analyze_penalty_patterns(self) -> Dict[str, Any]:
        """Analyze penalty patterns across all games."""
        patterns = {
            'total_penalties': 0,
            'penalty_counts_by_source': {},
            'common_discrepancy_types': {},
            'complex_scenario_frequency': {},
            'data_quality_distribution': []
        }
        
        try:
            for game_id, analysis in self.season_results.items():
                penalty_analysis = analysis.get('penalty_analysis', {})
                
                # Count penalties by source
                counts = penalty_analysis.get('penalty_counts', {})
                for source, count in counts.items():
                    if source not in patterns['penalty_counts_by_source']:
                        patterns['penalty_counts_by_source'][source] = 0
                    patterns['penalty_counts_by_source'][source] += count
                    patterns['total_penalties'] += count
                
                # Track discrepancy types
                discrepancies = penalty_analysis.get('discrepancies', [])
                for disc in discrepancies:
                    disc_type = disc.get('type', 'unknown')
                    if disc_type not in patterns['common_discrepancy_types']:
                        patterns['common_discrepancy_types'][disc_type] = 0
                    patterns['common_discrepancy_types'][disc_type] += 1
                
                # Track complex scenarios
                scenarios = penalty_analysis.get('complex_scenarios', [])
                for scenario in scenarios:
                    scenario_type = scenario.get('type', 'unknown')
                    if scenario_type not in patterns['complex_scenario_frequency']:
                        patterns['complex_scenario_frequency'][scenario_type] = 0
                    patterns['complex_scenario_frequency'][scenario_type] += 1
                
                # Track data quality
                quality = penalty_analysis.get('data_quality', {})
                if quality.get('overall_score'):
                    patterns['data_quality_distribution'].append(quality['overall_score'])
            
            # Calculate averages for data quality
            if patterns['data_quality_distribution']:
                patterns['avg_data_quality'] = sum(patterns['data_quality_distribution']) / len(patterns['data_quality_distribution'])
                patterns['min_data_quality'] = min(patterns['data_quality_distribution'])
                patterns['max_data_quality'] = max(patterns['data_quality_distribution'])
            
        except Exception as e:
            print(f"Error analyzing penalty patterns: {e}")
        
        return patterns
    
    def analyze_data_source_reliability(self) -> Dict[str, Any]:
        """Analyze reliability of different data sources."""
        reliability = {
            'source_availability': {},
            'source_error_rates': {},
            'source_data_completeness': {}
        }
        
        try:
            for game_id, analysis in self.season_results.items():
                sources = analysis.get('data_sources', {})
                
                for source, data in sources.items():
                    # Track availability
                    if source not in reliability['source_availability']:
                        reliability['source_availability'][source] = {'available': 0, 'total': 0}
                    
                    reliability['source_availability'][source]['total'] += 1
                    if 'error' not in data:
                        reliability['source_availability'][source]['available'] += 1
                    else:
                        # Track error rates
                        if source not in reliability['source_error_rates']:
                            reliability['source_error_rates'][source] = 0
                        reliability['source_error_rates'][source] += 1
                    
                    # Track data completeness
                    if source not in reliability['source_data_completeness']:
                        reliability['source_data_completeness'][source] = []
                    
                    penalty_analysis = analysis.get('penalty_analysis', {})
                    if penalty_analysis:
                        quality = penalty_analysis.get('data_quality', {})
                        completeness = quality.get('completeness', {})
                        if source in completeness:
                            reliability['source_data_completeness'][source].append(completeness[source])
            
            # Calculate reliability percentages
            for source, stats in reliability['source_availability'].items():
                if stats['total'] > 0:
                    stats['reliability_percentage'] = (stats['available'] / stats['total']) * 100
                    
        except Exception as e:
            print(f"Error analyzing data source reliability: {e}")
        
        return reliability
    
    def identify_common_issues(self) -> List[Dict[str, Any]]:
        """Identify common reconciliation issues across games."""
        common_issues = []
        
        try:
            # Collect all issues
            all_issues = []
            for game_id, analysis in self.season_results.items():
                issues = analysis.get('reconciliation_issues', [])
                for issue in issues:
                    issue['game_id'] = game_id
                    all_issues.append(issue)
            
            # Group by type
            issue_types = {}
            for issue in all_issues:
                issue_type = issue.get('type', 'unknown')
                if issue_type not in issue_types:
                    issue_types[issue_type] = []
                issue_types[issue_type].append(issue)
            
            # Create summary for each type
            for issue_type, issues in issue_types.items():
                if len(issues) > 1:  # Only include if it appears in multiple games
                    common_issues.append({
                        'type': issue_type,
                        'frequency': len(issues),
                        'affected_games': [issue['game_id'] for issue in issues],
                        'severity_distribution': self.count_severities(issues),
                        'example_issues': issues[:3]  # First 3 examples
                    })
            
            # Sort by frequency
            common_issues.sort(key=lambda x: x['frequency'], reverse=True)
            
        except Exception as e:
            print(f"Error identifying common issues: {e}")
        
        return common_issues
    
    def count_severities(self, issues: List[Dict[str, Any]]) -> Dict[str, int]:
        """Count severity levels in a list of issues."""
        severity_counts = {}
        for issue in issues:
            severity = issue.get('severity', 'unknown')
            if severity not in severity_counts:
                severity_counts[severity] = 0
            severity_counts[severity] += 1
        return severity_counts
    
    def generate_season_recommendations(self, season_summary: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate recommendations based on season-wide analysis."""
        recommendations = []
        
        try:
            # Data quality recommendations
            if season_summary['overall_data_quality'] < 80:
                recommendations.append({
                    'priority': 'high',
                    'category': 'data_quality',
                    'action': 'Improve overall data quality',
                    'description': f'Current quality score: {season_summary["overall_data_quality"]:.1f}%. Focus on most problematic data sources.',
                    'impact': 'Higher reconciliation accuracy'
                })
            
            # Source reliability recommendations
            source_reliability = season_summary.get('data_source_reliability', {})
            for source, stats in source_reliability.get('source_availability', {}).items():
                if stats.get('reliability_percentage', 100) < 90:
                    recommendations.append({
                        'priority': 'medium',
                        'category': 'source_reliability',
                        'action': f'Improve {source} reliability',
                        'description': f'{source} has {stats["reliability_percentage"]:.1f}% reliability. Investigate error causes.',
                        'impact': 'More consistent data availability'
                    })
            
            # Common issues recommendations
            common_issues = season_summary.get('common_issues', [])
            if common_issues:
                top_issue = common_issues[0]
                recommendations.append({
                    'priority': 'high',
                    'category': 'systematic_issues',
                    'action': f'Address {top_issue["type"]} systematically',
                    'description': f'This issue appears in {top_issue["frequency"]} games. Implement systematic solution.',
                    'impact': 'Reduce recurring reconciliation problems'
                })
            
            # Penalty pattern recommendations
            penalty_patterns = season_summary.get('penalty_patterns', {})
            if penalty_patterns.get('common_discrepancy_types'):
                top_discrepancy = max(penalty_patterns['common_discrepancy_types'].items(), key=lambda x: x[1])
                recommendations.append({
                    'priority': 'medium',
                    'category': 'penalty_reconciliation',
                    'action': f'Focus on {top_discrepancy[0]} discrepancies',
                    'description': f'This discrepancy type appears {top_discrepancy[1]} times. Review extraction logic.',
                    'impact': 'Better penalty data consistency'
                })
            
            # General recommendations
            recommendations.append({
                'priority': 'low',
                'category': 'process_improvement',
                'action': 'Establish automated monitoring',
                'description': 'Create automated alerts for data quality issues and reconciliation failures.',
                'impact': 'Proactive issue detection'
            })
            
        except Exception as e:
            print(f"Error generating season recommendations: {e}")
        
        return recommendations
    
    def generate_season_report(self, season_summary: Dict[str, Any], output_file: Optional[Path] = None) -> str:
        """Generate a comprehensive season report."""
        report = []
        report.append("=" * 100)
        report.append("NHL SEASON RECONCILIATION ANALYSIS REPORT")
        report.append("=" * 100)
        report.append(f"Season: {season_summary.get('season', 'Unknown')}")
        report.append(f"Analysis Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append(f"Total Games: {season_summary.get('total_games', 0)}")
        report.append(f"Processed Games: {season_summary.get('processed_games', 0)}")
        report.append(f"Games with Issues: {season_summary.get('games_with_issues', 0)}")
        report.append("=" * 100)
        report.append("")
        
        # Overall Data Quality
        report.append("OVERALL DATA QUALITY")
        report.append("-" * 50)
        overall_quality = season_summary.get('overall_data_quality', 0)
        report.append(f"Average Quality Score: {overall_quality:.1f}%")
        
        if overall_quality >= 80:
            report.append("🎉 Excellent overall data quality!")
        elif overall_quality >= 60:
            report.append("⚠️  Good data quality with room for improvement")
        else:
            report.append("🚨 Poor data quality - significant issues detected")
        report.append("")
        
        # Penalty Patterns
        penalty_patterns = season_summary.get('penalty_patterns', {})
        if penalty_patterns:
            report.append("PENALTY PATTERNS")
            report.append("-" * 50)
            report.append(f"Total Penalties Processed: {penalty_patterns.get('total_penalties', 0)}")
            
            # Penalty counts by source
            counts_by_source = penalty_patterns.get('penalty_counts_by_source', {})
            if counts_by_source:
                report.append("\nPenalty Counts by Source:")
                for source, count in counts_by_source.items():
                    report.append(f"  {source}: {count}")
            
            # Common discrepancy types
            discrepancy_types = penalty_patterns.get('common_discrepancy_types', {})
            if discrepancy_types:
                report.append("\nMost Common Discrepancy Types:")
                sorted_discrepancies = sorted(discrepancy_types.items(), key=lambda x: x[1], reverse=True)
                for disc_type, count in sorted_discrepancies[:5]:  # Top 5
                    report.append(f"  {disc_type}: {count} occurrences")
            
            # Complex scenarios
            complex_scenarios = penalty_patterns.get('complex_scenario_frequency', {})
            if complex_scenarios:
                report.append("\nComplex Scenario Frequency:")
                sorted_scenarios = sorted(complex_scenarios.items(), key=lambda x: x[1], reverse=True)
                for scenario_type, count in sorted_scenarios:
                    report.append(f"  {scenario_type}: {count} games")
            
            # Data quality distribution
            quality_dist = penalty_patterns.get('data_quality_distribution', [])
            if quality_dist:
                report.append(f"\nData Quality Distribution:")
                report.append(f"  Average: {penalty_patterns.get('avg_data_quality', 0):.1f}%")
                report.append(f"  Range: {penalty_patterns.get('min_data_quality', 0):.1f}% - {penalty_patterns.get('max_data_quality', 0):.1f}%")
            
            report.append("")
        
        # Data Source Reliability
        source_reliability = season_summary.get('data_source_reliability', {})
        if source_reliability:
            report.append("DATA SOURCE RELIABILITY")
            report.append("-" * 50)
            
            availability = source_reliability.get('source_availability', {})
            for source, stats in availability.items():
                reliability_pct = stats.get('reliability_percentage', 0)
                report.append(f"{source}: {reliability_pct:.1f}% reliable ({stats['available']}/{stats['total']} games)")
            
            # Error rates
            error_rates = source_reliability.get('source_error_rates', {})
            if error_rates:
                report.append("\nSources with Errors:")
                for source, error_count in error_rates.items():
                    total_games = availability.get(source, {}).get('total', 0)
                    if total_games > 0:
                        error_rate = (error_count / total_games) * 100
                        report.append(f"  {source}: {error_rate:.1f}% error rate")
            
            report.append("")
        
        # Common Issues
        common_issues = season_summary.get('common_issues', [])
        if common_issues:
            report.append("COMMON RECONCILIATION ISSUES")
            report.append("-" * 50)
            
            for i, issue in enumerate(common_issues[:10], 1):  # Top 10
                report.append(f"{i}. {issue['type'].replace('_', ' ').title()}")
                report.append(f"   Frequency: {issue['frequency']} games")
                report.append(f"   Severity Distribution: {issue['severity_distribution']}")
                report.append(f"   Affected Games: {', '.join(issue['affected_games'][:5])}")
                if len(issue['affected_games']) > 5:
                    report.append(f"   ... and {len(issue['affected_games']) - 5} more")
                report.append("")
        
        # Recommendations
        recommendations = season_summary.get('recommendations', [])
        if recommendations:
            report.append("RECOMMENDATIONS")
            report.append("-" * 50)
            
            # Group by priority
            high_priority = [r for r in recommendations if r.get('priority') == 'high']
            medium_priority = [r for r in recommendations if r.get('priority') == 'medium']
            low_priority = [r for r in recommendations if r.get('priority') == 'low']
            
            if high_priority:
                report.append("🔴 HIGH PRIORITY:")
                for i, rec in enumerate(high_priority, 1):
                    report.append(f"  {i}. {rec['action']}")
                    report.append(f"     {rec['description']}")
                    report.append(f"     Impact: {rec['impact']}")
                    report.append("")
            
            if medium_priority:
                report.append("🟡 MEDIUM PRIORITY:")
                for i, rec in enumerate(medium_priority, 1):
                    report.append(f"  {i}. {rec['action']}")
                    report.append(f"     {rec['description']}")
                    report.append(f"     Impact: {rec['impact']}")
                    report.append("")
            
            if low_priority:
                report.append("🟢 LOW PRIORITY:")
                for i, rec in enumerate(low_priority, 1):
                    report.append(f"  {i}. {rec['action']}")
                    report.append(f"     {rec['description']}")
                    report.append(f"     Impact: {rec['impact']}")
                    report.append("")
        
        report.append("=" * 100)
        report.append("END OF SEASON REPORT")
        report.append("=" * 100)
        
        report_text = "\n".join(report)
        
        # Save to file if specified
        if output_file:
            output_file.parent.mkdir(parents=True, exist_ok=True)
            with open(output_file, 'w') as f:
                f.write(report_text)
        
        return report_text
    
    def export_season_data(self, season_summary: Dict[str, Any], output_dir: Optional[Path] = None):
        """Export season data for further analysis."""
        if not output_dir:
            output_dir = Path("reconciliation_reviews") / "season_analysis"
        
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Export season summary
        summary_file = output_dir / f"season_summary_{season_summary.get('season', 'unknown')}.json"
        with open(summary_file, 'w') as f:
            json.dump(season_summary, f, indent=2)
        
        # Export individual game results
        games_dir = output_dir / "individual_games"
        games_dir.mkdir(exist_ok=True)
        
        for game_id, analysis in self.season_results.items():
            game_file = games_dir / f"game_analysis_{game_id}.json"
            with open(game_file, 'w') as f:
                json.dump(analysis, f, indent=2)
        
        # Export patterns data
        patterns_file = output_dir / f"patterns_{season_summary.get('season', 'unknown')}.json"
        with open(patterns_file, 'w') as f:
            json.dump({
                'penalty_patterns': season_summary.get('penalty_patterns', {}),
                'data_source_reliability': season_summary.get('data_source_reliability', {}),
                'common_issues': season_summary.get('common_issues', [])
            }, f, indent=2)
        
        print(f"✅ Season data exported to: {output_dir}")
        print(f"  Summary: {summary_file}")
        print(f"  Individual games: {games_dir}")
        print(f"  Patterns: {patterns_file}")

def main():
    """Main function for batch reconciliation review."""
    print("NHL Data Reconciliation - Batch Review System")
    print("=" * 60)
    
    # Get parameters
    season = input("Enter season (e.g., 20242025): ").strip()
    if not season:
        season = "20242025"
    
    max_games_input = input("Enter maximum number of games to process (or press Enter for all): ").strip()
    max_games = None
    if max_games_input:
        try:
            max_games = int(max_games_input)
        except ValueError:
            print("Invalid number. Processing all games.")
    
    # Start batch review
    batch_reviewer = BatchReconciliationReviewer()
    
    print(f"\nStarting batch review for season {season}...")
    if max_games:
        print(f"Processing up to {max_games} games")
    else:
        print("Processing all available games")
    
    # Perform batch review
    season_summary = batch_reviewer.review_season_games(season, max_games)
    
    if season_summary:
        # Generate report
        print(f"\nGenerating season report...")
        report = batch_reviewer.generate_season_report(season_summary)
        
        # Save report
        output_dir = Path("reconciliation_reviews")
        output_dir.mkdir(exist_ok=True)
        
        report_file = output_dir / f"season_report_{season}.txt"
        with open(report_file, 'w') as f:
            f.write(report)
        
        print(f"✅ Season report saved to: {report_file}")
        
        # Export data
        print(f"\nExporting season data...")
        batch_reviewer.export_season_data(season_summary)
        
        # Display summary
        print(f"\n" + "="*60)
        print("BATCH REVIEW COMPLETED")
        print("="*60)
        print(f"Season: {season}")
        print(f"Games Processed: {season_summary.get('processed_games', 0)}")
        print(f"Overall Data Quality: {season_summary.get('overall_data_quality', 0):.1f}%")
        print(f"Games with Issues: {season_summary.get('games_with_issues', 0)}")
        print(f"Common Issues Identified: {len(season_summary.get('common_issues', []))}")
        print(f"Recommendations Generated: {len(season_summary.get('recommendations', []))}")
        print("="*60)
        
        # Display report
        print("\n" + "="*100)
        print("SEASON REPORT")
        print("="*100)
        print(report)
    else:
        print("❌ Batch review failed or no games were processed.")

if __name__ == "__main__":
    main()

