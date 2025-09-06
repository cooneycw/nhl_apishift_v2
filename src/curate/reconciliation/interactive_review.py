#!/usr/bin/env python3
"""
Interactive Reconciliation Review Interface
=========================================
Allows collaborative review of reconciliation differences with context application.
"""

import json
import sys
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime

# Add src to path
sys.path.append('src')

from reconciliation_review import ReconciliationReviewer

class InteractiveReviewInterface:
    """
    Interactive interface for reviewing reconciliation differences.
    """
    
    def __init__(self, storage_path: str = "storage"):
        self.reviewer = ReconciliationReviewer(storage_path)
        self.current_analysis = None
        self.context_notes = {}
        
    def start_review_session(self, season: str, game_id: str):
        """Start a new review session for a specific game."""
        print(f"\n{'='*80}")
        print(f"Starting Reconciliation Review Session")
        print(f"{'='*80}")
        print(f"Game ID: {game_id}")
        print(f"Season: {season}")
        print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*80}\n")
        
        # Load and analyze the game
        print("Loading game data and performing reconciliation analysis...")
        self.current_analysis = self.reviewer.analyze_game_reconciliation(season, game_id)
        
        if 'error' in self.current_analysis:
            print(f"Error during analysis: {self.current_analysis['error']}")
            return
        
        # Display summary
        self.display_analysis_summary()
        
        # Start interactive review
        self.interactive_review_loop()
    
    def display_analysis_summary(self):
        """Display a summary of the reconciliation analysis."""
        if not self.current_analysis:
            return
        
        print("\n" + "="*60)
        print("RECONCILIATION ANALYSIS SUMMARY")
        print("="*60)
        
        # Data sources
        sources = self.current_analysis.get('data_sources', {})
        print(f"Data Sources Available: {len(sources)}")
        for source, data in sources.items():
            if 'error' in data:
                print(f"  ❌ {source}: ERROR")
            else:
                print(f"  ✅ {source}: Available")
        
        # Penalty analysis
        penalty_analysis = self.current_analysis.get('penalty_analysis', {})
        if penalty_analysis:
            counts = penalty_analysis.get('penalty_counts', {})
            print(f"\nPenalty Counts:")
            for source, count in counts.items():
                print(f"  {source}: {count}")
            
            # Data quality
            quality = penalty_analysis.get('data_quality', {})
            if quality:
                score = quality.get('overall_score', 0)
                print(f"\nData Quality Score: {score:.1f}%")
        
        # Issues summary
        issues = self.current_analysis.get('reconciliation_issues', [])
        if issues:
            high_priority = [i for i in issues if i.get('severity') == 'high']
            medium_priority = [i for i in issues if i.get('severity') == 'medium']
            low_priority = [i for i in issues if i.get('severity') == 'low']
            
            print(f"\nReconciliation Issues:")
            print(f"  🔴 High Priority: {len(high_priority)}")
            print(f"  🟡 Medium Priority: {len(medium_priority)}")
            print(f"  🟢 Low Priority: {len(low_priority)}")
        
        print("="*60)
    
    def interactive_review_loop(self):
        """Main interactive review loop."""
        while True:
            print("\n" + "-"*50)
            print("REVIEW OPTIONS:")
            print("-"*50)
            print("1. Review penalty discrepancies")
            print("2. Review complex penalty scenarios")
            print("3. Review data quality issues")
            print("4. Add context notes")
            print("5. View context notes")
            print("6. Generate detailed report")
            print("7. Export review results")
            print("8. Exit review session")
            print("-"*50)
            
            choice = input("\nSelect an option (1-8): ").strip()
            
            if choice == '1':
                self.review_penalty_discrepancies()
            elif choice == '2':
                self.review_complex_scenarios()
            elif choice == '3':
                self.review_data_quality()
            elif choice == '4':
                self.add_context_notes()
            elif choice == '5':
                self.view_context_notes()
            elif choice == '6':
                self.generate_detailed_report()
            elif choice == '7':
                self.export_review_results()
            elif choice == '8':
                print("\nEnding review session...")
                break
            else:
                print("Invalid choice. Please select 1-8.")
    
    def review_penalty_discrepancies(self):
        """Review penalty discrepancies in detail."""
        if not self.current_analysis:
            return
        
        discrepancies = self.current_analysis.get('penalty_analysis', {}).get('discrepancies', [])
        
        if not discrepancies:
            print("\n✅ No penalty discrepancies found!")
            return
        
        print(f"\n🔍 Reviewing {len(discrepancies)} penalty discrepancies:")
        print("-" * 60)
        
        for i, disc in enumerate(discrepancies, 1):
            print(f"\n{i}. {disc['type'].replace('_', ' ').title()}")
            print(f"   Description: {disc['description']}")
            print(f"   Severity: {disc['severity']}")
            
            if 'sources' in disc:
                print(f"   Sources: {', '.join(disc['sources'])}")
            
            if 'penalty' in disc:
                penalty = disc['penalty']
                print(f"   Penalty Details:")
                print(f"     Time: {penalty.get('time', 'N/A')}")
                print(f"     Player: {penalty.get('player', 'N/A')}")
                print(f"     Team: {penalty.get('team', 'N/A')}")
                print(f"     Description: {penalty.get('description', 'N/A')}")
                print(f"     Minutes: {penalty.get('penalty_minutes', 'N/A')}")
            
            # Ask for context
            context = input(f"\n   Add context for this discrepancy (or press Enter to skip): ").strip()
            if context:
                self.context_notes[f'discrepancy_{i}'] = {
                    'type': 'discrepancy',
                    'description': disc['description'],
                    'context': context,
                    'timestamp': datetime.now().isoformat()
                }
                print("   ✅ Context note added!")
            
            print("-" * 60)
    
    def review_complex_scenarios(self):
        """Review complex penalty scenarios."""
        if not self.current_analysis:
            return
        
        scenarios = self.current_analysis.get('penalty_analysis', {}).get('complex_scenarios', [])
        
        if not scenarios:
            print("\n✅ No complex penalty scenarios found!")
            return
        
        print(f"\n🎯 Reviewing {len(scenarios)} complex penalty scenarios:")
        print("-" * 60)
        
        for i, scenario in enumerate(scenarios, 1):
            print(f"\n{i}. {scenario['type'].replace('_', ' ').title()}")
            print(f"   Description: {scenario['description']}")
            print(f"   Impact: {scenario['impact']}")
            
            if 'penalties' in scenario:
                print(f"   Penalties involved: {len(scenario['penalties'])}")
                for j, penalty in enumerate(scenario['penalties'][:3], 1):  # Show first 3
                    print(f"     {j}. {penalty.get('time', 'N/A')} - {penalty.get('player', 'N/A')} - {penalty.get('description', 'N/A')}")
                if len(scenario['penalties']) > 3:
                    print(f"     ... and {len(scenario['penalties']) - 3} more")
            
            # Ask for context
            context = input(f"\n   Add context for this scenario (or press Enter to skip): ").strip()
            if context:
                self.context_notes[f'scenario_{i}'] = {
                    'type': 'complex_scenario',
                    'description': scenario['description'],
                    'context': context,
                    'timestamp': datetime.now().isoformat()
                }
                print("   ✅ Context note added!")
            
            print("-" * 60)
    
    def review_data_quality(self):
        """Review data quality issues."""
        if not self.current_analysis:
            return
        
        quality = self.current_analysis.get('penalty_analysis', {}).get('data_quality', {})
        
        if not quality:
            print("\n❌ No data quality information available!")
            return
        
        print(f"\n📊 Data Quality Review:")
        print("-" * 60)
        
        # Overall score
        score = quality.get('overall_score', 0)
        print(f"Overall Quality Score: {score:.1f}%")
        
        if score >= 80:
            print("🎉 Excellent data quality!")
        elif score >= 60:
            print("⚠️  Good data quality with room for improvement")
        else:
            print("🚨 Poor data quality - significant issues detected")
        
        # Completeness by source
        completeness = quality.get('completeness', {})
        if completeness:
            print(f"\nData Completeness by Source:")
            for source, status in completeness.items():
                if status == 'complete':
                    print(f"  ✅ {source}: Complete")
                elif status == 'partial':
                    print(f"  ⚠️  {source}: Partial")
                else:
                    print(f"  ❌ {source}: Missing")
        
        # Consistency
        consistency = quality.get('consistency', {})
        if consistency:
            print(f"\nData Consistency:")
            for metric, status in consistency.items():
                if status == 'consistent':
                    print(f"  ✅ {metric}: Consistent")
                else:
                    print(f"  ❌ {metric}: Inconsistent")
        
        # Ask for quality context
        context = input(f"\nAdd context about data quality (or press Enter to skip): ").strip()
        if context:
            self.context_notes['data_quality'] = {
                'type': 'data_quality',
                'score': score,
                'context': context,
                'timestamp': datetime.now().isoformat()
            }
            print("✅ Data quality context note added!")
        
        print("-" * 60)
    
    def add_context_notes(self):
        """Add general context notes."""
        print(f"\n📝 Add Context Notes:")
        print("-" * 60)
        
        note_type = input("Note type (e.g., 'general', 'penalty_rules', 'data_source', 'custom'): ").strip()
        if not note_type:
            note_type = 'general'
        
        context = input("Enter context note: ").strip()
        if not context:
            print("No context provided. Skipping...")
            return
        
        self.context_notes[f'context_{len(self.context_notes) + 1}'] = {
            'type': note_type,
            'context': context,
            'timestamp': datetime.now().isoformat()
        }
        
        print("✅ Context note added!")
    
    def view_context_notes(self):
        """View all context notes."""
        if not self.context_notes:
            print("\n📝 No context notes added yet.")
            return
        
        print(f"\n📝 Context Notes ({len(self.context_notes)} total):")
        print("-" * 60)
        
        for note_id, note in self.context_notes.items():
            print(f"\n{note_id}:")
            print(f"  Type: {note['type']}")
            print(f"  Context: {note['context']}")
            print(f"  Added: {note['timestamp']}")
            print("-" * 30)
    
    def generate_detailed_report(self):
        """Generate a detailed report with context notes."""
        if not self.current_analysis:
            return
        
        print("\n📋 Generating detailed report with context notes...")
        
        # Create enhanced analysis with context
        enhanced_analysis = self.current_analysis.copy()
        enhanced_analysis['context_notes'] = self.context_notes
        enhanced_analysis['review_timestamp'] = datetime.now().isoformat()
        
        # Generate report
        report = self.reviewer.generate_review_report(enhanced_analysis)
        
        # Add context notes to report
        if self.context_notes:
            report += "\n\n" + "="*80
            report += "\nCONTEXT NOTES FROM REVIEW SESSION"
            report += "\n" + "="*80
            
            for note_id, note in self.context_notes.items():
                report += f"\n{note_id}:\n"
                report += f"Type: {note['type']}\n"
                report += f"Context: {note['context']}\n"
                report += f"Added: {note['timestamp']}\n"
                report += "-" * 40 + "\n"
        
        # Save report
        output_dir = Path("reconciliation_reviews")
        output_dir.mkdir(exist_ok=True)
        
        game_id = self.current_analysis.get('game_id', 'unknown')
        report_file = output_dir / f"detailed_report_with_context_{game_id}.txt"
        
        with open(report_file, 'w') as f:
            f.write(report)
        
        print(f"✅ Detailed report saved to: {report_file}")
        
        # Display report
        print("\n" + "="*80)
        print("DETAILED REPORT")
        print("="*80)
        print(report)
    
    def export_review_results(self):
        """Export review results and context notes."""
        if not self.current_analysis:
            return
        
        print("\n💾 Exporting review results...")
        
        # Create export data
        export_data = {
            'game_info': {
                'game_id': self.current_analysis.get('game_id'),
                'season': self.current_analysis.get('season'),
                'analysis_timestamp': self.current_analysis.get('timestamp'),
                'review_timestamp': datetime.now().isoformat()
            },
            'reconciliation_analysis': self.current_analysis,
            'context_notes': self.context_notes,
            'review_summary': {
                'total_notes': len(self.context_notes),
                'note_types': list(set(note['type'] for note in self.context_notes.values())),
                'session_duration': 'N/A'  # Could calculate if we track start time
            }
        }
        
        # Save export
        output_dir = Path("reconciliation_reviews")
        output_dir.mkdir(exist_ok=True)
        
        game_id = self.current_analysis.get('game_id', 'unknown')
        export_file = output_dir / f"review_export_{game_id}.json"
        
        with open(export_file, 'w') as f:
            json.dump(export_data, f, indent=2)
        
        print(f"✅ Review results exported to: {export_file}")
        
        # Also save context notes separately
        context_file = output_dir / f"context_notes_{game_id}.json"
        with open(context_file, 'w') as f:
            json.dump(self.context_notes, f, indent=2)
        
        print(f"✅ Context notes saved to: {context_file}")

def main():
    """Main function for the interactive review interface."""
    print("NHL Data Reconciliation - Interactive Review Interface")
    print("=" * 60)
    
    # Get game information
    season = input("Enter season (e.g., 20242025): ").strip()
    if not season:
        season = "20242025"
    
    game_id = input("Enter game ID (e.g., 2024021130): ").strip()
    if not game_id:
        game_id = "2024021130"
    
    # Start review session
    interface = InteractiveReviewInterface()
    interface.start_review_session(season, game_id)

if __name__ == "__main__":
    main()

