# NHL Data Reconciliation Review System

This system provides a comprehensive framework for reviewing and reconciling differences between NHL data sources, with a focus on penalty data and collaborative analysis.

## Overview

The reconciliation review system consists of three main components:

1. **ReconciliationReviewer** - Core analysis engine that identifies differences between data sources
2. **InteractiveReviewInterface** - Interactive tool for collaborative review with context notes
3. **BatchReconciliationReviewer** - Batch processing for season-wide pattern analysis

## Key Features

- **Multi-source Data Analysis**: Compares data from Gamecenter Landing, Boxscores, Play-by-Play, and parsed HTML
- **Penalty Data Focus**: Specialized analysis for penalty reconciliation including complex scenarios
- **Interactive Review**: Collaborative interface for adding context and notes
- **Pattern Recognition**: Identifies common issues and trends across multiple games
- **Comprehensive Reporting**: Generates detailed reports with recommendations
- **Data Export**: Exports results in multiple formats for further analysis

## Installation and Setup

### Prerequisites

- Python 3.7+
- Required packages: `beautifulsoup4`, `pandas`, `pathlib`

### Setup

1. Ensure the `src/parse/html_penalty_parser.py` module is available
2. Place the review scripts in your project directory
3. Verify your storage structure matches the expected paths

## Usage

### 1. Individual Game Review

Use the interactive interface to review a single game:

```bash
python interactive_review.py
```

This will:
- Prompt for season and game ID
- Load and analyze all available data sources
- Present an interactive menu for review options
- Allow you to add context notes for discrepancies
- Generate detailed reports with your insights

### 2. Batch Season Review

Process multiple games to identify patterns:

```bash
python batch_reconciliation_review.py
```

This will:
- Process all available games in a season (or a specified number)
- Identify common reconciliation issues
- Analyze data source reliability
- Generate season-wide recommendations
- Export comprehensive analysis data

### 3. Direct Analysis

Use the core reviewer directly in your code:

```python
from reconciliation_review import ReconciliationReviewer

reviewer = ReconciliationReviewer()
analysis = reviewer.analyze_game_reconciliation("20242025", "2024021130")
report = reviewer.generate_review_report(analysis)
```

## Review Process

### Step 1: Data Source Analysis

The system automatically:
- Loads data from all available sources
- Identifies missing or corrupted data sources
- Extracts penalty information from each source
- Maps relationships between penalties across sources

### Step 2: Discrepancy Identification

Automatically detects:
- **Count Mismatches**: Different penalty counts between sources
- **Missing Penalties**: Penalties present in one source but not others
- **Data Inconsistencies**: Mismatched penalty minutes, types, or descriptions
- **Complex Scenarios**: Simultaneous penalties, team penalties, non-power play situations

### Step 3: Interactive Review

You can:
- Review each discrepancy in detail
- Add context notes explaining the difference
- Review complex penalty scenarios
- Assess data quality issues
- Add general context about penalty rules or data sources

### Step 4: Report Generation

The system generates:
- **Detailed Analysis**: Complete reconciliation findings
- **Context Notes**: Your insights and explanations
- **Recommendations**: Prioritized actions for improvement
- **Export Data**: Structured data for further analysis

## Review Options

### 1. Review Penalty Discrepancies
- Examine each identified discrepancy
- Add context explaining why differences exist
- Note any systematic issues or data source problems

### 2. Review Complex Penalty Scenarios
- Analyze simultaneous penalties
- Review team penalties and serving players
- Examine non-power play penalties
- Add context about NHL rules and scenarios

### 3. Review Data Quality Issues
- Assess completeness of each data source
- Identify consistency problems
- Note areas for improvement

### 4. Add Context Notes
- General notes about penalty rules
- Data source characteristics
- Custom observations and insights

## Context Note Types

### Discrepancy Context
Explain why specific discrepancies exist:
- "Penalty was offsetting, so no power play resulted"
- "Team penalty served by different player"
- "Timing difference due to clock synchronization"

### Complex Scenario Context
Document NHL rules and scenarios:
- "Simultaneous penalties result in 4-on-4, not power play"
- "Bench penalties must be served by designated player"
- "Fighting penalties don't create power plays"

### Data Source Context
Note characteristics of different sources:
- "HTML reports have more detailed penalty descriptions"
- "Gamecenter Landing has most accurate timing"
- "Boxscores only show PIM totals, not individual penalties"

## Output Files

### Individual Game Review
- `reconciliation_analysis_{game_id}.json` - Complete analysis data
- `reconciliation_report_{game_id}.txt` - Human-readable report
- `interactive_review_{game_id}.json` - Interactive review data
- `context_notes_{game_id}.json` - Your context notes

### Batch Season Review
- `season_report_{season}.txt` - Season-wide analysis report
- `season_summary_{season}.json` - Season summary data
- `patterns_{season}.json` - Identified patterns and trends
- Individual game analysis files in `individual_games/` subdirectory

## Data Quality Metrics

The system calculates:
- **Overall Quality Score**: Percentage-based assessment
- **Source Completeness**: How complete each data source is
- **Data Consistency**: How well sources agree with each other
- **Error Rates**: Frequency of data source failures

## Recommendations

The system generates prioritized recommendations:
- **High Priority**: Critical issues affecting data accuracy
- **Medium Priority**: Important improvements for reliability
- **Low Priority**: Process improvements and monitoring

## Best Practices

### 1. Start with Individual Games
- Begin with a few games to understand the system
- Add context notes as you review
- Identify patterns in discrepancies

### 2. Use Batch Review for Patterns
- Process multiple games to find systematic issues
- Focus on common discrepancy types
- Prioritize improvements based on frequency

### 3. Document Context Thoroughly
- Explain why discrepancies exist
- Note NHL rules and scenarios
- Document data source characteristics
- Add examples and edge cases

### 4. Iterate and Improve
- Use findings to improve data extraction
- Update parsing logic based on patterns
- Establish validation rules for common issues

## Troubleshooting

### Common Issues

1. **Missing Data Sources**: Ensure all expected JSON and HTML files exist
2. **Parsing Errors**: Check HTML file formats and encoding
3. **Memory Issues**: For large seasons, process games in batches
4. **Path Issues**: Verify storage directory structure matches expectations

### Debug Mode

Enable debug logging by modifying the logger configuration in the scripts.

## Integration with Main Pipeline

The reconciliation review system integrates with your main data processing pipeline:

1. **Data Collection**: Uses collected JSON and HTML files
2. **HTML Parsing**: Leverages the enhanced HTML penalty parser
3. **Reconciliation Analysis**: Identifies differences and issues
4. **Context Application**: Your insights improve understanding
5. **Recommendation Implementation**: Findings guide pipeline improvements

## Next Steps

After completing a review session:

1. **Implement Recommendations**: Address high-priority issues
2. **Update Parsing Logic**: Improve HTML extraction based on findings
3. **Establish Validation**: Create automated checks for common issues
4. **Monitor Progress**: Track improvement in reconciliation rates
5. **Expand Coverage**: Apply insights to other data types

## Support

For questions or issues:
1. Check the generated reports for detailed analysis
2. Review context notes for explanations
3. Examine exported data for further investigation
4. Use the interactive interface for detailed exploration

---

This system provides the foundation for achieving 100% reconciliation accuracy through systematic analysis and collaborative review. Your domain expertise and context notes are essential for understanding the nuances of NHL data and improving the overall data quality.

