## NHL HTML Reports - Comprehensive Data Dimensionality Analysis

This reference documents the complete analytical capabilities of each NHL HTML report type beyond current reconciliation usage. Understanding these dimensions is critical for expanding into advanced analytics (time on ice, power play analysis, situational stats, etc.).

### Report Overview & Full Data Structures

#### GS (Game Summary) - Complete Team & Player Game Stats
**Primary Purpose**: Official game summary with comprehensive team/player statistics and meta-information

**Data Scope & Granularity**:
- **Team Level**: Period-by-period scoring breakdown (REG/OT), final scores, team statistics
- **Player Level**: Complete game statistics (goals, assists, +/-, PIM, shots, hits, blocks, faceoffs, TOI)
- **Game Meta**: Officials, three stars, venue, penalties by period
- **Phase Breakdown**: Team scoring by REG/OT; player stats are cumulative game totals (REG+OT)

**Key Analytical Capabilities**:
- ✅ **Time on Ice**: Individual TOI, shifts, average shift length
- ✅ **Penalty Analysis**: Complete penalty summary with player, time, type, duration
- ✅ **Situational Strength**: Goal strength indicators (EV, PP, SH, EN, PS)
- ✅ **Shot Metrics**: Player shots, attempts blocked, missed shots
- ✅ **Physical Play**: Hits given/taken, blocked shots
- ✅ **Faceoffs**: Individual player faceoff W/L, percentages
- ✅ **Plus/Minus**: Player plus/minus ratings
- ❌ **Power Play TOI**: Not broken down by situation
- ❌ **Individual Shootout Goals**: Only outcome (winner/loser)
- ❌ **Phase-specific Player Stats**: Player stats not separated by REG/OT

#### ES (Event Summary) - Detailed Player Performance Analytics  
**Primary Purpose**: Most comprehensive individual player statistics with advanced situational breakdown

**Data Scope & Granularity**:
- **Player Level**: Most detailed individual stats available in NHL HTML reports
- **Team Level**: Computed aggregates from player stats (goals, assists, shots, faceoffs)
- **Situational Breakdown**: Power play, short-handed, even strength TOI
- **Phase Limitation**: No REG/OT/SO separation—all stats are game totals

**Key Analytical Capabilities**:
- ✅ **Advanced TOI Breakdown**: Total, shifts, avg shift, PP time, SH time, EV time
- ✅ **Shot Metrics**: Shots, attempts blocked, missed shots, shot attempts
- ✅ **Puck Possession**: Giveaways, takeaways
- ✅ **Physical Play**: Hits, blocked shots
- ✅ **Faceoff Analytics**: W/L, percentages by player
- ✅ **Defensive Metrics**: Blocked shots, takeaways
- ✅ **Game Flow**: Most complete individual player performance picture
- ❌ **Phase Breakdown**: Cannot separate REG vs OT vs SO performance
- ❌ **Power Play Success**: No PP goals/assists specifically tracked
- ❌ **Shootout Data**: No individual shootout attempt tracking

#### PL (Play-by-Play) - Event-Level Granular Data
**Primary Purpose**: Complete event stream with precise timing and game situation context

**Data Scope & Granularity**:
- **Event Level**: Every game event with exact timing, location, players involved
- **Phase Breakdown**: Complete REG/OT/SO separation with period-specific data
- **Situational Context**: Game state, score, man advantage situation for each event
- **Player Actions**: Goals, shots, hits, penalties, faceoffs, saves, blocks, takeaways, giveaways

**Key Analytical Capabilities**:
- ✅ **Complete Event Stream**: Every action with timing and context
- ✅ **Phase-Specific Analysis**: Can analyze REG vs OT vs SO performance separately
- ✅ **Situational Hockey**: Events during PP, SH, 4v4, 3v3, 6v5, etc.
- ✅ **Game Flow Analysis**: Event sequence, momentum shifts, scoring chances
- ✅ **Individual Shootout Goals**: Complete shootout attempt tracking
- ✅ **Shot Location**: Shot coordinates and types
- ✅ **Penalty Context**: Exact penalty timing and game impact
- ✅ **Goalie Analysis**: Save sequences, goals against context
- ❌ **Time on Ice**: Not available in PL reports
- ❌ **Assist Reliability**: Assist data often incomplete/unreliable in PL summaries
- ❌ **Player Totals**: Must be computed from event stream

### Advanced Analytics Potential by Report Type

#### Time on Ice Analysis
- **GS**: Basic TOI (total, shifts, avg) ✅
- **ES**: Advanced TOI breakdown (PP/SH/EV splits) ✅
- **PL**: No TOI data ❌

#### Power Play/Penalty Kill Analysis  
- **GS**: Goal strength indicators, penalty summary ✅
- **ES**: PP/SH TOI, but not PP-specific goals/assists ⚠️
- **PL**: Complete PP/PK event context and timing ✅

#### Game Situation Analytics
- **GS**: Limited to goal strength ⚠️
- **ES**: Situational TOI breakdown ✅
- **PL**: Complete game state context for every event ✅

#### Shot Quality & Location
- **GS**: Basic shot counts ⚠️
- **ES**: Shot attempts, blocks, misses ✅
- **PL**: Shot coordinates and types ✅

#### Momentum & Game Flow
- **GS**: Period breakdowns only ⚠️
- **ES**: No temporal dimension ❌
- **PL**: Complete event sequencing ✅

### Data Integration Strategy for Advanced Analytics

**For Time on Ice Analysis**:
- Primary: ES (situational breakdown) + GS (basic validation)
- PL: Use for game situation context only

**For Power Play Analysis**:
- Goals/Points: GS + PL event validation
- TOI: ES situational breakdown
- Context: PL complete event stream

**For Game Flow/Momentum**:
- Primary: PL complete event stream
- Validation: GS period summaries
- Player context: ES individual stats

**For Individual Performance**:
- Comprehensive stats: ES (most complete)
- Phase breakdown: Computed from PL events
- Validation: GS summary stats

### Current Reconciliation vs Full Capability

**Current Usage** (reconciliation-focused):
- Basic goal/assist counting across sources
- Phase separation (REG/OT/SO) validation
- Simple discrepancy flagging

**Unexploited Analytical Potential**:
- Situational performance analysis (PP/SH/EV)
- Shot quality and efficiency metrics
- Game state and momentum analysis
- Advanced defensive metrics (blocks, takeaways)
- Faceoff performance by situation
- Power play efficiency and timing
- Individual player impact metrics

### Future Analytics Expansion Recommendations

1. **Time on Ice Analytics**: Leverage ES situational TOI breakdowns for player usage analysis
2. **Power Play Effectiveness**: Combine GS goal strength + ES TOI + PL timing
3. **Shot Quality Models**: Use PL shot coordinates + ES attempt data + GS shot totals
4. **Game Situation Impact**: PL event stream analysis with player context from ES
5. **Defensive Metrics**: ES takeaway/giveaway + PL defensive events + GS blocked shots
6. **Goalie Performance**: PL save sequences + GS goalie stats + ES context


