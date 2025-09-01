#!/usr/bin/env python3
"""
Shift Charts Data Models
========================

Pydantic models for NHL shift charts data structure.
"""

from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime
from enum import Enum

class ShiftEventType(Enum):
    """Shift event types based on typeCode."""
    SHIFT_START = 517
    GOAL = 505
    PENALTY = 509
    FACE_OFF = 502
    SHOT = 506
    HIT = 503
    GIVEAWAY = 504
    TAKEAWAY = 525
    BLOCKED_SHOT = 508
    MISSED_SHOT = 507

class ShiftChartEntry(BaseModel):
    """Individual shift chart entry."""
    id: int = Field(..., description="Unique shift entry ID")
    detailCode: int = Field(..., description="Detail code for the shift")
    duration: Optional[str] = Field(None, description="Shift duration in MM:SS format")
    endTime: Optional[str] = Field(None, description="Shift end time in MM:SS format")
    eventDescription: Optional[str] = Field(None, description="Event description (e.g., 'EVG', 'PPG')")
    eventDetails: Optional[str] = Field(None, description="Event details (e.g., 'John Tavares, Mitchell Marner')")
    eventNumber: int = Field(..., description="Event number in the game")
    firstName: str = Field(..., description="Player first name")
    gameId: int = Field(..., description="Game ID")
    hexValue: str = Field(..., description="Color hex value for visualization")
    lastName: str = Field(..., description="Player last name")
    period: int = Field(..., description="Period number (1, 2, 3, 4 for OT)")
    playerId: int = Field(..., description="Player ID")
    shiftNumber: int = Field(..., description="Shift number for the player")
    startTime: str = Field(..., description="Shift start time in MM:SS format")
    teamAbbrev: str = Field(..., description="Team abbreviation (e.g., 'BOS', 'TOR')")
    teamId: int = Field(..., description="Team ID")
    teamName: str = Field(..., description="Full team name")
    typeCode: int = Field(..., description="Event type code")

class ShiftChartResponse(BaseModel):
    """Complete shift chart API response."""
    data: List[ShiftChartEntry] = Field(..., description="List of shift chart entries")
    total: int = Field(..., description="Total number of entries")

class PlayerShiftSummary(BaseModel):
    """Summary of player shifts for a game."""
    player_id: int = Field(..., description="Player ID")
    player_name: str = Field(..., description="Player full name")
    team_abbrev: str = Field(..., description="Team abbreviation")
    total_shifts: int = Field(..., description="Total number of shifts")
    total_time_on_ice: str = Field(..., description="Total time on ice in MM:SS format")
    average_shift_length: str = Field(..., description="Average shift length in MM:SS format")
    longest_shift: str = Field(..., description="Longest shift duration")
    shortest_shift: str = Field(..., description="Shortest shift duration")
    goals: int = Field(0, description="Number of goals scored during shifts")
    assists: int = Field(0, description="Number of assists during shifts")
    penalties: int = Field(0, description="Number of penalties taken during shifts")

class TeamShiftSummary(BaseModel):
    """Summary of team shifts for a game."""
    team_abbrev: str = Field(..., description="Team abbreviation")
    team_name: str = Field(..., description="Full team name")
    total_players: int = Field(..., description="Number of players with shifts")
    total_shifts: int = Field(..., description="Total number of shifts")
    total_time_on_ice: str = Field(..., description="Total team time on ice")
    player_summaries: List[PlayerShiftSummary] = Field(..., description="Individual player summaries")

class GameShiftSummary(BaseModel):
    """Complete game shift summary."""
    game_id: int = Field(..., description="Game ID")
    home_team: TeamShiftSummary = Field(..., description="Home team shift summary")
    away_team: TeamShiftSummary = Field(..., description="Away team shift summary")
    total_entries: int = Field(..., description="Total shift chart entries")
    collection_timestamp: datetime = Field(default_factory=datetime.now, description="When data was collected")

def parse_shift_chart_data(raw_data: Dict[str, Any]) -> ShiftChartResponse:
    """Parse raw shift chart data into structured format."""
    return ShiftChartResponse(**raw_data)

def create_player_shift_summary(entries: List[ShiftChartEntry], player_id: int) -> PlayerShiftSummary:
    """Create a player shift summary from shift chart entries."""
    player_entries = [e for e in entries if e.playerId == player_id]
    if not player_entries:
        return None
    
    # Get player info from first entry
    first_entry = player_entries[0]
    
    # Calculate shift statistics
    total_shifts = len([e for e in player_entries if e.typeCode == ShiftEventType.SHIFT_START.value])
    
    # Calculate time on ice
    shift_durations = []
    goals = 0
    assists = 0
    penalties = 0
    
    for entry in player_entries:
        if entry.duration:
            shift_durations.append(entry.duration)
        
        # Count events
        if entry.typeCode == ShiftEventType.GOAL.value:
            goals += 1
        elif entry.typeCode == ShiftEventType.PENALTY.value:
            penalties += 1
    
    # Calculate time statistics
    total_seconds = sum(parse_time_to_seconds(d) for d in shift_durations if d)
    total_time = format_seconds_to_time(total_seconds)
    
    if shift_durations:
        avg_seconds = total_seconds // len(shift_durations)
        avg_time = format_seconds_to_time(avg_seconds)
        longest_shift = max(shift_durations, key=lambda x: parse_time_to_seconds(x) if x else 0)
        shortest_shift = min(shift_durations, key=lambda x: parse_time_to_seconds(x) if x else 0)
    else:
        avg_time = "00:00"
        longest_shift = "00:00"
        shortest_shift = "00:00"
    
    return PlayerShiftSummary(
        player_id=player_id,
        player_name=f"{first_entry.firstName} {first_entry.lastName}",
        team_abbrev=first_entry.teamAbbrev,
        total_shifts=total_shifts,
        total_time_on_ice=total_time,
        average_shift_length=avg_time,
        longest_shift=longest_shift,
        shortest_shift=shortest_shift,
        goals=goals,
        assists=assists,
        penalties=penalties
    )

def create_team_shift_summary(entries: List[ShiftChartEntry], team_abbrev: str) -> TeamShiftSummary:
    """Create a team shift summary from shift chart entries."""
    team_entries = [e for e in entries if e.teamAbbrev == team_abbrev]
    if not team_entries:
        return None
    
    # Get team info from first entry
    first_entry = team_entries[0]
    
    # Get unique players
    player_ids = list(set(e.playerId for e in team_entries))
    
    # Create player summaries
    player_summaries = []
    for player_id in player_ids:
        summary = create_player_shift_summary(entries, player_id)
        if summary:
            player_summaries.append(summary)
    
    # Calculate team totals
    total_shifts = sum(p.total_shifts for p in player_summaries)
    total_seconds = sum(parse_time_to_seconds(p.total_time_on_ice) for p in player_summaries)
    total_time = format_seconds_to_time(total_seconds)
    
    return TeamShiftSummary(
        team_abbrev=team_abbrev,
        team_name=first_entry.teamName,
        total_players=len(player_summaries),
        total_shifts=total_shifts,
        total_time_on_ice=total_time,
        player_summaries=player_summaries
    )

def create_game_shift_summary(entries: List[ShiftChartEntry]) -> GameShiftSummary:
    """Create a complete game shift summary."""
    if not entries:
        return None
    
    # Get unique teams
    teams = list(set(e.teamAbbrev for e in entries))
    if len(teams) != 2:
        raise ValueError(f"Expected 2 teams, found {len(teams)}")
    
    # Create team summaries
    home_team = create_team_shift_summary(entries, teams[0])
    away_team = create_team_shift_summary(entries, teams[1])
    
    return GameShiftSummary(
        game_id=entries[0].gameId,
        home_team=home_team,
        away_team=away_team,
        total_entries=len(entries)
    )

def parse_time_to_seconds(time_str: str) -> int:
    """Parse MM:SS time string to seconds."""
    if not time_str:
        return 0
    
    try:
        minutes, seconds = map(int, time_str.split(':'))
        return minutes * 60 + seconds
    except (ValueError, AttributeError):
        return 0

def format_seconds_to_time(seconds: int) -> str:
    """Format seconds to MM:SS time string."""
    minutes = seconds // 60
    remaining_seconds = seconds % 60
    return f"{minutes:02d}:{remaining_seconds:02d}"
