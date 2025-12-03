"""
Fantasy Baseball League Data Collection System
Automatically updates weekly during the season

Uses Yahoo Fantasy API for:
- Rosters (who owns which player)
- Team info (logos, manager names)
- Transactions
- Scoring settings

Uses Fangraphs (via pybaseball) for:
- Player batting stats
- Player pitching stats
"""

import json
import os
import re
import time
import unicodedata
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from yahoo_oauth import OAuth2
from yahoo_fantasy_api import Game, League
import pandas as pd

# Try to import pybaseball - will be used for Fangraphs data
try:
    from pybaseball import batting_stats, pitching_stats, cache
    # Enable caching to avoid repeated requests
    cache.enable()
    PYBASEBALL_AVAILABLE = True
except ImportError:
    PYBASEBALL_AVAILABLE = False
    print("WARNING: pybaseball not installed. Install with: pip install pybaseball")

# =============================================================================
# CONFIGURATION
# =============================================================================

CURRENT_SEASON = 2025
HISTORICAL_SEASONS = [2019, 2020, 2021, 2022, 2023, 2024]
DATA_DIR = "data"

# Override league IDs for specific years (if needed)
LEAGUE_ID_OVERRIDES = {
    2020: "398.l.17906"  # Use this specific league for 2020
}

# Manager name corrections - maps raw names to standardized names
MANAGER_NAME_MAP = {
    # Handles case variations (John, JOHN -> John)
    # Will be applied after title case conversion
}

# Manager team disambiguation for 2023 (Logan C vs Logan S)
MANAGER_TEAM_2023 = {
    "Draft Pool": "Logan C",
    "Peanut Butter & Elly": "Logan S"
}

# =============================================================================
# DEFAULT SCORING SETTINGS (Yahoo Points League defaults)
# These will be overwritten by actual league settings if available
# =============================================================================

DEFAULT_BATTING_SCORING = {
    '1B': 2.6,      # Singles
    '2B': 5.2,      # Doubles
    '3B': 7.8,      # Triples
    'HR': 10.4,     # Home Runs
    'RBI': 1.9,     # Runs Batted In
    'R': 1.9,       # Runs
    'BB': 2.6,      # Walks
    'HBP': 2.6,     # Hit By Pitch
    'SB': 4.2,      # Stolen Bases
    'CS': -2.6,     # Caught Stealing
    'SO': -1,       # Strikeouts (batting)
}

DEFAULT_PITCHING_SCORING = {
    'IP': 5,        # Innings Pitched (per full inning)
    'W': 4,         # Wins
    'L': -4,        # Losses
    'SV': 8,        # Saves
    'HLD': 4,       # Holds
    'ER': -3,       # Earned Runs
    'H': -1,        # Hits Allowed
    'BB': -1,       # Walks Allowed
    'K': 3,         # Strikeouts (pitching)
    'QS': 4,        # Quality Starts
    'CG': 5,        # Complete Games
    'SO': 5,        # Shutouts
}

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def safe_int(value, default: int = 0) -> int:
    """Safely convert a value to int, handling NaN and None."""
    import math
    if value is None:
        return default
    try:
        if isinstance(value, float) and math.isnan(value):
            return default
        return int(value)
    except (ValueError, TypeError):
        return default

def safe_float(value, default: float = 0.0) -> float:
    """Safely convert a value to float, handling NaN and None."""
    import math
    if value is None:
        return default
    try:
        float_val = float(value)
        if math.isnan(float_val):
            return default
        return float_val
    except (ValueError, TypeError):
        return default

def normalize_manager_name(manager_name: str, year: int = None, team_name: str = None) -> str:
    """Normalize manager names to proper case and handle special cases"""
    normalized = manager_name.strip().title()
    
    if year == 2023 and team_name:
        if team_name in MANAGER_TEAM_2023:
            return MANAGER_TEAM_2023[team_name]
    
    if normalized in MANAGER_NAME_MAP:
        return MANAGER_NAME_MAP[normalized]
    
    return normalized

def normalize_player_name(name: str) -> str:
    """Normalize player names for matching between Yahoo and Fangraphs."""
    if not name:
        return ""
    
    # Handle double-encoded UTF-8 from pybaseball's batting_stats_range/pitching_stats_range
    # These functions return strings like 'Julio Rodr\xc3\xadguez' where \xc3\xad should be í
    
    # Method 1: Handle literal escape sequences (string contains actual \x characters)
    # This converts 'Rodr\xc3\xadguez' (literal backslash-x) to 'Rodríguez'
    if '\\x' in name:
        try:
            # decode('unicode_escape') converts \xNN to actual bytes
            # then encode as latin-1 and decode as utf-8 to get proper unicode
            name = name.encode('utf-8').decode('unicode_escape').encode('latin-1').decode('utf-8')
        except (UnicodeDecodeError, UnicodeEncodeError):
            pass
    
    # Method 2: Handle bytes stored as latin-1 characters (high bytes 128-255)
    # The bytes are correct UTF-8 but stored as latin-1 characters in the string
    if any(128 <= ord(c) <= 255 for c in name):
        try:
            # Encode as latin-1 (which preserves the byte values) then decode as UTF-8
            name = name.encode('latin-1').decode('utf-8')
        except (UnicodeDecodeError, UnicodeEncodeError):
            # If that fails, the string is probably already correct
            pass
    
    # Remove common suffixes/parentheticals
    name = re.sub(r'\s*\(Batter\)|\s*\(Pitcher\)', '', name, flags=re.IGNORECASE)
    
    # Use Unicode NFD normalization to decompose accented characters,
    # then remove the combining diacritical marks
    # This handles ALL accented characters (á, é, í, ó, ú, ñ, ü, etc.)
    name = unicodedata.normalize('NFD', name)
    name = ''.join(c for c in name if unicodedata.category(c) != 'Mn')
    
    name = ' '.join(name.split())
    return name.lower().strip()

def match_player_name(yahoo_name: str, fangraphs_names: List[str]) -> Optional[str]:
    """Find the best matching Fangraphs name for a Yahoo player name."""
    normalized_yahoo = normalize_player_name(yahoo_name)
    
    # Exact match
    for fg_name in fangraphs_names:
        if normalize_player_name(fg_name) == normalized_yahoo:
            return fg_name
    
    # Match without Jr., Sr., II, III, etc.
    suffix_pattern = r'\s+(jr\.?|sr\.?|ii|iii|iv)$'
    yahoo_no_suffix = re.sub(suffix_pattern, '', normalized_yahoo, flags=re.IGNORECASE)
    
    for fg_name in fangraphs_names:
        fg_normalized = normalize_player_name(fg_name)
        fg_no_suffix = re.sub(suffix_pattern, '', fg_normalized, flags=re.IGNORECASE)
        
        if yahoo_no_suffix == fg_no_suffix:
            return fg_name
    
    # Match last name + first initial
    yahoo_parts = normalized_yahoo.split()
    if len(yahoo_parts) >= 2:
        yahoo_last = yahoo_parts[-1].rstrip('.')
        yahoo_first_initial = yahoo_parts[0][0] if yahoo_parts[0] else ''
        
        for fg_name in fangraphs_names:
            fg_normalized = normalize_player_name(fg_name)
            fg_parts = fg_normalized.split()
            if len(fg_parts) >= 2:
                fg_last = fg_parts[-1].rstrip('.')
                fg_first_initial = fg_parts[0][0] if fg_parts[0] else ''
                
                if yahoo_last == fg_last and yahoo_first_initial == fg_first_initial:
                    return fg_name
    
    return None


def debug_player_names(year: int = 2024):
    """Debug command to compare how Yahoo and Fangraphs store player names."""
    print("=" * 60)
    print(f"DEBUGGING PLAYER NAME STORAGE FOR {year}")
    print("=" * 60)
    
    oauth = setup_oauth()
    
    # Get Yahoo roster names
    print("\n--- Fetching Yahoo roster names ---")
    rosters = get_rosters(oauth, year)
    yahoo_players = []
    for team_key, players in rosters.items():
        yahoo_players.extend(players)
    
    yahoo_names = [p.get('name', '') for p in yahoo_players]
    
    # Get Fangraphs names
    print("\n--- Fetching Fangraphs names ---")
    settings = get_league_settings(oauth, year)
    start_date = settings.get('start_date')
    end_date = settings.get('end_date')
    
    batting_df = get_fangraphs_batting_stats(year, start_date, end_date)
    pitching_df = get_fangraphs_pitching_stats(year, start_date, end_date)
    
    fg_batting_names = batting_df['Name'].tolist() if not batting_df.empty else []
    fg_pitching_names = pitching_df['Name'].tolist() if not pitching_df.empty else []
    fg_all_names = fg_batting_names + fg_pitching_names
    
    # Show raw encoding of a few Fangraphs names with special chars
    print("\n--- Sample Fangraphs names with special characters ---")
    fg_special = [n for n in fg_all_names if any(ord(c) > 127 for c in n)][:10]
    for name in fg_special:
        print(f"  Raw: {repr(name)}")
        print(f"  Bytes: {name.encode('utf-8', errors='replace')}")
        print(f"  Normalized: {repr(normalize_player_name(name))}")
        print()
    
    # Search for specific known players in Fangraphs
    print("\n--- Searching for specific players in Fangraphs data ---")
    test_players = ['rodriguez', 'ramirez', 'hernandez', 'diaz', 'lopez', 'perez']
    for search in test_players:
        matches = [n for n in fg_all_names if search in n.lower() or search in normalize_player_name(n)]
        print(f"  '{search}': {matches[:5]}")
    
    # Find names with special characters
    print("\n--- Yahoo names with special characters ---")
    for name in yahoo_names:
        if any(ord(c) > 127 for c in name):
            normalized = normalize_player_name(name)
            print(f"  Yahoo: {repr(name)}")
            print(f"    Normalized: {repr(normalized)}")
            
            # Try to find match in Fangraphs
            match = match_player_name(name, fg_all_names)
            if match:
                print(f"    FG Match: {repr(match)}")
                print(f"    FG Normalized: {repr(normalize_player_name(match))}")
            else:
                print(f"    NO MATCH FOUND")
                # Show similar names (search in both raw and normalized)
                last_name = normalized.split()[-1] if normalized.split() else ''
                for fg_name in fg_all_names:
                    fg_norm = normalize_player_name(fg_name)
                    if last_name and last_name in fg_norm:
                        print(f"      Similar FG: {repr(fg_name)} -> {repr(fg_norm)}")
            print()
    
    # Show unmatched players
    print("\n--- All unmatched Yahoo players ---")
    unmatched = []
    for name in yahoo_names:
        if not match_player_name(name, fg_all_names):
            unmatched.append(name)
    
    for name in unmatched[:20]:
        print(f"  {repr(name)} -> {repr(normalize_player_name(name))}")
    
    if len(unmatched) > 20:
        print(f"  ... and {len(unmatched) - 20} more")
    
    print(f"\nTotal: {len(unmatched)} unmatched out of {len(yahoo_names)} players")
    print("=" * 60)

def _build_scoring_df(lg: League, settings: Dict) -> pd.DataFrame:
    """
    Refactored function to generate a DataFrame of scoring settings.
    This resolves the stat name lookups more efficiently and robustly than 
    the original nested loop.
    """
    # 1. Get a map of {StatID: StatName/Abbreviation}
    try:
        stat_categories = lg.stat_categories()
        # Prefer 'display_name' (e.g. 'HR') over 'name' (e.g. 'Home Runs')
        id_to_name = {
            str(s['stat_id']): s.get('display_name') or s.get('name') 
            for s in stat_categories
        }
    except Exception as e:
        print(f"    ⚠ Warning: Could not fetch dynamic stat categories: {e}. Using internal map.")
        id_to_name = {} # Fallback to empty map
        id_to_name.update(globals().get('YAHOO_STAT_ID_MAP', {}))

    # 2. Extract point modifiers
    point_dic = settings.get('stat_modifiers', {}).get('stats', [])
    data = []
    
    for modifier in point_dic:
        stat_id = str(modifier['stat']['stat_id'])
        value = float(modifier['stat']['value'])
        
        # Use the name map
        stat_name = id_to_name.get(stat_id, f"Stat_{stat_id}")

        data.append({
            'stat_id': stat_id, 
            'stat_name': stat_name, 
            'value': value
        })

    return pd.DataFrame(data)

# =============================================================================
# SETUP & AUTHENTICATION
# =============================================================================

def setup_oauth():
    """Initialize OAuth for Yahoo Fantasy API"""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    oauth_path = os.path.join(script_dir, 'oauth2.json')
    
    if not os.path.exists(oauth_path):
        raise FileNotFoundError(f"oauth2.json not found at: {oauth_path}")
    
    oauth = OAuth2(None, None, from_file=oauth_path)
    return oauth

def get_league_id_by_name(oauth, year: int) -> str:
    """Get league ID for a specific year"""
    if year in LEAGUE_ID_OVERRIDES:
        print(f"  Using override league ID: {LEAGUE_ID_OVERRIDES[year]}")
        return LEAGUE_ID_OVERRIDES[year]
    
    gm = Game(oauth, 'mlb')
    league_ids = gm.league_ids(year=year)
    
    if league_ids:
        return league_ids[0]
    return None

# =============================================================================
# YAHOO API DATA FUNCTIONS
# =============================================================================

def get_teams(oauth, year: int) -> List[Dict]:
    """Get all teams for a given season"""
    gm = Game(oauth, 'mlb')
    league_id = get_league_id_by_name(oauth, year)
    
    if not league_id:
        return []
    
    lg = League(oauth, league_id)
    standings = lg.standings()
    teams = lg.teams()

    keys = [standings[i]['team_key'] for i in range(len(standings))]
    
    tm = []
    for key in keys:
        tm.append({
            'team_key': teams[key]['team_key'],
            'team_name': teams[key]['name'],
            'team_logo': teams[key]['team_logos'][0]['team_logo']['url'],
            'manager': teams[key]['managers'][0]['manager']['nickname']
        })
    return tm

def get_week_scores(oauth, year: int, week: int) -> List[Dict]:
    """Get matchup scores for a specific week"""
    gm = Game(oauth, 'mlb')
    league_id = get_league_id_by_name(oauth, year)
    
    if not league_id:
        return []
    
    lg = League(oauth, league_id)

    try:
        matchup_data = lg.matchups(week=week)
    except:
        return []

    data = []
    
    try:
        matchups = matchup_data['fantasy_content']['league'][1]['scoreboard']['0']['matchups']
        
        for i in range(0, 6):
            try:
                matchup_key = str(i)
                if matchup_key not in matchups:
                    break
                    
                matchup = matchups[matchup_key]['matchup']['0']['teams']
                
                team1 = matchup['0']['team']
                team2 = matchup['1']['team']
                
                team1_key = team1[0][0]['team_key']
                team1_score = team1[1].get('team_points', {}).get('total', 0)
                team2_key = team2[0][0]['team_key']
                team2_score = team2[1].get('team_points', {}).get('total', 0)
                week_num = team1[1].get('team_stats', {}).get('week', week)

                data.append({
                    'team_key': team1_key,
                    'team_score': float(team1_score),
                    'week': int(week_num),
                    'opponent_key': team2_key,
                    'opponent_score': float(team2_score)
                })
                data.append({
                    'team_key': team2_key,
                    'team_score': float(team2_score),
                    'week': int(week_num),
                    'opponent_key': team1_key,
                    'opponent_score': float(team1_score)
                })
            except (KeyError, IndexError):
                continue
    except (KeyError, IndexError):
        pass

    return data

def get_draft_results(oauth, year: int) -> List[Dict]:
    """Get draft results for a season"""
    gm = Game(oauth, 'mlb')
    league_id = get_league_id_by_name(oauth, year)
    
    if not league_id:
        return []
    
    lg = League(oauth, league_id)
    return lg.draft_results()

def get_standings(oauth, year: int) -> List[Dict]:
    """Get full season standings"""
    gm = Game(oauth, 'mlb')
    league_id = get_league_id_by_name(oauth, year)
    
    if not league_id:
        return []
    
    lg = League(oauth, league_id)
    standings = lg.standings()
    teams = lg.teams()

    results = []
    for standing in standings:
        team_key = standing['team_key']
        team_info = teams[team_key]
        team_name = team_info.get('name', 'Unknown Team')
        raw_manager = team_info['managers'][0]['manager'].get('nickname', 'Unknown Manager')
        manager = normalize_manager_name(raw_manager, year, team_name)
        
        results.append({
            'rank': int(standing.get('rank', 0)),
            'team_key': team_key,
            'team_name': team_name,
            'manager': manager,
            'wins': int(standing.get('outcome_totals', {}).get('wins', 0)),
            'losses': int(standing.get('outcome_totals', {}).get('losses', 0)),
            'ties': int(standing.get('outcome_totals', {}).get('ties', 0)),
            'win_pct': float(standing.get('outcome_totals', {}).get('percentage', 0)),
            'points_for': float(standing.get('points_for', 0)),
            'points_against': float(standing.get('points_against', 0))
        })
    
    return results

def get_all_season_scores(oauth, year: int, num_weeks: int = 24) -> List[Dict]:
    """Get all weekly scores for entire season"""
    all_scores = []
    
    for week in range(1, num_weeks + 1):
        try:
            week_data = get_week_scores(oauth, year, week)
            all_scores.extend(week_data)
            print(f"  ✓ Week {week} collected")
        except Exception as e:
            print(f"  ✗ Week {week} not available: {e}")
            break
    
    return all_scores

def get_rosters_with_stats(oauth, year: int) -> List[Dict]:
    """Get all team rosters with player stats from Yahoo."""
    gm = Game(oauth, 'mlb')
    league_id = get_league_id_by_name(oauth, year)
    
    if not league_id:
        return []
    
    lg = League(oauth, league_id)
    teams_data = lg.teams()
    
    all_players = []
    
    for team_key, team_info in teams_data.items():
        team_name = team_info.get('name', 'Unknown Team')
        raw_manager = team_info['managers'][0]['manager'].get('nickname', 'Unknown Manager')
        manager = normalize_manager_name(raw_manager, year, team_name)
        team_logo = team_info.get('team_logos', [{}])[0].get('team_logo', {}).get('url', '')
        
        print(f"    Getting roster with stats for {team_name}...")
        
        try:
            team_obj = lg.to_team(team_key)
            
            # Get roster with stats - this should include season stats
            roster = team_obj.roster(week=None)
            
            for player in roster:
                player_id = player.get('player_id', '')
                name = player.get('name', '')
                if isinstance(name, dict):
                    name = name.get('full', '')
                
                position_type = player.get('position_type', '')
                eligible_positions = player.get('eligible_positions', [])
                if isinstance(eligible_positions, str):
                    eligible_positions = [eligible_positions]
                
                primary_position = eligible_positions[0] if eligible_positions else ''
                
                # Get player stats and points from Yahoo
                player_stats = player.get('player_stats', {})
                player_points = player.get('player_points', {})
                
                # Extract stats
                stats = {}
                stats_list = player_stats.get('stats', [])
                if isinstance(stats_list, list):
                    for stat in stats_list:
                        stat_id = str(stat.get('stat_id', ''))
                        stat_value = stat.get('value', 0)
                        # Map stat IDs to names
                        stat_name = YAHOO_STATS_ID_MAP.get(stat_id)
                        if stat_name:
                            try:
                                stats[stat_name] = float(stat_value) if '.' in str(stat_value) else int(stat_value)
                            except (ValueError, TypeError):
                                stats[stat_name] = 0
                
                # Get fantasy points directly from Yahoo
                fantasy_points = 0
                if player_points:
                    fantasy_points = float(player_points.get('total', 0))
                
                all_players.append({
                    'player_id': player_id,
                    'name': name,
                    'position_type': position_type,
                    'eligible_positions': eligible_positions,
                    'primary_position': primary_position,
                    'selected_position': player.get('selected_position', ''),
                    'status': player.get('status', ''),
                    'team_key': team_key,
                    'team_name': team_name,
                    'team_logo': team_logo,
                    'manager': manager,
                    'stats': stats,
                    'fantasy_points': fantasy_points,
                    'headshot_url': player.get('headshot', {}).get('url', '') if isinstance(player.get('headshot'), dict) else '',
                    'mlb_team': player.get('editorial_team_abbr', ''),
                })
            
        except Exception as e:
            print(f"    ⚠ Could not get roster for {team_name}: {e}")
            import traceback
            traceback.print_exc()
    
    # Sort by fantasy points
    all_players.sort(key=lambda x: x.get('fantasy_points', 0), reverse=True)
    return all_players

# =============================================================================
# STAT NAME MAPPING FOR DATAFRAME INTEGRATION
# =============================================================================
# Maps Yahoo Stat Names/Abbreviations to the internal abbreviations
# used by Fangraphs data and fantasy point calculation functions (e.g., 'BB' -> 'BBA' for pitchers).
EXTERNAL_NAME_TO_INTERNAL_ABBR = {
    # Batting
    'R': 'R', 'Runs': 'R', 'H': 'H', 'Hits': 'H', '1B': '1B', 'Singles': '1B', '2B': '2B', 
    'Doubles': '2B', '3B': '3B', 'Triples': '3B', 'HR': 'HR', 'Home Runs': 'HR', 'RBI': 'RBI', 
    'Runs Batted In': 'RBI', 'SB': 'SB', 'Stolen Bases': 'SB', 'CS': 'CS', 'Caught Stealing': 'CS', 
    'BB': 'BB', 'Walks': 'BB', 'IBB': 'IBB', 'Intentional Walks': 'IBB', 'SO': 'SO', 
    'Strikeouts': 'SO', 'HBP': 'HBP', 'Hit By Pitch': 'HBP', 'AVG': 'AVG', 'OBP': 'OBP',
    'SLG': 'SLG', 'OPS': 'OPS', 'AB': 'AB', 'PA': 'PA',
    'Hit for the Cycle': 'CYC', 'Grand Slams': 'SLAM',
    
    # Pitching - Base stats
    'IP': 'IP', 'Innings Pitched': 'IP', 'W': 'W', 'Wins': 'W', 'L': 'L', 'Losses': 'L', 
    'SV': 'SV', 'Saves': 'SV', 'HLD': 'HLD', 'Holds': 'HLD', 'ER': 'ER', 'Earned Runs': 'ER',
    'BF': 'BF', 'Batters Faced': 'BF', 
    
    # Pitching - Stats that need special Yahoo abbreviations
    'HA': 'HA', 'Hits Allowed': 'HA', 'Hits Against': 'HA',  # Yahoo uses HA for pitching hits
    'BBA': 'BBA', 'Walks Allowed': 'BBA', 'Walks Against': 'BBA', 'BB Allowed': 'BBA',  # Yahoo uses BBA for pitching walks
    'K': 'K', 'Pitching Strikeouts': 'K', 'Strikeouts Pitched': 'K',  # Yahoo uses K for pitching strikeouts
    
    # Other pitching modifiers
    'CG': 'CG', 'Complete Games': 'CG', 'SHO': 'ShO', 'ShO': 'ShO', 'Shutouts': 'ShO', 
    'QS': 'QS', 'Quality Starts': 'QS', 'NH': 'NH', 'No Hitter': 'NH', 'No Hitters': 'NH',
    'PG': 'PG', 'Perfect Game': 'PG', 'Perfect Games': 'PG',
    'PICK': 'PICK', 'Pickoffs': 'PICK',
}

# Yahoo stat ID to name mapping
YAHOO_STAT_ID_MAP = {
    # Batting
    '9': '1B',      # Singles
    '10': '2B',     # Doubles
    '11': '3B',     # Triples
    '12': 'HR',     # Home Runs
    '13': 'RBI',    # RBI
    '16': 'SB',     # Stolen Bases
    '17': 'CS',     # Caught Stealing
    '18': 'BB',     # Walks
    '19': 'IBB',    # Intentional Walks
    '20': 'HBP',    # Hit By Pitch
    '21': 'SO',     # Strikeouts
    '64': 'CYC',    # Cycle
    '66': 'SLAM',    # Grand Slam
    # Pitching
    '50': 'IP',     # Innings Pitched
    '37': 'ER',     # Earned Runs
    '34': 'HA',     # Hits Allowed (also 'H' for pitchers)
    '39': 'BBA',    # Walks Allowed (also 'BB' for pitchers)
    '28': 'W',      # Wins
    '29': 'L',      # Losses
    '32': 'SV',     # Saves
    '42': 'K',      # Strikeouts (pitching)
    '48': 'HLD',    # Holds
    '30': 'CG',     # Complete Games
    '31': 'ShO',    # Shutouts
    '83': 'QS',     # Quality Starts
    '72': 'PICK',    # Pickoff
    '79': 'NH'      # No Hitter
}


def get_rosters(oauth, year: int) -> Dict[str, List[Dict]]:
    """Get all team rosters for a given season."""
    gm = Game(oauth, 'mlb')
    league_id = get_league_id_by_name(oauth, year)
    
    if not league_id:
        return {}
    
    lg = League(oauth, league_id)
    teams_data = lg.teams()
    
    rosters = {}
    
    for team_key, team_info in teams_data.items():
        team_name = team_info.get('name', 'Unknown Team')
        raw_manager = team_info['managers'][0]['manager'].get('nickname', 'Unknown Manager')
        manager = normalize_manager_name(raw_manager, year, team_name)
        team_logo = team_info.get('team_logos', [{}])[0].get('team_logo', {}).get('url', '')
        
        print(f"    Getting roster for {team_name}...")
        
        try:
            team_obj = lg.to_team(team_key)
            roster = team_obj.roster(week=None)
            
            players = []
            for player in roster:
                player_id = player.get('player_id', '')
                name = player.get('name', '')
                if isinstance(name, dict):
                    name = name.get('full', '')
                
                position_type = player.get('position_type', '')
                eligible_positions = player.get('eligible_positions', [])
                if isinstance(eligible_positions, str):
                    eligible_positions = [eligible_positions]
                
                primary_position = eligible_positions[0] if eligible_positions else ''
                
                players.append({
                    'player_id': player_id,
                    'name': name,
                    'position_type': position_type,
                    'eligible_positions': eligible_positions,
                    'primary_position': primary_position,
                    'selected_position': player.get('selected_position', ''),
                    'status': player.get('status', ''),
                    'team_key': team_key,
                    'team_name': team_name,
                    'team_logo': team_logo,
                    'manager': manager,
                })
            
            rosters[team_key] = players
            
        except Exception as e:
            print(f"    ⚠ Could not get roster for {team_name}: {e}")
            rosters[team_key] = []
    
    return rosters

def get_league_settings(oauth, year: int) -> Dict:
    """Fetch league settings, categorizing points based on unique Stat ID."""
    gm = Game(oauth, 'mlb')
    league_id = get_league_id_by_name(oauth, year)
    
    if not league_id:
        return {
            'batting': DEFAULT_BATTING_SCORING, 
            'pitching': DEFAULT_PITCHING_SCORING,
            'start_date': None,
            'end_date': None
        }
    
    lg = League(oauth, league_id)
    
    try:
        settings = lg.settings()
        
        # Get season dates
        start_date = settings.get('start_date', None)
        end_date = settings.get('end_date', None)
        print(f"    League dates: {start_date} to {end_date}")
        
        # -------------------------------------------------------------------------
        # 1. Fetch Scoring as DataFrame
        # -------------------------------------------------------------------------
        scoring_df = _build_scoring_df(lg, settings)
        
        if scoring_df.empty:
            print("    ⚠ Could not retrieve scoring modifiers. Using default scoring.")
            return {
                'batting': DEFAULT_BATTING_SCORING, 
                'pitching': DEFAULT_PITCHING_SCORING,
                'start_date': start_date,
                'end_date': end_date
            }
        
        # -------------------------------------------------------------------------
        # 2. Map Stat IDs to Position Type
        # -------------------------------------------------------------------------
        # Standard Yahoo Batting IDs
        KNOWN_BATTING_IDS = {
            '9', '10', '11', '12', '13', '16', '17', '18', '19', '20', '21', '64', '66'
        }
        
        # Standard Yahoo Pitching IDs
        KNOWN_PITCHING_IDS = {
            '50', '37', '34', '39', '28', '29', '32', '42', '48', '30', '31', '83', '72', '79'
        }

        batting_scoring = {}
        pitching_scoring = {}
        
        # -------------------------------------------------------------------------
        # 3. Categorize and Standardize Names using the DataFrame
        # -------------------------------------------------------------------------
        for index, row in scoring_df.iterrows():
            stat_id = str(row['stat_id'])
            yahoo_name = str(row['stat_name'])
            point_val = row['value']
            
            # Look up the standardized name (e.g., 'Home Runs' -> 'HR')
            internal_abbr = globals().get('EXTERNAL_NAME_TO_INTERNAL_ABBR', {}).get(yahoo_name, yahoo_name)
            
            if stat_id in KNOWN_BATTING_IDS:
                batting_scoring[internal_abbr] = point_val
            
            elif stat_id in KNOWN_PITCHING_IDS:
                # Use the internal abbreviation, which handles the necessary renames (BB -> BBA, H -> HA)
                pitching_scoring[internal_abbr] = point_val
            
            else:
                # Fallback: Assign to batting unless it looks clearly like a pitching stat
                if internal_abbr in ['IP', 'W', 'L', 'SV', 'HLD', 'ERA', 'WHIP', 'CG', 'ShO', 'QS', 'NH', 'PG']:
                    pitching_scoring[internal_abbr] = point_val
                else:
                    batting_scoring[internal_abbr] = point_val


        print(f"    Batting scoring: {batting_scoring}")
        print(f"    Pitching scoring: {pitching_scoring}")
        
        return {
            'batting': batting_scoring if batting_scoring else DEFAULT_BATTING_SCORING,
            'pitching': pitching_scoring if pitching_scoring else DEFAULT_PITCHING_SCORING,
            'start_date': start_date,
            'end_date': end_date,
            'raw_settings': settings
        }
        
    except Exception as e:
        print(f"  ⚠ Could not get league settings: {e}")
        import traceback
        traceback.print_exc()
        return {
            'batting': DEFAULT_BATTING_SCORING, 
            'pitching': DEFAULT_PITCHING_SCORING,
            'start_date': None,
            'end_date': None
        }


def get_league_scoring_settings(oauth, year: int) -> Dict:
    """Wrapper for backwards compatibility."""
    return get_league_settings(oauth, year)

def get_all_transactions(oauth, year: int, max_per_type: int = 1000) -> List[Dict]:
    """Fetch all transactions for the league."""
    gm = Game(oauth, 'mlb')
    league_id = get_league_id_by_name(oauth, year)
    
    if not league_id:
        return []

    lg = League(oauth, league_id)
    
    try:
        trans_types = ['add', 'drop', 'add/drop', 'trade']
        all_transactions = []
        
        for trans_type in trans_types:
            try:
                print(f"    Fetching {trans_type} transactions...")
                # Don't use 'start' parameter - it's not supported in all versions
                transactions_raw = lg.transactions(trans_type, count=max_per_type)
                
                if not transactions_raw:
                    print(f"      ✓ 0 {trans_type} transactions")
                    continue
                
                type_count = 0
                for trans in transactions_raw:
                    trans_data = {
                        'transaction_key': trans.get('transaction_key', ''),
                        'transaction_id': trans.get('transaction_id', ''),
                        'type': trans.get('type', trans_type),
                        'timestamp': trans.get('timestamp', ''),
                        'status': trans.get('status', ''),
                        'players': []
                    }
                    
                    players = trans.get('players', [])
                    if isinstance(players, dict):
                        player_count = players.get('count', 0)
                        for i in range(int(player_count)):
                            player_info = players.get(str(i), {})
                            if player_info:
                                player = player_info.get('player', [[{}]])
                                if isinstance(player, list) and len(player) > 0:
                                    player_data = player[0]
                                    if isinstance(player_data, list) and len(player_data) > 0:
                                        player_data = player_data[0]
                                else:
                                    player_data = player
                                
                                trans_player_data = player_info.get('transaction_data', {})
                                if isinstance(trans_player_data, list) and len(trans_player_data) > 0:
                                    trans_player_data = trans_player_data[0]
                                
                                player_name = ''
                                if isinstance(player_data, dict):
                                    name_info = player_data.get('name', {})
                                    if isinstance(name_info, dict):
                                        player_name = name_info.get('full', '')
                                    elif isinstance(name_info, str):
                                        player_name = name_info
                                
                                player_trans = {
                                    'player_key': player_data.get('player_key', '') if isinstance(player_data, dict) else '',
                                    'player_name': player_name,
                                    'transaction_type': trans_player_data.get('type', '') if isinstance(trans_player_data, dict) else '',
                                    'source_type': trans_player_data.get('source_type', '') if isinstance(trans_player_data, dict) else '',
                                    'source_team_key': trans_player_data.get('source_team_key', '') if isinstance(trans_player_data, dict) else '',
                                    'source_team_name': trans_player_data.get('source_team_name', '') if isinstance(trans_player_data, dict) else '',
                                    'destination_team_key': trans_player_data.get('destination_team_key', '') if isinstance(trans_player_data, dict) else '',
                                    'destination_team_name': trans_player_data.get('destination_team_name', '') if isinstance(trans_player_data, dict) else '',
                                }
                                trans_data['players'].append(player_trans)
                    
                    all_transactions.append(trans_data)
                    type_count += 1
                
                print(f"      ✓ {type_count} {trans_type} transactions")
                    
            except Exception as e:
                print(f"    ⚠ Could not get {trans_type} transactions: {e}")
                continue
        
        all_transactions.sort(key=lambda x: x.get('timestamp', ''), reverse=True)
        print(f"    Total: {len(all_transactions)} transactions")
        return all_transactions
        
    except Exception as e:
        print(f"  ⚠ Could not get transactions: {e}")
        return []

# =============================================================================
# FANGRAPHS DATA FUNCTIONS (via pybaseball)
# =============================================================================

def get_fangraphs_batting_stats(year: int, start_date: str = None, end_date: str = None, timeout: int = 180) -> pd.DataFrame:
    """Fetch batting stats from Fangraphs for a given year or date range.
    
    Args:
        year: The season year
        start_date: Optional start date in 'YYYY-MM-DD' format
        end_date: Optional end date in 'YYYY-MM-DD' format
        timeout: Timeout in seconds
    """
    if not PYBASEBALL_AVAILABLE:
        print("  ⚠ pybaseball not available")
        return pd.DataFrame()
    
    import signal
    import sys
    
    def timeout_handler(signum, frame):
        raise TimeoutError(f"Fangraphs batting request timed out after {timeout}s")
    
    try:
        if start_date and end_date:
            print(f"    Fetching Fangraphs batting stats for {start_date} to {end_date}...")
        else:
            print(f"    Fetching Fangraphs batting stats for {year}...")
        print(f"    (This may take 1-2 minutes on first run...)")
        sys.stdout.flush()
        
        # Set timeout (Unix only - Windows will skip this)
        if hasattr(signal, 'SIGALRM'):
            old_handler = signal.signal(signal.SIGALRM, timeout_handler)
            signal.alarm(timeout)
        
        try:
            if start_date and end_date:
                # Use batting_stats_range for date-specific stats
                from pybaseball import batting_stats_range
                batters = batting_stats_range(start_date, end_date)
            else:
                # Fall back to full season
                batters = batting_stats(year, qual=1)
        finally:
            # Reset alarm
            if hasattr(signal, 'SIGALRM'):
                signal.alarm(0)
                signal.signal(signal.SIGALRM, old_handler)
        
        print(f"    ✓ Got {len(batters)} batters from Fangraphs")
        return batters
    except TimeoutError as e:
        print(f"    ⚠ {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"    ⚠ Could not fetch Fangraphs batting stats: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()

def get_fangraphs_pitching_stats(year: int, start_date: str = None, end_date: str = None, timeout: int = 180) -> pd.DataFrame:
    """Fetch pitching stats from Fangraphs for a given year or date range.
    
    Args:
        year: The season year
        start_date: Optional start date in 'YYYY-MM-DD' format
        end_date: Optional end date in 'YYYY-MM-DD' format
        timeout: Timeout in seconds
    """
    if not PYBASEBALL_AVAILABLE:
        print("  ⚠ pybaseball not available")
        return pd.DataFrame()
    
    import signal
    import sys
    
    def timeout_handler(signum, frame):
        raise TimeoutError(f"Fangraphs pitching request timed out after {timeout}s")
    
    try:
        if start_date and end_date:
            print(f"    Fetching Fangraphs pitching stats for {start_date} to {end_date}...")
        else:
            print(f"    Fetching Fangraphs pitching stats for {year}...")
        print(f"    (This may take 1-2 minutes on first run...)")
        sys.stdout.flush()
        
        # Set timeout (Unix only - Windows will skip this)
        if hasattr(signal, 'SIGALRM'):
            old_handler = signal.signal(signal.SIGALRM, timeout_handler)
            signal.alarm(timeout)
        
        try:
            if start_date and end_date:
                # Use pitching_stats_range for date-specific stats
                from pybaseball import pitching_stats_range
                pitchers = pitching_stats_range(start_date, end_date)
            else:
                # Fall back to full season
                pitchers = pitching_stats(year, qual=1)
        finally:
            # Reset alarm
            if hasattr(signal, 'SIGALRM'):
                signal.alarm(0)
                signal.signal(signal.SIGALRM, old_handler)
        
        print(f"    ✓ Got {len(pitchers)} pitchers from Fangraphs")
        return pitchers
    except TimeoutError as e:
        print(f"    ⚠ {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"    ⚠ Could not fetch Fangraphs pitching stats: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()

def calculate_batting_fantasy_points(stats: Dict, scoring: Dict) -> float:
    """Calculate fantasy points for a batter.
    
    Maps Fangraphs stat names to Yahoo scoring abbreviations.
    """
    points = 0.0
    
    # Fangraphs column name -> Yahoo scoring abbreviation
    FANGRAPHS_TO_YAHOO_BATTING = {
        '1B': '1B',
        '2B': '2B', 
        '3B': '3B',
        'HR': 'HR',
        'R': 'R',
        'RBI': 'RBI',
        'SB': 'SB',
        'CS': 'CS',
        'BB': 'BB',
        'SO': 'SO',
        'HBP': 'HBP',
        'IBB': 'IBB',
    }
    
    # Calculate singles if not present
    if '1B' not in stats and 'H' in stats:
        doubles = stats.get('2B', 0) or 0
        triples = stats.get('3B', 0) or 0
        hrs = stats.get('HR', 0) or 0
        hits = stats.get('H', 0) or 0
        stats['1B'] = hits - doubles - triples - hrs
    
    # Calculate points using the mapping
    for fg_stat, yahoo_stat in FANGRAPHS_TO_YAHOO_BATTING.items():
        if fg_stat in stats and stats[fg_stat] is not None:
            stat_value = float(stats[fg_stat]) if stats[fg_stat] else 0
            if yahoo_stat in scoring:
                points += stat_value * scoring[yahoo_stat]
    
    return round(points, 1)

def calculate_pitching_fantasy_points(stats: Dict, scoring: Dict) -> float:
    """Calculate fantasy points for a pitcher.
    
    Maps Fangraphs stat names to Yahoo scoring abbreviations.
    Note: Yahoo uses different names for pitching stats:
    - Hits Allowed = 'HA' (Fangraphs: 'H')
    - Walks Allowed = 'BBA' (Fangraphs: 'BB')  
    - Strikeouts = 'K' (Fangraphs: 'SO')
    """
    points = 0.0
    
    # Fangraphs column name -> Yahoo scoring abbreviation
    FANGRAPHS_TO_YAHOO_PITCHING = {
        'IP': 'IP',
        'W': 'W',
        'L': 'L',
        'SV': 'SV',
        'HLD': 'HLD',
        'ER': 'ER',
        'H': 'HA',      # Hits Allowed
        'BB': 'BBA',    # Walks Allowed
        'SO': 'K',      # Strikeouts (pitching)
        'QS': 'QS',
        'CG': 'CG',
        'ShO': 'ShO',
    }
    
    for fg_stat, yahoo_stat in FANGRAPHS_TO_YAHOO_PITCHING.items():
        if fg_stat in stats and stats[fg_stat] is not None:
            stat_value = float(stats[fg_stat]) if stats[fg_stat] else 0
            if yahoo_stat in scoring:
                points += stat_value * scoring[yahoo_stat]
    
    return round(points, 1)

# =============================================================================
# PLAYER STATS INTEGRATION
# =============================================================================

def build_player_stats(oauth, year: int) -> List[Dict]:
    """Build complete player stats using Fangraphs with league date range and scoring."""
    print(f"\n  Building player stats for {year}...")
    
    # Always use Fangraphs with proper league settings
    return build_player_stats_with_fangraphs(oauth, year)


def get_player_headshots(oauth, year: int, player_ids: List[str]) -> Dict[str, str]:
    """Fetch player headshots from Yahoo using player_details.
    
    Args:
        oauth: OAuth object
        year: Season year
        player_ids: List of player IDs (numeric) to fetch headshots for
        
    Returns:
        Dict mapping player_id to headshot URL
    """
    gm = Game(oauth, 'mlb')
    league_id = get_league_id_by_name(oauth, year)
    
    if not league_id:
        return {}
    
    lg = League(oauth, league_id)
    headshots = {}
    
    # Get the game key for constructing full player keys
    # League ID format is like "398.l.17906" where 398 is the game key
    game_key = league_id.split('.')[0]
    
    # Process in batches to avoid API limits
    batch_size = 25
    total = len(player_ids)
    
    for i in range(0, total, batch_size):
        batch = player_ids[i:i + batch_size]
        batch_num = (i // batch_size) + 1
        total_batches = (total + batch_size - 1) // batch_size
        
        print(f"      Fetching headshots batch {batch_num}/{total_batches}...")
        
        for player_id in batch:
            try:
                # Construct full player key: "{game_key}.p.{player_id}"
                full_player_key = f"{game_key}.p.{player_id}"
                
                # Get player details from Yahoo
                details = lg.player_details(full_player_key)
                
                if details:
                    # The response structure can vary - let's handle different formats
                    headshot_url = ''
                    
                    # If it's a list, get the first element
                    if isinstance(details, list) and len(details) > 0:
                        player_info = details[0]
                    else:
                        player_info = details
                    
                    # Navigate through possible nested structures
                    if isinstance(player_info, dict):
                        # Direct headshot field
                        if 'headshot' in player_info:
                            hs = player_info['headshot']
                            if isinstance(hs, dict):
                                headshot_url = hs.get('url', '')
                            elif isinstance(hs, str):
                                headshot_url = hs
                        
                        # Sometimes it's under image_url
                        if not headshot_url and 'image_url' in player_info:
                            headshot_url = player_info['image_url']
                        
                        # Check for nested player data
                        if not headshot_url and 'player' in player_info:
                            player_data = player_info['player']
                            if isinstance(player_data, list):
                                for item in player_data:
                                    if isinstance(item, dict) and 'headshot' in item:
                                        hs = item['headshot']
                                        if isinstance(hs, dict):
                                            headshot_url = hs.get('url', '')
                                        break
                    
                    if headshot_url:
                        headshots[str(player_id)] = headshot_url
                        
            except Exception as e:
                # Log first few errors for debugging
                if len(headshots) < 3:
                    print(f"        Debug - Error for player {player_id}: {e}")
                pass
        
        # Small delay between batches to be nice to the API
        if i + batch_size < total:
            time.sleep(0.5)
    
    return headshots


def build_player_stats_with_fangraphs(oauth, year: int) -> List[Dict]:
    """Build player stats using Fangraphs with league-specific dates and scoring."""
    print(f"\n  Building player stats with Fangraphs for {year}...")
    
    # Get rosters from Yahoo
    print(f"  Step 1: Fetching rosters from Yahoo...")
    rosters = get_rosters(oauth, year)
    
    all_players = []
    for team_key, players in rosters.items():
        all_players.extend(players)
    
    print(f"    ✓ Got {len(all_players)} rostered players from Yahoo")
    
    # Get league settings (scoring + dates)
    print(f"  Step 2: Fetching league settings (scoring & dates)...")
    settings = get_league_settings(oauth, year)
    batting_scoring = settings.get('batting', DEFAULT_BATTING_SCORING)
    pitching_scoring = settings.get('pitching', DEFAULT_PITCHING_SCORING)
    start_date = settings.get('start_date')
    end_date = settings.get('end_date')
    
    # Get Fangraphs stats using league date range
    print(f"  Step 3: Fetching stats from Fangraphs...")
    batting_df = get_fangraphs_batting_stats(year, start_date, end_date)
    pitching_df = get_fangraphs_pitching_stats(year, start_date, end_date)
    
    if batting_df.empty and pitching_df.empty:
        print("  ⚠ No Fangraphs data available, returning roster-only data")
        return all_players
    
    batting_names = batting_df['Name'].tolist() if not batting_df.empty else []
    pitching_names = pitching_df['Name'].tolist() if not pitching_df.empty else []
    
    # Match players and add stats (headshots from MLB using mlbID)
    print(f"  Step 4: Matching players and calculating fantasy points...")
    matched_count = 0
    unmatched_players = []
    
    for player in all_players:
        player_name = player.get('name', '')
        position_type = player.get('position_type', '')
        
        player['stats'] = {}
        player['fantasy_points'] = 0
        player['headshot_url'] = ''
        player['mlb_team'] = ''
        
        if position_type == 'B':
            fg_name = match_player_name(player_name, batting_names)
            if fg_name and not batting_df.empty:
                player_row = batting_df[batting_df['Name'] == fg_name].iloc[0]
                
                # Get MLB ID for headshot
                if 'mlbID' in player_row.index:
                    mlb_id = player_row.get('mlbID')
                    if mlb_id and not pd.isna(mlb_id):
                        player['headshot_url'] = f"https://img.mlbstatic.com/mlb-photos/image/upload/d_people:generic:headshot:67:current.png/w_213,q_auto:best/v1/people/{int(mlb_id)}/headshot/67/current"
                
                stats = {
                    'G': safe_int(player_row.get('G', 0)),
                    'AB': safe_int(player_row.get('AB', 0)),
                    'PA': safe_int(player_row.get('PA', 0)),
                    'H': safe_int(player_row.get('H', 0)),
                    '2B': safe_int(player_row.get('2B', 0)),
                    '3B': safe_int(player_row.get('3B', 0)),
                    'HR': safe_int(player_row.get('HR', 0)),
                    'R': safe_int(player_row.get('R', 0)),
                    'RBI': safe_int(player_row.get('RBI', 0)),
                    'BB': safe_int(player_row.get('BB', 0)),
                    'SO': safe_int(player_row.get('SO', 0)),
                    'SB': safe_int(player_row.get('SB', 0)),
                    'CS': safe_int(player_row.get('CS', 0)),
                    'HBP': safe_int(player_row.get('HBP', 0)),
                    # Fangraphs uses 'BA' for batting average, not 'AVG'
                    'AVG': round(safe_float(player_row.get('BA', 0) or player_row.get('AVG', 0)), 3),
                    'OBP': round(safe_float(player_row.get('OBP', 0)), 3),
                    'SLG': round(safe_float(player_row.get('SLG', 0)), 3),
                    'OPS': round(safe_float(player_row.get('OPS', 0)), 3),
                }
                
                # Calculate AVG if not available but we have H and AB
                if stats['AVG'] == 0 and stats['AB'] > 0 and stats['H'] > 0:
                    stats['AVG'] = round(stats['H'] / stats['AB'], 3)
                
                stats['1B'] = stats['H'] - stats['2B'] - stats['3B'] - stats['HR']
                
                player['stats'] = stats
                player['fantasy_points'] = calculate_batting_fantasy_points(stats, batting_scoring)
                player['mlb_team'] = player_row.get('Team', '') or player_row.get('Tm', '')
                matched_count += 1
            else:
                unmatched_players.append(f"{player_name} (B)")
                
        elif position_type == 'P':
            fg_name = match_player_name(player_name, pitching_names)
            if fg_name and not pitching_df.empty:
                player_row = pitching_df[pitching_df['Name'] == fg_name].iloc[0]
                
                # Get MLB ID for headshot
                if 'mlbID' in player_row.index:
                    mlb_id = player_row.get('mlbID')
                    if mlb_id and not pd.isna(mlb_id):
                        player['headshot_url'] = f"https://img.mlbstatic.com/mlb-photos/image/upload/d_people:generic:headshot:67:current.png/w_213,q_auto:best/v1/people/{int(mlb_id)}/headshot/67/current"
                
                stats = {
                    'G': safe_int(player_row.get('G', 0)),
                    'GS': safe_int(player_row.get('GS', 0)),
                    'W': safe_int(player_row.get('W', 0)),
                    'L': safe_int(player_row.get('L', 0)),
                    'SV': safe_int(player_row.get('SV', 0)),
                    'HLD': safe_int(player_row.get('HLD', 0)) if 'HLD' in player_row else 0,
                    'IP': round(safe_float(player_row.get('IP', 0)), 1),
                    'H': safe_int(player_row.get('H', 0)),
                    'ER': safe_int(player_row.get('ER', 0)),
                    'HR': safe_int(player_row.get('HR', 0)),
                    'BB': safe_int(player_row.get('BB', 0)),
                    'SO': safe_int(player_row.get('SO', 0)),
                    # TBF = Total Batters Faced (Fangraphs uses 'TBF' or 'BF')
                    'TBF': safe_int(player_row.get('TBF', 0) or player_row.get('BF', 0)),
                    'ERA': round(safe_float(player_row.get('ERA', 0)), 2),
                    'WHIP': round(safe_float(player_row.get('WHIP', 0)), 2),
                    # Try multiple possible column names for K/9 and BB/9
                    'K/9': round(safe_float(player_row.get('K/9', 0) or player_row.get('K9', 0) or player_row.get('SO9', 0)), 2),
                    'BB/9': round(safe_float(player_row.get('BB/9', 0) or player_row.get('BB9', 0)), 2),
                }
                
                # Calculate K/9 and BB/9 if not available but we have IP and SO/BB
                if stats['K/9'] == 0 and stats['IP'] > 0 and stats['SO'] > 0:
                    stats['K/9'] = round((stats['SO'] * 9) / stats['IP'], 2)
                if stats['BB/9'] == 0 and stats['IP'] > 0 and stats['BB'] > 0:
                    stats['BB/9'] = round((stats['BB'] * 9) / stats['IP'], 2)
                
                # Calculate K% and BB% if we have TBF
                if stats['TBF'] > 0:
                    stats['K%'] = round((stats['SO'] / stats['TBF']) * 100, 1)
                    stats['BB%'] = round((stats['BB'] / stats['TBF']) * 100, 1)
                else:
                    stats['K%'] = 0.0
                    stats['BB%'] = 0.0
                
                player['stats'] = stats
                player['fantasy_points'] = calculate_pitching_fantasy_points(stats, pitching_scoring)
                player['mlb_team'] = player_row.get('Team', '') or player_row.get('Tm', '')
                matched_count += 1
            else:
                unmatched_players.append(f"{player_name} (P)")
    
    print(f"    ✓ Matched {matched_count}/{len(all_players)} players")
    
    if unmatched_players and len(unmatched_players) <= 20:
        print(f"    Unmatched: {', '.join(unmatched_players)}")
    elif unmatched_players:
        print(f"    {len(unmatched_players)} unmatched players")
    
    all_players.sort(key=lambda x: x.get('fantasy_points', 0), reverse=True)
    return all_players

# =============================================================================
# MANAGER STATS FUNCTIONS
# =============================================================================

def calculate_manager_stats(all_seasons_data: Dict) -> Dict:
    """Calculate all-time manager statistics"""
    manager_stats = {}
    
    for year, data in all_seasons_data.items():
        standings = data.get('standings', [])
        
        if year == 2019:
            standings = correct_2019_playoffs(data)
        
        for team in standings:
            manager_raw = team['manager']
            
            if manager_raw == "Logan":
                if year == 2023:
                    if team['team_key'] == "422.l.6780.t.4":
                        manager = "Logan C"
                    elif team['team_key'] == "422.l.6780.t.12":
                        manager = "Logan S"
                    elif "Draft Pool" in team['team_name']:
                        manager = "Logan C"
                    elif "Peanut Butter" in team['team_name'] or "Elly" in team['team_name']:
                        manager = "Logan S"
                    else:
                        manager = "Logan"
                elif year >= 2020 and year <= 2022:
                    manager = "Logan C"
                elif year >= 2024:
                    manager = "Logan S"
                else:
                    manager = "Logan"
            elif manager_raw == "Josh":
                if year >= 2019 and year <= 2022:
                    if team['team_key'].endswith('t.1'):
                        manager = "Josh B"
                    else:
                        manager = "Josh S"
                elif year >= 2023:
                    if team['team_key'].endswith('t.1'):
                        manager = "Josh B"
                    else:
                        manager = "Josh"
                else:
                    manager = "Josh"
            else:
                manager = manager_raw
            
            if manager not in manager_stats:
                manager_stats[manager] = {
                    'manager_name': manager,
                    'first_season': year,
                    'total_wins': 0,
                    'total_losses': 0,
                    'total_ties': 0,
                    'championships': 0,
                    'runner_ups': 0,
                    'playoff_appearances': 0,
                    'seasons_played': 0,
                    'total_points_for': 0,
                    'season_history': []
                }
            
            manager_stats[manager]['total_wins'] += team['wins']
            manager_stats[manager]['total_losses'] += team['losses']
            manager_stats[manager]['total_ties'] += team['ties']
            manager_stats[manager]['total_points_for'] += team['points_for']
            manager_stats[manager]['seasons_played'] += 1
            
            if team['rank'] == 1:
                manager_stats[manager]['championships'] += 1
            if team['rank'] == 2:
                manager_stats[manager]['runner_ups'] += 1
            if team['rank'] <= 6:
                manager_stats[manager]['playoff_appearances'] += 1
            
            manager_stats[manager]['season_history'].append({
                'year': year,
                'team_name': team['team_name'],
                'rank': team['rank'],
                'wins': team['wins'],
                'losses': team['losses'],
                'points_for': team['points_for']
            })
    
    for manager in manager_stats.values():
        total_games = manager['total_wins'] + manager['total_losses']
        manager['win_pct'] = round(manager['total_wins'] / total_games, 3) if total_games > 0 else 0
        manager['avg_finish'] = round(
            sum(s['rank'] for s in manager['season_history']) / len(manager['season_history']), 1
        )
    
    return manager_stats

def correct_2019_playoffs(season_data: Dict) -> List[Dict]:
    """Correct 2019 standings based on actual playoff results"""
    standings = season_data.get('standings', [])
    
    CORRECT_2019_RANKS = {'Ryan': 1, 'Rich': 2, 'Tyler': 3}
    
    corrected_standings = []
    for team in standings:
        team_copy = team.copy()
        manager = team['manager']
        
        if manager in CORRECT_2019_RANKS:
            team_copy['rank'] = CORRECT_2019_RANKS[manager]
        
        corrected_standings.append(team_copy)
    
    corrected_standings.sort(key=lambda x: x['rank'])
    return corrected_standings

# =============================================================================
# DATA COLLECTION & EXPORT FUNCTIONS
# =============================================================================

def create_directory_structure():
    """Create necessary directories"""
    dirs = [
        DATA_DIR,
        f"{DATA_DIR}/current_season",
        f"{DATA_DIR}/historical",
        f"{DATA_DIR}/managers",
        f"{DATA_DIR}/players"
    ]
    for directory in dirs:
        os.makedirs(directory, exist_ok=True)

def collect_historical_data(oauth):
    """Collect all historical season data"""
    print("Collecting historical data...")
    
    for year in HISTORICAL_SEASONS:
        print(f"\nCollecting {year} season...")
        
        standings = get_standings(oauth, year)
        scores = get_all_season_scores(oauth, year)
        try:
            draft = get_draft_results(oauth, year)
        except:
            draft = []
        teams = get_teams(oauth, year)
        
        year_dir = f"{DATA_DIR}/historical/{year}"
        os.makedirs(year_dir, exist_ok=True)
        
        with open(f"{year_dir}/final_standings.json", 'w', encoding='utf-8') as f:
            json.dump(standings, f, indent=2, ensure_ascii=False)
        with open(f"{year_dir}/all_scores.json", 'w', encoding='utf-8') as f:
            json.dump(scores, f, indent=2, ensure_ascii=False)
        with open(f"{year_dir}/draft.json", 'w', encoding='utf-8') as f:
            json.dump(draft, f, indent=2, ensure_ascii=False)
        with open(f"{year_dir}/teams.json", 'w', encoding='utf-8') as f:
            json.dump(teams, f, indent=2, ensure_ascii=False)
        
        print(f"✓ {year} season saved")

def collect_historical_player_data(oauth):
    """Collect player stats and transactions for historical seasons"""
    print("\nCollecting historical player data...")
    
    for year in HISTORICAL_SEASONS:
        print(f"\n{'='*50}")
        print(f"Collecting {year} player data...")
        print('='*50)
        
        year_dir = f"{DATA_DIR}/historical/{year}"
        os.makedirs(year_dir, exist_ok=True)
        
        try:
            player_stats = build_player_stats(oauth, year)
            with open(f"{year_dir}/player_stats.json", 'w', encoding='utf-8') as f:
                json.dump(player_stats, f, indent=2, ensure_ascii=False)
            print(f"  ✓ Player stats saved ({len(player_stats)} players)")
        except Exception as e:
            print(f"  ⚠ Could not collect player stats: {e}")
            import traceback
            traceback.print_exc()
        
        try:
            transactions = get_all_transactions(oauth, year)
            with open(f"{year_dir}/transactions.json", 'w', encoding='utf-8') as f:
                json.dump(transactions, f, indent=2, ensure_ascii=False)
            print(f"  ✓ Transactions saved ({len(transactions)} transactions)")
        except Exception as e:
            print(f"  ⚠ Could not collect transactions: {e}")
        
        try:
            scoring = get_league_scoring_settings(oauth, year)
            with open(f"{year_dir}/scoring_settings.json", 'w', encoding='utf-8') as f:
                json.dump(scoring, f, indent=2, ensure_ascii=False)
            print(f"  ✓ Scoring settings saved")
        except Exception as e:
            print(f"  ⚠ Could not save scoring settings: {e}")

def collect_current_season_data(oauth):
    """Collect current season data"""
    print(f"\nUpdating {CURRENT_SEASON} season data...")
    
    current_season_dir = f"{DATA_DIR}/current_season"
    os.makedirs(current_season_dir, exist_ok=True)
    
    standings = get_standings(oauth, CURRENT_SEASON)
    teams = get_teams(oauth, CURRENT_SEASON)
    scores = get_all_season_scores(oauth, CURRENT_SEASON, num_weeks=26)
    
    with open(f"{current_season_dir}/standings.json", 'w', encoding='utf-8') as f:
        json.dump(standings, f, indent=2, ensure_ascii=False)
    with open(f"{current_season_dir}/teams.json", 'w', encoding='utf-8') as f:
        json.dump(teams, f, indent=2, ensure_ascii=False)
    with open(f"{current_season_dir}/all_scores.json", 'w', encoding='utf-8') as f:
        json.dump(scores, f, indent=2, ensure_ascii=False)
    
    scores_by_week = {}
    for score in scores:
        week = score['week']
        if week not in scores_by_week:
            scores_by_week[week] = []
        scores_by_week[week].append(score)
    
    for week, week_scores in scores_by_week.items():
        with open(f"{current_season_dir}/week_{week}_scores.json", 'w', encoding='utf-8') as f:
            json.dump(week_scores, f, indent=2, ensure_ascii=False)
    
    print(f"✓ Current season updated ({len(scores_by_week)} weeks)")

def collect_current_season_player_data(oauth):
    """Collect player stats and transactions for current season"""
    print(f"\n{'='*50}")
    print(f"Updating {CURRENT_SEASON} player data...")
    print('='*50)
    
    current_season_dir = f"{DATA_DIR}/current_season"
    os.makedirs(current_season_dir, exist_ok=True)
    
    try:
        player_stats = build_player_stats(oauth, CURRENT_SEASON)
        with open(f"{current_season_dir}/player_stats.json", 'w', encoding='utf-8') as f:
            json.dump(player_stats, f, indent=2, ensure_ascii=False)
        print(f"  ✓ Player stats saved ({len(player_stats)} players)")
    except Exception as e:
        print(f"  ⚠ Could not collect player stats: {e}")
        import traceback
        traceback.print_exc()
    
    try:
        transactions = get_all_transactions(oauth, CURRENT_SEASON)
        with open(f"{current_season_dir}/transactions.json", 'w', encoding='utf-8') as f:
            json.dump(transactions, f, indent=2, ensure_ascii=False)
        print(f"  ✓ Transactions saved ({len(transactions)} transactions)")
    except Exception as e:
        print(f"  ⚠ Could not collect transactions: {e}")
    
    try:
        scoring = get_league_scoring_settings(oauth, CURRENT_SEASON)
        with open(f"{current_season_dir}/scoring_settings.json", 'w', encoding='utf-8') as f:
            json.dump(scoring, f, indent=2, ensure_ascii=False)
        print(f"  ✓ Scoring settings saved")
    except Exception as e:
        print(f"  ⚠ Could not save scoring settings: {e}")

def update_manager_stats(oauth):
    """Update manager statistics"""
    print("\nUpdating manager statistics...")
    
    all_seasons = {}
    
    for year in HISTORICAL_SEASONS:
        standings_file = f"{DATA_DIR}/historical/{year}/final_standings.json"
        scores_file = f"{DATA_DIR}/historical/{year}/all_scores.json"
        
        if os.path.exists(standings_file):
            with open(standings_file, 'r', encoding='utf-8') as f:
                standings = json.load(f)
            scores = []
            if os.path.exists(scores_file):
                with open(scores_file, 'r', encoding='utf-8') as f:
                    scores = json.load(f)
            all_seasons[year] = {'standings': standings, 'scores': scores}
            print(f"  ✓ Loaded {year} season")
    
    current_standings_file = f"{DATA_DIR}/current_season/standings.json"
    if os.path.exists(current_standings_file):
        with open(current_standings_file, 'r', encoding='utf-8') as f:
            standings = json.load(f)
        scores = []
        current_scores_file = f"{DATA_DIR}/current_season/all_scores.json"
        if os.path.exists(current_scores_file):
            with open(current_scores_file, 'r', encoding='utf-8') as f:
                scores = json.load(f)
        all_seasons[CURRENT_SEASON] = {'standings': standings, 'scores': scores}
        print(f"  ✓ Loaded {CURRENT_SEASON} season")
    
    if not all_seasons:
        print("  ⚠ No season data available")
        return
    
    manager_stats = calculate_manager_stats(all_seasons)
    
    managers_dir = f"{DATA_DIR}/managers"
    os.makedirs(managers_dir, exist_ok=True)
    
    with open(f"{managers_dir}/all_time_stats.json", 'w', encoding='utf-8') as f:
        json.dump(list(manager_stats.values()), f, indent=2)
    
    manager_history = []
    for manager_data in manager_stats.values():
        for season in manager_data['season_history']:
            manager_history.append({'manager': manager_data['manager_name'], **season})
    
    with open(f"{managers_dir}/manager_history.json", 'w', encoding='utf-8') as f:
        json.dump(manager_history, f, indent=2, ensure_ascii=False)
    
    print(f"✓ Manager stats updated ({len(manager_stats)} managers)")

def build_player_career_history():
    """Aggregate player data across all seasons"""
    print("\nBuilding player career history...")
    
    player_careers = {}
    
    for year in HISTORICAL_SEASONS + [CURRENT_SEASON]:
        if year == CURRENT_SEASON:
            player_file = f"{DATA_DIR}/current_season/player_stats.json"
        else:
            player_file = f"{DATA_DIR}/historical/{year}/player_stats.json"
        
        if os.path.exists(player_file):
            with open(player_file, 'r', encoding='utf-8') as f:
                players = json.load(f)
            
            for player in players:
                name = player.get('name', '')
                if not name:
                    continue
                
                name_key = normalize_player_name(name)
                
                if name_key not in player_careers:
                    player_careers[name_key] = {
                        'name': name,
                        'seasons': [],
                        'career_fantasy_points': 0
                    }
                
                player_careers[name_key]['seasons'].append({
                    'year': year,
                    'team_name': player.get('team_name', ''),
                    'manager': player.get('manager', ''),
                    'fantasy_points': player.get('fantasy_points', 0),
                    'position_type': player.get('position_type', ''),
                    'stats': player.get('stats', {})
                })
                player_careers[name_key]['career_fantasy_points'] += player.get('fantasy_points', 0)
    
    players_dir = f"{DATA_DIR}/players"
    os.makedirs(players_dir, exist_ok=True)
    
    with open(f"{players_dir}/player_history.json", 'w', encoding='utf-8') as f:
        json.dump(player_careers, f, indent=2, ensure_ascii=False)
    
    print(f"✓ Player career history built ({len(player_careers)} players)")

def create_league_info():
    """Create league metadata file"""
    league_info = {
        'league_name': 'Fantasy Baseball Civil War',
        'founded': min(HISTORICAL_SEASONS) if HISTORICAL_SEASONS else CURRENT_SEASON,
        'current_season': CURRENT_SEASON,
        'total_teams': 12,
        'league_type': 'Points',
        'last_updated': datetime.now().isoformat()
    }
    with open(f"{DATA_DIR}/league_info.json", 'w', encoding='utf-8') as f:
        json.dump(league_info, f, indent=2, ensure_ascii=False)

# =============================================================================
# MAIN EXECUTION FUNCTIONS
# =============================================================================

def initial_setup():
    """Run this ONCE to collect all historical data"""
    print("=" * 60)
    print("INITIAL SETUP - Collecting All Historical Data")
    print("=" * 60)
    
    create_directory_structure()
    oauth = setup_oauth()
    
    if HISTORICAL_SEASONS:
        collect_historical_data(oauth)
    
    collect_current_season_data(oauth)
    update_manager_stats(oauth)
    create_league_info()
    
    print("\n" + "=" * 60)
    print("✓ Initial setup complete!")
    print("=" * 60)

def weekly_update():
    """Run this WEEKLY during the season"""
    print("=" * 60)
    print(f"WEEKLY UPDATE - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    oauth = setup_oauth()
    collect_current_season_data(oauth)
    update_manager_stats(oauth)
    create_league_info()
    
    print("\n" + "=" * 60)
    print("✓ Weekly update complete!")
    print("=" * 60)

def player_data_setup():
    """Run this to collect player stats and transactions for all seasons"""
    print("=" * 60)
    print("PLAYER DATA SETUP - Collecting Player Stats & Transactions")
    print("(Using Fangraphs for stats, Yahoo for rosters)")
    print("=" * 60)
    
    if not PYBASEBALL_AVAILABLE:
        print("\n⚠ ERROR: pybaseball is required for player stats.")
        print("Install it with: pip install pybaseball")
        return
    
    create_directory_structure()
    oauth = setup_oauth()
    
    if HISTORICAL_SEASONS:
        collect_historical_player_data(oauth)
    
    collect_current_season_player_data(oauth)
    build_player_career_history()
    
    print("\n" + "=" * 60)
    print("✓ Player data setup complete!")
    print("=" * 60)

def weekly_update_with_players():
    """Run this WEEKLY to update everything including player data"""
    print("=" * 60)
    print(f"FULL WEEKLY UPDATE - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    oauth = setup_oauth()
    collect_current_season_data(oauth)
    
    if PYBASEBALL_AVAILABLE:
        collect_current_season_player_data(oauth)
        build_player_career_history()
    else:
        print("\n⚠ pybaseball not available, skipping player stats")
    
    update_manager_stats(oauth)
    create_league_info()
    
    print("\n" + "=" * 60)
    print("✓ Full weekly update complete!")
    print("=" * 60)

def check_available_seasons():
    """Check which seasons your Yahoo account has league data for"""
    print("=" * 60)
    print("CHECKING AVAILABLE SEASONS")
    print("=" * 60)
    
    oauth = setup_oauth()
    gm = Game(oauth, 'mlb')
    
    for year in range(2015, 2026):
        try:
            league_ids = gm.league_ids(year=year)
            if league_ids:
                print(f"\n{year}:")
                for league_id in league_ids:
                    try:
                        lg = League(oauth, league_id)
                        league_metadata = lg.metadata()
                        league_name = league_metadata.get('name', 'Unknown')
                        print(f"  - '{league_name}' (ID: {league_id})")
                    except:
                        print(f"  - ID: {league_id}")
        except:
            pass
    
    print("\n" + "=" * 60)


def test_settings(year: int = None):
    """Test and display league settings (scoring, dates, stat mappings)"""
    if year is None:
        year = CURRENT_SEASON
    
    print("=" * 60)
    print(f"TESTING LEAGUE SETTINGS FOR {year}")
    print("=" * 60)
    
    oauth = setup_oauth()
    gm = Game(oauth, 'mlb')
    league_id = get_league_id_by_name(oauth, year)
    
    if not league_id:
        print(f"⚠ No league found for {year}")
        return
    
    print(f"\nLeague ID: {league_id}")
    
    lg = League(oauth, league_id)
    
    # Get raw settings
    print("\n--- RAW SETTINGS ---")
    try:
        settings = lg.settings()
        print(f"Start Date: {settings.get('start_date')}")
        print(f"End Date: {settings.get('end_date')}")
        print(f"Playoff Start Week: {settings.get('playoff_start_week')}")
        
        # Show stat modifiers
        print("\n--- STAT MODIFIERS (Scoring) ---")
        stat_modifiers = settings.get('stat_modifiers', {}).get('stats', [])
        for mod in stat_modifiers:
            stat_info = mod.get('stat', {})
            stat_id = stat_info.get('stat_id')
            value = stat_info.get('value')
            print(f"  Stat ID {stat_id}: {value} points")
        
    except Exception as e:
        print(f"⚠ Error getting settings: {e}")
        import traceback
        traceback.print_exc()
    
    # Get stat ID map
    print("\n--- STAT ID MAP ---")
    try:
        # CORRECTED: Access stat_id_map as an attribute
        stat_id_map = lg.stats_id_map 
        for name, stat_id in sorted(stat_id_map.items(), key=lambda x: x[1]):
            print(f"  {name}: {stat_id}")
    except Exception as e:
        print(f"⚠ Error getting stat_id_map: {e}")
    
    # Get processed settings
    print("\n--- PROCESSED SCORING ---")
    processed = get_league_settings(oauth, year)
    print(f"\nBatting Scoring:")
    for stat, value in sorted(processed.get('batting', {}).items()):
        print(f"  {stat}: {value}")
    print(f"\nPitching Scoring:")
    for stat, value in sorted(processed.get('pitching', {}).items()):
        print(f"  {stat}: {value}")
    
    print("\n" + "=" * 60)


def test_fangraphs():
    """Test Fangraphs data fetching"""
    print("=" * 60)
    print("TESTING FANGRAPHS DATA")
    print("=" * 60)
    
    if not PYBASEBALL_AVAILABLE:
        print("\n⚠ pybaseball not installed!")
        print("Install with: pip install pybaseball")
        return
    
    print("\nNote: First-time fetches can take 1-2 minutes as data is downloaded.")
    print("Subsequent runs use cached data and are much faster.\n")
    
    print("Fetching 2024 batting stats...")
    batters = get_fangraphs_batting_stats(2024)
    if not batters.empty:
        print(f"✓ Got {len(batters)} batters")
        print(batters.head(3)[['Name', 'Team', 'G', 'HR', 'RBI']].to_string())
    else:
        print("✗ Failed to get batting stats")
    
    print("\nFetching 2024 pitching stats...")
    pitchers = get_fangraphs_pitching_stats(2024)
    if not pitchers.empty:
        print(f"✓ Got {len(pitchers)} pitchers")
        print(pitchers.head(3)[['Name', 'Team', 'W', 'SO', 'ERA']].to_string())
    else:
        print("✗ Failed to get pitching stats")
    
    print("\n" + "=" * 60)
    if not batters.empty and not pitchers.empty:
        print("✓ Fangraphs test complete! You can now run 'python collect_data.py players'")
    else:
        print("⚠ Fangraphs test had issues. Check your internet connection.")
    print("=" * 60)


def test_pybaseball_encoding():
    """Test how pybaseball encodes player names with special characters."""
    print("=" * 60)
    print("TESTING PYBASEBALL NAME ENCODING")
    print("=" * 60)
    
    if not PYBASEBALL_AVAILABLE:
        print("ERROR: pybaseball not available")
        return
    
    from pybaseball import batting_stats, pitching_stats, batting_stats_range, pitching_stats_range
    
    # Test players we know have accented names
    test_names = ['rodriguez', 'ramirez', 'diaz', 'lopez', 'perez', 'acuna', 'soto']
    
    print("\n--- Testing batting_stats (full season) ---")
    try:
        batters = batting_stats(2024, qual=1)
        print(f"Got {len(batters)} batters")
        print(f"Columns: {list(batters.columns)[:10]}...")
        
        for search in test_names:
            matches = batters[batters['Name'].str.lower().str.contains(search, na=False)]
            if not matches.empty:
                print(f"\n  Searching '{search}': {len(matches)} matches")
                for name in matches['Name'].head(3):
                    print(f"    - {repr(name)}")
    except Exception as e:
        print(f"Error with batting_stats: {e}")
    
    print("\n\n--- Testing batting_stats_range (date range) ---")
    try:
        batters_range = batting_stats_range('2024-03-28', '2024-09-29')
        print(f"Got {len(batters_range)} batters")
        print(f"Columns: {list(batters_range.columns)[:10]}...")
        
        for search in test_names:
            matches = batters_range[batters_range['Name'].str.lower().str.contains(search, na=False)]
            if not matches.empty:
                print(f"\n  Searching '{search}': {len(matches)} matches")
                for name in matches['Name'].head(3):
                    print(f"    - {repr(name)}")
            else:
                print(f"\n  Searching '{search}': NO MATCHES")
    except Exception as e:
        print(f"Error with batting_stats_range: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n\n--- COMPARING: Looking for 'Julio Rodriguez' specifically ---")
    try:
        batters = batting_stats(2024, qual=1)
        batters_range = batting_stats_range('2024-03-28', '2024-09-29')
        
        print(f"\nIn batting_stats (full season):")
        julio_full = batters[batters['Name'].str.contains('Julio', na=False, case=False)]
        for _, row in julio_full.iterrows():
            print(f"  Raw: {repr(row['Name'])}")
            print(f"  Normalized: {repr(normalize_player_name(row['Name']))}")
        
        print(f"\nIn batting_stats_range (date range):")
        julio_range = batters_range[batters_range['Name'].str.contains('Julio', na=False, case=False)]
        for _, row in julio_range.iterrows():
            print(f"  Raw: {repr(row['Name'])}")
            print(f"  Normalized: {repr(normalize_player_name(row['Name']))}")
        
        if julio_range.empty:
            print("  (No 'Julio' found in basic search - trying to find in all names)")
            # Search through all names for ones containing the bytes for 'Julio'
            for name in batters_range['Name']:
                if 'Jul' in str(name) or 'jul' in str(name).lower():
                    print(f"  Found: {repr(name)}")
                    print(f"  Normalized: {repr(normalize_player_name(name))}")
        
        # Test that Yahoo name would match
        yahoo_name = 'Julio Rodríguez'
        print(f"\n  Yahoo name: {repr(yahoo_name)}")
        print(f"  Yahoo normalized: {repr(normalize_player_name(yahoo_name))}")
        
        print(f"\n  MATCH TEST: ", end="")
        if julio_range.empty:
            print("Cannot test - Julio not found in batting_stats_range")
        else:
            fg_name = julio_range.iloc[0]['Name']
            if normalize_player_name(yahoo_name) == normalize_player_name(fg_name):
                print(f"SUCCESS! '{yahoo_name}' matches '{fg_name}'")
            else:
                print(f"FAILED! '{normalize_player_name(yahoo_name)}' != '{normalize_player_name(fg_name)}'")
                
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n" + "=" * 60)
    print("ENCODING TEST COMPLETE")
    print("=" * 60)


def test_single_year_players(year: int = 2024):
    """Test player data collection for a single year"""
    print("=" * 60)
    print(f"TESTING PLAYER DATA COLLECTION FOR {year}")
    print("=" * 60)
    
    if not PYBASEBALL_AVAILABLE:
        print("\n⚠ pybaseball not installed!")
        print("Install with: pip install pybaseball")
        return
    
    create_directory_structure()
    oauth = setup_oauth()
    
    print(f"\nCollecting {year} player data...")
    player_stats = build_player_stats(oauth, year)
    
    if player_stats:
        # Save to test file
        test_file = f"{DATA_DIR}/test_player_stats_{year}.json"
        with open(test_file, 'w', encoding='utf-8') as f:
            json.dump(player_stats, f, indent=2, ensure_ascii=False)
        
        print(f"\n✓ Collected {len(player_stats)} players")
        print(f"✓ Saved to {test_file}")
        
        # Show top 5 by fantasy points
        print(f"\nTop 5 players by fantasy points:")
        for i, p in enumerate(player_stats[:5]):
            print(f"  {i+1}. {p['name']} ({p['manager']}) - {p['fantasy_points']} pts")
    else:
        print("✗ Failed to collect player data")
    
    print("\n" + "=" * 60)


def test_player_scoring(player_name: str, year: int = 2024):
    """Debug scoring calculation for a specific player"""
    print("=" * 60)
    print(f"TESTING SCORING FOR: {player_name} ({year})")
    print("=" * 60)
    
    if not PYBASEBALL_AVAILABLE:
        print("\n⚠ pybaseball not installed!")
        return
    
    oauth = setup_oauth()
    
    # Get league settings
    print("\n--- LEAGUE SETTINGS ---")
    settings = get_league_settings(oauth, year)
    batting_scoring = settings.get('batting', {})
    pitching_scoring = settings.get('pitching', {})
    start_date = settings.get('start_date')
    end_date = settings.get('end_date')
    
    print(f"Date range: {start_date} to {end_date}")
    print(f"\nBatting scoring: {batting_scoring}")
    print(f"\nPitching scoring: {pitching_scoring}")
    
    # Get Fangraphs stats
    print("\n--- FETCHING FANGRAPHS STATS ---")
    batting_df = get_fangraphs_batting_stats(year, start_date, end_date)
    pitching_df = get_fangraphs_pitching_stats(year, start_date, end_date)
    
    # Search for player
    print(f"\n--- SEARCHING FOR {player_name} ---")
    
    # Check batting
    if not batting_df.empty:
        batting_match = batting_df[batting_df['Name'].str.contains(player_name, case=False, na=False)]
        if not batting_match.empty:
            print(f"\nFound in BATTERS:")
            row = batting_match.iloc[0]
            print(f"  Name: {row['Name']}")
            print(f"  Team: {row.get('Team', 'N/A')}")
            
            # Build stats dict
            stats = {
                'G': int(row.get('G', 0) or 0),
                'AB': int(row.get('AB', 0) or 0),
                'H': int(row.get('H', 0) or 0),
                '2B': int(row.get('2B', 0) or 0),
                '3B': int(row.get('3B', 0) or 0),
                'HR': int(row.get('HR', 0) or 0),
                'R': int(row.get('R', 0) or 0),
                'RBI': int(row.get('RBI', 0) or 0),
                'BB': int(row.get('BB', 0) or 0),
                'SO': int(row.get('SO', 0) or 0),
                'SB': int(row.get('SB', 0) or 0),
                'CS': int(row.get('CS', 0) or 0),
                'HBP': int(row.get('HBP', 0) or 0),
            }
            stats['1B'] = stats['H'] - stats['2B'] - stats['3B'] - stats['HR']
            
            print(f"\n  Stats from Fangraphs:")
            for stat, val in stats.items():
                print(f"    {stat}: {val}")
            
            # Calculate points step by step
            print(f"\n  POINT CALCULATION:")
            total_points = 0
            for fg_stat in ['1B', '2B', '3B', 'HR', 'R', 'RBI', 'SB', 'CS', 'BB', 'SO', 'HBP']:
                if fg_stat in stats and fg_stat in batting_scoring:
                    stat_val = stats[fg_stat]
                    point_val = batting_scoring[fg_stat]
                    points = stat_val * point_val
                    total_points += points
                    print(f"    {fg_stat}: {stat_val} × {point_val} = {points:.1f}")
            
            print(f"\n  TOTAL: {total_points:.1f} points")
    
    # Check pitching
    if not pitching_df.empty:
        pitching_match = pitching_df[pitching_df['Name'].str.contains(player_name, case=False, na=False)]
        if not pitching_match.empty:
            print(f"\nFound in PITCHERS:")
            row = pitching_match.iloc[0]
            print(f"  Name: {row['Name']}")
            print(f"  Team: {row.get('Team', 'N/A')}")
            
            # Build stats dict
            stats = {
                'G': int(row.get('G', 0) or 0),
                'GS': int(row.get('GS', 0) or 0),
                'W': int(row.get('W', 0) or 0),
                'L': int(row.get('L', 0) or 0),
                'SV': int(row.get('SV', 0) or 0),
                'HLD': int(row.get('HLD', 0) or 0) if 'HLD' in row else 0,
                'IP': float(row.get('IP', 0) or 0),
                'H': int(row.get('H', 0) or 0),
                'ER': int(row.get('ER', 0) or 0),
                'BB': int(row.get('BB', 0) or 0),
                'SO': int(row.get('SO', 0) or 0),
            }
            
            print(f"\n  Stats from Fangraphs:")
            for stat, val in stats.items():
                print(f"    {stat}: {val}")
            
            # Calculate points step by step
            print(f"\n  POINT CALCULATION:")
            # Mapping: Fangraphs -> Yahoo
            fg_to_yahoo = {'IP': 'IP', 'W': 'W', 'L': 'L', 'SV': 'SV', 'HLD': 'HLD', 
                          'ER': 'ER', 'H': 'HA', 'BB': 'BBA', 'SO': 'K'}
            
            total_points = 0
            for fg_stat, yahoo_stat in fg_to_yahoo.items():
                if fg_stat in stats and yahoo_stat in pitching_scoring:
                    stat_val = stats[fg_stat]
                    point_val = pitching_scoring[yahoo_stat]
                    points = stat_val * point_val
                    total_points += points
                    print(f"    {fg_stat} ({yahoo_stat}): {stat_val} × {point_val} = {points:.1f}")
            
            print(f"\n  TOTAL: {total_points:.1f} points")
    
    print("\n" + "=" * 60)


def update_headshots_only():
    """Fetch headshots and add them to existing player_stats.json files."""
    print("=" * 60)
    print("UPDATING PLAYER HEADSHOTS")
    print("=" * 60)
    
    oauth = setup_oauth()
    
    # Process historical seasons
    for year in HISTORICAL_SEASONS:
        player_file = f"{DATA_DIR}/historical/{year}/player_stats.json"
        if os.path.exists(player_file):
            print(f"\n--- {year} ---")
            update_headshots_for_file(oauth, year, player_file)
    
    # Process current season
    current_file = f"{DATA_DIR}/current_season/player_stats.json"
    if os.path.exists(current_file):
        print(f"\n--- {CURRENT_SEASON} (current) ---")
        update_headshots_for_file(oauth, CURRENT_SEASON, current_file)
    
    # Rebuild player history with headshots
    print("\n--- Rebuilding player history ---")
    build_player_career_history()
    
    print("\n" + "=" * 60)
    print("✓ Headshots update complete!")
    print("=" * 60)


def test_headshot(player_id: str, year: int = None):
    """Test fetching a single player's headshot for debugging."""
    if year is None:
        year = CURRENT_SEASON
    
    print("=" * 60)
    print(f"TESTING HEADSHOT FETCH FOR PLAYER {player_id} ({year})")
    print("=" * 60)
    
    oauth = setup_oauth()
    gm = Game(oauth, 'mlb')
    league_id = get_league_id_by_name(oauth, year)
    
    if not league_id:
        print(f"⚠ No league found for {year}")
        return
    
    print(f"League ID: {league_id}")
    
    lg = League(oauth, league_id)
    
    # Get the game key
    game_key = league_id.split('.')[0]
    full_player_key = f"{game_key}.p.{player_id}"
    print(f"Full player key: {full_player_key}")
    
    try:
        print(f"\nCalling lg.player_details('{full_player_key}')...")
        details = lg.player_details(full_player_key)
        
        print(f"\nResponse type: {type(details)}")
        print(f"Response content:")
        print(json.dumps(details, indent=2, default=str))
        
    except Exception as e:
        print(f"⚠ Error: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n" + "=" * 60)


def update_headshots_for_file(oauth, year: int, filepath: str):
    """Update headshots in a single player_stats.json file using Fangraphs mlbID."""
    # Load existing data
    with open(filepath, 'r', encoding='utf-8') as f:
        players = json.load(f)
    
    print(f"  Loading {len(players)} players from {filepath}")
    
    # Get players that need headshots
    players_needing_headshots = [
        p for p in players 
        if not p.get('headshot_url') or p.get('headshot_url') == ''
    ]
    
    if not players_needing_headshots:
        print(f"  ✓ All players already have headshots")
        return
    
    print(f"  {len(players_needing_headshots)} players need headshots")
    
    # Get league settings for date range
    settings = get_league_settings(oauth, year)
    start_date = settings.get('start_date')
    end_date = settings.get('end_date')
    
    # Fetch Fangraphs data to get player IDs
    print(f"  Fetching Fangraphs data for player IDs...")
    batting_df = get_fangraphs_batting_stats(year, start_date, end_date)
    pitching_df = get_fangraphs_pitching_stats(year, start_date, end_date)
    
    batting_names = batting_df['Name'].tolist() if not batting_df.empty else []
    pitching_names = pitching_df['Name'].tolist() if not pitching_df.empty else []
    
    # Update players with headshots from MLB using mlbID
    updated_count = 0
    for player in players:
        # Skip if already has headshot
        if player.get('headshot_url'):
            continue
            
        player_name = player.get('name', '')
        position_type = player.get('position_type', '')
        
        mlb_id = None
        
        if position_type == 'B' and not batting_df.empty:
            fg_name = match_player_name(player_name, batting_names)
            if fg_name:
                player_row = batting_df[batting_df['Name'] == fg_name].iloc[0]
                if 'mlbID' in player_row.index:
                    mlb_id = player_row.get('mlbID')
                            
        elif position_type == 'P' and not pitching_df.empty:
            fg_name = match_player_name(player_name, pitching_names)
            if fg_name:
                player_row = pitching_df[pitching_df['Name'] == fg_name].iloc[0]
                if 'mlbID' in player_row.index:
                    mlb_id = player_row.get('mlbID')
        
        if mlb_id and not pd.isna(mlb_id):
            # Use MLB's official headshot service
            player['headshot_url'] = f"https://img.mlbstatic.com/mlb-photos/image/upload/d_people:generic:headshot:67:current.png/w_213,q_auto:best/v1/people/{int(mlb_id)}/headshot/67/current"
            updated_count += 1
    
    print(f"  ✓ Updated {updated_count} players with MLB headshots")
    
    # Save updated data
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(players, f, indent=2, ensure_ascii=False)
    
    print(f"  ✓ Saved to {filepath}")


# =============================================================================
# MAIN
# =============================================================================

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
        
        if command == "setup":
            initial_setup()
        elif command == "check":
            check_available_seasons()
        elif command == "players":
            player_data_setup()
        elif command == "headshots":
            update_headshots_only()
        elif command == "full":
            weekly_update_with_players()
        elif command == "test-fangraphs":
            test_fangraphs()
        elif command == "test-settings":
            # Test league settings - defaults to current year, or specify year as 2nd arg
            year = int(sys.argv[2]) if len(sys.argv) > 2 else CURRENT_SEASON
            test_settings(year)
        elif command == "test-year":
            # Test single year - defaults to 2024, or specify year as 3rd arg
            year = int(sys.argv[2]) if len(sys.argv) > 2 else 2024
            test_single_year_players(year)
        elif command == "test-player":
            # Test scoring for a specific player
            if len(sys.argv) < 3:
                print("Usage: python collect_data.py test-player 'Player Name' [year]")
            else:
                player_name = sys.argv[2]
                year = int(sys.argv[3]) if len(sys.argv) > 3 else 2024
                test_player_scoring(player_name, year)
        elif command == "test-headshot":
            # Test headshot fetch for a single player
            if len(sys.argv) < 3:
                print("Usage: python collect_data.py test-headshot <player_id> [year]")
                print("Example: python collect_data.py test-headshot 9124 2021")
            else:
                player_id = sys.argv[2]
                year = int(sys.argv[3]) if len(sys.argv) > 3 else CURRENT_SEASON
                test_headshot(player_id, year)
        elif command == "debug-names":
            # Debug player name matching between Yahoo and Fangraphs
            year = int(sys.argv[2]) if len(sys.argv) > 2 else 2024
            debug_player_names(year)
        elif command == "test-encoding":
            test_pybaseball_encoding()
        else:
            print(f"Unknown command: {command}")
            print("\nAvailable commands:")
            print("  setup              - Initial setup (historical data)")
            print("  check              - Check available seasons")
            print("  players            - Collect player stats (Fangraphs + Yahoo)")
            print("  headshots          - Update existing player data with headshots only")
            print("  full               - Weekly update with player data")
            print("  test-fangraphs     - Test Fangraphs connection")
            print("  test-encoding      - Test pybaseball name encoding")
            print("  test-settings [yr] - Show league scoring settings for a year")
            print("  test-year [yr]     - Test player collection for one year (default 2024)")
            print("  test-player 'Name' [yr] - Debug scoring for a specific player")
            print("  test-headshot <id> [yr] - Debug headshot fetch for a player ID")
            print("  debug-names [yr]   - Debug player name matching Yahoo vs Fangraphs")
            print("  (none)             - Weekly update (no player data)")
    else:
        weekly_update()